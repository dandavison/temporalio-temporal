package tests

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	sdklog "go.temporal.io/sdk/log"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"
)

// ---------------------------------------------------------------------------
// Test 1: Diagnostic — does GetWorkflowExecutionHistory include transient
// events for a started transient WFT?
// ---------------------------------------------------------------------------

type PrematureEOSSuite struct {
	testcore.FunctionalTestBase
}

func TestPrematureEOSSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(PrematureEOSSuite))
}

func (s *PrematureEOSSuite) TestTransientWFTEventsInGetHistory() {
	id := "functional-transient-wft-events-in-get-history"
	wt := "functional-transient-wft-events-in-get-history-type"
	tl := "functional-transient-wft-events-in-get-history-taskqueue"
	identity := "worker1"

	we, err := s.FrontendClient().StartWorkflowExecution(testcore.NewContext(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: wt},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		WorkflowRunTimeout:  durationpb.New(60 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		Identity:            identity,
	})
	s.NoError(err)

	wfExec := &commonpb.WorkflowExecution{WorkflowId: id, RunId: we.RunId}

	stage := 0
	var startedEventID int64
	var historyLastEventID int64

	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		stage++
		switch stage {
		case 1:
			return nil, nil
		case 2:
			// Fail this WFT to create a transient retry.
			return nil, errors.New("intentional failure") //nolint:err113
		case 3:
			// Transient retry (attempt=2). Record the StartedEventId the SDK would see.
			startedEventID = task.GetStartedEventId()

			// While the transient WFT is started but not completed, query the history API.
			allEvents := s.GetHistory(s.Namespace().String(), wfExec)
			historyLastEventID = allEvents[len(allEvents)-1].GetEventId()

			return []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
					CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
						Result: payloads.EncodeString("done"),
					}},
			}}, nil
		}
		return nil, errors.New("unexpected stage") //nolint:err113
	}

	//nolint:staticcheck // SA1019 TaskPoller replacement needed
	poller := &testcore.TaskPoller{
		Client:              s.FrontendClient(),
		Namespace:           s.Namespace().String(),
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	// Stage 1: complete first WFT.
	_, err = poller.PollAndProcessWorkflowTask()
	s.NoError(err)

	// Send signal to create a new WFT.
	err = s.SendSignal(s.Namespace().String(), wfExec, "trigger", nil, identity)
	s.NoError(err)

	// Stage 2: fail WFT → transient retry created.
	_, err = poller.PollAndProcessWorkflowTask()
	s.NoError(err)

	// Stage 3: process transient retry (attempt=2).
	_, err = poller.PollAndProcessWorkflowTask(testcore.WithRetries(3))
	s.NoError(err)

	s.T().Logf("StartedEventId from poll response: %d", startedEventID)
	s.T().Logf("Last event from GetWorkflowExecutionHistory: %d", historyLastEventID)

	gap := startedEventID - historyLastEventID
	if gap > 0 {
		s.T().Logf("BUG CONFIRMED: GetWorkflowExecutionHistory is missing %d transient event(s)", gap)
	} else {
		s.T().Logf("GetWorkflowExecutionHistory includes transient events (PR #9325 working for this case)")
	}
	s.Equal(startedEventID, historyLastEventID,
		"GetWorkflowExecutionHistory should include transient events up to StartedEventId")
}

// ---------------------------------------------------------------------------
// Test 2: Stress test — explore parameter space for premature end of stream
// via speculative WFTs (Updates) + sticky cache miss.
//
// Key insight: transient WFTs (from failures) CANNOT trigger this bug because
// the server clears sticky on WFT failure (workflow_task_state_machine.go:980),
// so the retry goes to the normal queue with full history. The bug requires
// SPECULATIVE WFTs (from Updates) dispatched to the sticky queue where the SDK
// has a cache miss and calls GetWorkflowExecutionHistory.
// ---------------------------------------------------------------------------

func TestPrematureEndOfStreamStress(t *testing.T) {
	t.Parallel()

	scenarios := []stressScenario{
		{
			name:              "simple_purge",
			numWorkflows:      1,
			iterations:        20,
			purgeBeforeUpdate: true,
			getHistoryDelay:   0,
			concurrentSignal:  false,
			wftTimeout:        5 * time.Second,
		},
		{
			name:              "purge_with_signal",
			numWorkflows:      1,
			iterations:        20,
			purgeBeforeUpdate: true,
			getHistoryDelay:   20 * time.Millisecond,
			concurrentSignal:  true,
			wftTimeout:        5 * time.Second,
		},
		{
			name:              "short_wft_timeout_slow_history",
			numWorkflows:      1,
			iterations:        10,
			purgeBeforeUpdate: true,
			getHistoryDelay:   1500 * time.Millisecond,
			concurrentSignal:  false,
			wftTimeout:        1 * time.Second,
		},
		{
			name:              "high_concurrency",
			numWorkflows:      10,
			iterations:        10,
			purgeBeforeUpdate: true,
			getHistoryDelay:   0,
			concurrentSignal:  false,
			wftTimeout:        5 * time.Second,
		},
		{
			name:              "high_concurrency_with_signal",
			numWorkflows:      10,
			iterations:        10,
			purgeBeforeUpdate: true,
			getHistoryDelay:   10 * time.Millisecond,
			concurrentSignal:  true,
			wftTimeout:        5 * time.Second,
		},
		{
			name:              "natural_eviction",
			numWorkflows:      10,
			iterations:        10,
			purgeBeforeUpdate: false,
			getHistoryDelay:   0,
			concurrentSignal:  false,
			wftTimeout:        5 * time.Second,
		},
	}

	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			env := testcore.NewEnv(t)
			runStressScenario(
				t,
				env.GetTestCluster().Host().FrontendGRPCAddress(),
				env.Namespace().String(),
				env.FrontendClient(),
				sc,
			)
		})
	}
}

type stressScenario struct {
	name              string
	numWorkflows      int
	iterations        int
	purgeBeforeUpdate bool
	getHistoryDelay   time.Duration
	concurrentSignal  bool
	wftTimeout        time.Duration
}

func runStressScenario(
	t *testing.T,
	frontendAddr string,
	ns string,
	frontendClient workflowservice.WorkflowServiceClient,
	sc stressScenario,
) {
	t.Helper()

	logCap := newLogCapture()
	interceptor := &speculativeInterceptor{
		getHistoryDelay: sc.getHistoryDelay,
		triggerCh:       make(chan struct{}, 100),
	}

	sdkClient, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  frontendAddr,
		Namespace: ns,
		Logger:    logCap,
		ConnectionOptions: sdkclient.ConnectionOptions{
			DialOptions: []grpc.DialOption{
				grpc.WithUnaryInterceptor(interceptor.unary()),
			},
		},
	})
	require.NoError(t, err)
	defer sdkClient.Close()

	tq := fmt.Sprintf("stress-%s-%s", sc.name, uuid.NewString()[:8])
	w := sdkworker.New(sdkClient, tq, sdkworker.Options{})
	w.RegisterWorkflowWithOptions(speculativeWFTWorkflow, workflow.RegisterOptions{Name: "speculativeWFTWorkflow"})
	require.NoError(t, w.Start())
	defer w.Stop()

	var totalRepros atomic.Int32

	for iter := range sc.iterations {
		reproduced := runStressIteration(t, ns, frontendClient, sdkClient, sc, interceptor, logCap, tq, iter)
		if reproduced {
			totalRepros.Add(1)
		}
	}

	t.Logf("scenario=%s repros=%d/%d getHistoryCount=%d",
		sc.name,
		totalRepros.Load(), sc.iterations,
		interceptor.getHistoryCount.Load())

	if totalRepros.Load() > 0 {
		t.Logf("REPRODUCED premature end of stream in %d/%d iterations", totalRepros.Load(), sc.iterations)
	}
}

func runStressIteration(
	t *testing.T,
	ns string,
	frontendClient workflowservice.WorkflowServiceClient,
	sdkClient sdkclient.Client,
	sc stressScenario,
	interceptor *speculativeInterceptor,
	logCap *logCapture,
	tq string,
	iter int,
) bool {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	type wfRun struct {
		id    string
		runID string
	}
	runs := make([]wfRun, sc.numWorkflows)
	for i := range runs {
		wfID := fmt.Sprintf("stress-%s-iter%d-wf%d-%s", sc.name, iter, i, uuid.NewString()[:8])
		run, err := sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
			ID:                  wfID,
			TaskQueue:           tq,
			WorkflowTaskTimeout: sc.wftTimeout,
			WorkflowRunTimeout:  60 * time.Second,
		}, "speculativeWFTWorkflow")
		if err != nil {
			t.Logf("iter=%d wf=%d start failed: %v", iter, i, err)
			return false
		}
		runs[i] = wfRun{id: wfID, runID: run.GetRunID()}
	}

	// Wait for first WFT to complete (sticky established, workflow waiting for signal/update).
	time.Sleep(500 * time.Millisecond)

	logCap.reset()
	interceptor.armed.Store(true)

	// If concurrentSignal, start goroutine to send signals when GetHistory is detected.
	var signalWg sync.WaitGroup
	stopSignals := make(chan struct{})
	if sc.concurrentSignal {
		signalWg.Add(1)
		go func() {
			defer signalWg.Done()
			for {
				select {
				case <-stopSignals:
					return
				case <-interceptor.triggerCh:
					for _, r := range runs {
						_, _ = frontendClient.SignalWorkflowExecution(testcore.NewContext(),
							&workflowservice.SignalWorkflowExecutionRequest{
								Namespace:         ns,
								WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: r.id, RunId: r.runID},
								SignalName:        "work",
								Input:             payloads.EncodeString("noop"),
								Identity:          "stress-test",
							})
					}
				}
			}
		}()
	}

	// Purge sticky cache to force cache miss when the speculative WFT arrives.
	if sc.purgeBeforeUpdate {
		sdkworker.PurgeStickyWorkflowCache()
	}

	// Send Updates to all workflows. Each Update creates a speculative WFT on the
	// (now-empty) sticky queue. The SDK will poll it, detect cache miss, call
	// GetWorkflowExecutionHistory — and the speculative events may be missing.
	var updateWg sync.WaitGroup
	for _, r := range runs {
		updateWg.Add(1)
		go func() {
			defer updateWg.Done()
			handle, err := sdkClient.UpdateWorkflow(ctx, sdkclient.UpdateWorkflowOptions{
				WorkflowID:   r.id,
				RunID:        r.runID,
				UpdateName:   "myUpdate",
				WaitForStage: sdkclient.WorkflowUpdateStageCompleted,
			})
			if err != nil {
				return
			}
			_ = handle.Get(ctx, nil)
		}()
	}

	// Wait for updates to complete or timeout.
	done := make(chan struct{})
	go func() {
		updateWg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Logf("iter=%d: updates did not complete within 15s", iter)
	}

	interceptor.armed.Store(false)
	close(stopSignals)
	signalWg.Wait()

	reproduced := logCap.containsPrematureEOS()

	// Signal workflows to complete and clean up.
	for _, r := range runs {
		_ = sdkClient.SignalWorkflow(ctx, r.id, r.runID, "work", "done")
	}
	for _, r := range runs {
		_ = sdkClient.TerminateWorkflow(ctx, r.id, r.runID, "test cleanup")
	}

	return reproduced
}

// speculativeWFTWorkflow registers an Update handler and blocks on a signal.
func speculativeWFTWorkflow(ctx workflow.Context) error {
	err := workflow.SetUpdateHandler(ctx, "myUpdate", func(ctx workflow.Context) error {
		return nil
	})
	if err != nil {
		return err
	}

	ch := workflow.GetSignalChannel(ctx, "work")
	for {
		var action string
		ch.Receive(ctx, &action)
		if action == "done" {
			return nil
		}
	}
}

// ---------------------------------------------------------------------------
// Log capture
// ---------------------------------------------------------------------------

type logCapture struct {
	mu       sync.Mutex
	messages []string
}

func newLogCapture() *logCapture {
	return &logCapture{}
}

var _ sdklog.Logger = (*logCapture)(nil)

func (l *logCapture) Debug(msg string, keyvals ...interface{}) {}
func (l *logCapture) Info(msg string, keyvals ...interface{})  {}

func (l *logCapture) Warn(msg string, keyvals ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	full := fmt.Sprintf("%s %v", msg, keyvals)
	l.messages = append(l.messages, full)
}

func (l *logCapture) Error(msg string, keyvals ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	full := fmt.Sprintf("%s %v", msg, keyvals)
	l.messages = append(l.messages, full)
}

func (l *logCapture) With(keyvals ...interface{}) sdklog.Logger {
	return l
}

func (l *logCapture) containsPrematureEOS() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, m := range l.messages {
		if strings.Contains(m, "premature end of stream") {
			return true
		}
	}
	return false
}

func (l *logCapture) reset() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.messages = l.messages[:0]
}

// ---------------------------------------------------------------------------
// Test 3: Shard closure test — close shard between poll response and
// GetWorkflowExecutionHistory to lose speculative events.
// ---------------------------------------------------------------------------

func TestPrematureEndOfStreamShardClosure(t *testing.T) {
	t.Parallel()

	for i := range 5 {
		t.Run(fmt.Sprintf("iter%d", i), func(t *testing.T) {
			env := testcore.NewEnv(t, testcore.WithDedicatedCluster())
			runShardClosureIteration(t, env)
		})
	}
}

func runShardClosureIteration(t *testing.T, env testcore.Env) {
	t.Helper()

	logCap := newLogCapture()
	shardClosed := atomic.Bool{}

	interceptor := &speculativeInterceptor{
		triggerCh: make(chan struct{}, 10),
		onGetHistory: func() {
			if shardClosed.CompareAndSwap(false, true) {
				// Close the shard on the FIRST GetWorkflowExecutionHistory call.
				// This causes the shard to reopen with fresh mutable state from
				// persistence, losing the in-memory speculative events.
				env.CloseShard(env.NamespaceID().String(), t.Name()+"-wf")
			}
		},
	}

	sdkClient, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  env.GetTestCluster().Host().FrontendGRPCAddress(),
		Namespace: env.Namespace().String(),
		Logger:    logCap,
		ConnectionOptions: sdkclient.ConnectionOptions{
			DialOptions: []grpc.DialOption{
				grpc.WithUnaryInterceptor(interceptor.unary()),
			},
		},
	})
	require.NoError(t, err)
	defer sdkClient.Close()

	tq := fmt.Sprintf("shard-close-%s", uuid.NewString()[:8])
	wfID := t.Name() + "-wf"
	w := sdkworker.New(sdkClient, tq, sdkworker.Options{})
	w.RegisterWorkflowWithOptions(speculativeWFTWorkflow, workflow.RegisterOptions{Name: "speculativeWFTWorkflow"})
	require.NoError(t, w.Start())
	defer w.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	run, err := sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:                  wfID,
		TaskQueue:           tq,
		WorkflowTaskTimeout: 10 * time.Second,
		WorkflowRunTimeout:  60 * time.Second,
	}, "speculativeWFTWorkflow")
	require.NoError(t, err)

	// Wait for first WFT (sticky established).
	time.Sleep(500 * time.Millisecond)

	// Purge cache so next WFT on sticky queue triggers a cache miss → GetWorkflowExecutionHistory.
	sdkworker.PurgeStickyWorkflowCache()

	logCap.reset()
	interceptor.armed.Store(true)

	// Send Update → speculative WFT on sticky queue → cache miss → GetWorkflowExecutionHistory.
	// The interceptor closes the shard before the GetHistory RPC executes, so the shard reopens
	// with fresh mutable state that has no speculative events.
	handle, err := sdkClient.UpdateWorkflow(ctx, sdkclient.UpdateWorkflowOptions{
		WorkflowID:   wfID,
		RunID:        run.GetRunID(),
		UpdateName:   "myUpdate",
		WaitForStage: sdkclient.WorkflowUpdateStageCompleted,
	})
	if err != nil {
		t.Logf("Update returned error: %v", err)
	} else {
		_ = handle.Get(ctx, nil)
	}

	// Give time for the SDK to process the retry (if the first attempt failed).
	time.Sleep(3 * time.Second)

	interceptor.armed.Store(false)

	if logCap.containsPrematureEOS() {
		t.Logf("REPRODUCED premature end of stream with shard closure!")
	} else {
		t.Logf("No repro (getHistoryCount=%d)", interceptor.getHistoryCount.Load())
	}

	_ = sdkClient.SignalWorkflow(ctx, wfID, run.GetRunID(), "work", "done")
	_ = sdkClient.TerminateWorkflow(ctx, wfID, run.GetRunID(), "test cleanup")
}

// ---------------------------------------------------------------------------
// gRPC interceptor: delay GetWorkflowExecutionHistory / trigger callbacks
// ---------------------------------------------------------------------------

type speculativeInterceptor struct {
	getHistoryDelay time.Duration
	triggerCh       chan struct{}
	onGetHistory    func()
	getHistoryCount atomic.Int32
	armed           atomic.Bool
}

func (i *speculativeInterceptor) unary() grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req, reply interface{},
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		if i.armed.Load() && strings.HasSuffix(method, "GetWorkflowExecutionHistory") {
			i.getHistoryCount.Add(1)
			select {
			case i.triggerCh <- struct{}{}:
			default:
			}
			if i.onGetHistory != nil {
				i.onGetHistory()
			}
			if i.getHistoryDelay > 0 {
				time.Sleep(i.getHistoryDelay)
			}
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}
