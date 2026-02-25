package tests

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	sdklog "go.temporal.io/sdk/log"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/grpc"
)

// TestPrematureEndOfStream reproduces the "premature end of stream" bug (ACT-536).
//
// Mechanism: speculative WFT metadata is never persisted (RecordWorkflowTaskStarted
// sets updateAction.Noop=true for speculative WFTs). Closing the shard drops the
// in-memory speculative events. When the SDK has a sticky cache miss and calls
// GetWorkflowExecutionHistory, the reopened shard's mutable state has no speculative
// events to append, producing a 2-event gap.
func TestPrematureEndOfStream(t *testing.T) {
	env := testcore.NewEnv(t, testcore.WithDedicatedCluster())

	logCap := &logCapture{}
	armed := atomic.Bool{}
	shardClosed := atomic.Bool{}
	wfID := fmt.Sprintf("eos-%s", uuid.NewString()[:8])

	interceptor := grpc.WithUnaryInterceptor(func(
		ctx context.Context, method string, req, reply interface{},
		cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption,
	) error {
		if armed.Load() &&
			strings.HasSuffix(method, "GetWorkflowExecutionHistory") &&
			shardClosed.CompareAndSwap(false, true) {
			env.CloseShard(env.NamespaceID().String(), wfID)
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	})

	client, err := sdkclient.Dial(sdkclient.Options{
		HostPort:          env.GetTestCluster().Host().FrontendGRPCAddress(),
		Namespace:         env.Namespace().String(),
		Logger:            logCap,
		ConnectionOptions: sdkclient.ConnectionOptions{DialOptions: []grpc.DialOption{interceptor}},
	})
	require.NoError(t, err)
	defer client.Close()

	tq := fmt.Sprintf("eos-%s", uuid.NewString()[:8])
	w := sdkworker.New(client, tq, sdkworker.Options{})
	w.RegisterWorkflowWithOptions(
		func(ctx workflow.Context) error {
			_ = workflow.SetUpdateHandler(ctx, "u", func(ctx workflow.Context) error { return nil })
			workflow.GetSignalChannel(ctx, "done").Receive(ctx, nil)
			return nil
		},
		workflow.RegisterOptions{Name: "wf"},
	)
	require.NoError(t, w.Start())
	defer w.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	run, err := client.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID: wfID, TaskQueue: tq, WorkflowRunTimeout: 60 * time.Second,
	}, "wf")
	require.NoError(t, err)

	// Wait for first WFT to complete (establishes sticky binding).
	require.Eventually(t, func() bool {
		resp, err := env.FrontendClient().DescribeWorkflowExecution(ctx,
			&workflowservice.DescribeWorkflowExecutionRequest{
				Namespace: env.Namespace().String(),
				Execution: &commonpb.WorkflowExecution{WorkflowId: wfID, RunId: run.GetRunID()},
			})
		return err == nil && resp.GetWorkflowExecutionInfo().GetHistoryLength() >= 4
	}, 5*time.Second, 50*time.Millisecond)

	// Purge SDK sticky cache → next WFT on sticky queue will be a cache miss.
	sdkworker.PurgeStickyWorkflowCache()
	armed.Store(true)

	// Send Update → speculative WFT on sticky queue → SDK cache miss →
	// GetWorkflowExecutionHistory. The interceptor closes the shard before the
	// RPC, so speculative events are lost.
	handle, err := client.UpdateWorkflow(ctx, sdkclient.UpdateWorkflowOptions{
		WorkflowID: wfID, RunID: run.GetRunID(),
		UpdateName: "u", WaitForStage: sdkclient.WorkflowUpdateStageCompleted,
	})
	if err == nil {
		_ = handle.Get(ctx, nil)
	}

	require.Eventually(t, func() bool {
		return logCap.contains("premature end of stream")
	}, 10*time.Second, 100*time.Millisecond,
		"expected SDK to log 'premature end of stream'")

	_ = client.TerminateWorkflow(ctx, wfID, run.GetRunID(), "cleanup")
}

type logCapture struct {
	mu       sync.Mutex
	messages []string
}

var _ sdklog.Logger = (*logCapture)(nil)

func (l *logCapture) Debug(string, ...interface{}) {}
func (l *logCapture) Info(string, ...interface{})  {}
func (l *logCapture) Warn(msg string, kv ...interface{}) {
	l.mu.Lock()
	l.messages = append(l.messages, fmt.Sprintf("%s %v", msg, kv))
	l.mu.Unlock()
}
func (l *logCapture) Error(msg string, kv ...interface{}) {
	l.mu.Lock()
	l.messages = append(l.messages, fmt.Sprintf("%s %v", msg, kv))
	l.mu.Unlock()
}
func (l *logCapture) With(...interface{}) sdklog.Logger { return l }
func (l *logCapture) contains(s string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, m := range l.messages {
		if strings.Contains(m, s) {
			return true
		}
	}
	return false
}
