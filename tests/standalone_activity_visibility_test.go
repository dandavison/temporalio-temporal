package tests

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm/lib/activity"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
	"go.uber.org/fx"
	"google.golang.org/protobuf/types/known/durationpb"
)

type standaloneActivityVisibilitySuite struct {
	testcore.FunctionalTestBase
	tv         *testvars.TestVars
	recorder   *visibilityRecorder
}

func TestStandaloneActivityVisibilitySuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(standaloneActivityVisibilitySuite))
}

func (s *standaloneActivityVisibilitySuite) SetupSuite() {
	s.recorder = &visibilityRecorder{}
	recorder := s.recorder
	s.FunctionalTestBase.SetupSuiteWithCluster(
		testcore.WithFxOptionsForService(primitives.HistoryService,
			fx.Decorate(func(mgr manager.VisibilityManager) manager.VisibilityManager {
				recorder.VisibilityManager = mgr
				return recorder
			}),
		),
	)
	s.OverrideDynamicConfig(dynamicconfig.EnableChasm, true)
	s.OverrideDynamicConfig(activity.Enabled, true)
}

func (s *standaloneActivityVisibilitySuite) SetupTest() {
	s.FunctionalTestBase.SetupTest()
	s.tv = testvars.New(s.T())
	s.recorder.reset()
}

func (s *standaloneActivityVisibilitySuite) TestRetryVisibilityOperations() {
	t := s.T()
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()

	activityID := testcore.RandomizeStr(t.Name())
	taskQueue := testcore.RandomizeStr(t.Name())
	const totalAttempts = 10

	// Start the activity with a retry policy allowing 10 attempts.
	startResp, err := s.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
		Namespace:    s.Namespace().String(),
		ActivityId:   activityID,
		ActivityType: s.tv.ActivityType(),
		Identity:     s.tv.WorkerIdentity(),
		Input:        payloads.EncodeString("visibility-test-input"),
		TaskQueue:    &taskqueuepb.TaskQueue{Name: taskQueue},
		StartToCloseTimeout: durationpb.New(defaultStartToCloseTimeout),
		RetryPolicy: &commonpb.RetryPolicy{
			InitialInterval:    durationpb.New(1 * time.Millisecond),
			BackoffCoefficient: 1.0,
			MaximumAttempts:    totalAttempts,
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, startResp.GetRunId())

	// Fail attempts 1 through 9.
	for attempt := int32(1); attempt < totalAttempts; attempt++ {
		pollResp, err := s.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: s.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  s.tv.WorkerIdentity(),
		})
		require.NoError(t, err)
		require.EqualValues(t, attempt, pollResp.Attempt)

		_, err = s.FrontendClient().RespondActivityTaskFailed(ctx, &workflowservice.RespondActivityTaskFailedRequest{
			Namespace: s.Namespace().String(),
			TaskToken: pollResp.TaskToken,
			Identity:  s.tv.WorkerIdentity(),
			Failure: &failurepb.Failure{
				Message: "retryable failure",
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
					ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{NonRetryable: false},
				},
			},
		})
		require.NoError(t, err)
	}

	// Succeed on attempt 10.
	pollResp, err := s.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
		Namespace: s.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:  s.tv.WorkerIdentity(),
	})
	require.NoError(t, err)
	require.EqualValues(t, totalAttempts, pollResp.Attempt)

	_, err = s.FrontendClient().RespondActivityTaskCompleted(ctx, &workflowservice.RespondActivityTaskCompletedRequest{
		Namespace: s.Namespace().String(),
		TaskToken: pollResp.TaskToken,
		Result:    payloads.EncodeString("done"),
		Identity:  s.tv.WorkerIdentity(),
	})
	require.NoError(t, err)

	// Wait for the close visibility record to appear.
	s.Eventually(func() bool {
		return s.recorder.hasOperation("RecordWorkflowExecutionClosed")
	}, testcore.WaitForESToSettle, 100*time.Millisecond)

	// Print all recorded visibility operations as JSON.
	ops := s.recorder.operations()
	jsonBytes, err := json.MarshalIndent(ops, "", "  ")
	require.NoError(t, err)
	t.Logf("Visibility operations (%d total):\n%s", len(ops), string(jsonBytes))
}

// visibilityRecord captures a single visibility write operation.
type visibilityRecord struct {
	Operation        string            `json:"operation"`
	WorkflowID       string            `json:"workflow_id"`
	RunID            string            `json:"run_id"`
	WorkflowTypeName string            `json:"workflow_type_name"`
	Status           string            `json:"status"`
	TaskQueue        string            `json:"task_queue"`
	StartTime        time.Time         `json:"start_time"`
	CloseTime        *time.Time        `json:"close_time,omitempty"`
	ExecutionDuration *time.Duration    `json:"execution_duration,omitempty"`
	SearchAttributes map[string]string `json:"search_attributes,omitempty"`
	MemoKeys         []string          `json:"memo_keys,omitempty"`
}

func baseToRecord(op string, base *manager.VisibilityRequestBase) visibilityRecord {
	rec := visibilityRecord{
		Operation:        op,
		WorkflowID:       base.Execution.GetWorkflowId(),
		RunID:            base.Execution.GetRunId(),
		WorkflowTypeName: base.WorkflowTypeName,
		Status:           base.Status.String(),
		TaskQueue:        base.TaskQueue,
		StartTime:        base.StartTime,
	}
	if base.SearchAttributes != nil {
		rec.SearchAttributes = make(map[string]string, len(base.SearchAttributes.IndexedFields))
		for k, v := range base.SearchAttributes.IndexedFields {
			rec.SearchAttributes[k] = string(v.GetData())
		}
	}
	if base.Memo != nil {
		for k := range base.Memo.Fields {
			rec.MemoKeys = append(rec.MemoKeys, k)
		}
	}
	return rec
}

// visibilityRecorder wraps a VisibilityManager, recording all write operations.
type visibilityRecorder struct {
	manager.VisibilityManager
	mu   sync.Mutex
	recs []visibilityRecord
}

func (r *visibilityRecorder) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.recs = nil
}

func (r *visibilityRecorder) record(rec visibilityRecord) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.recs = append(r.recs, rec)
}

func (r *visibilityRecorder) operations() []visibilityRecord {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]visibilityRecord, len(r.recs))
	copy(out, r.recs)
	return out
}

func (r *visibilityRecorder) hasOperation(op string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, rec := range r.recs {
		if rec.Operation == op {
			return true
		}
	}
	return false
}

func (r *visibilityRecorder) RecordWorkflowExecutionStarted(ctx context.Context, req *manager.RecordWorkflowExecutionStartedRequest) error {
	r.record(baseToRecord("RecordWorkflowExecutionStarted", req.VisibilityRequestBase))
	return r.VisibilityManager.RecordWorkflowExecutionStarted(ctx, req)
}

func (r *visibilityRecorder) RecordWorkflowExecutionClosed(ctx context.Context, req *manager.RecordWorkflowExecutionClosedRequest) error {
	rec := baseToRecord("RecordWorkflowExecutionClosed", req.VisibilityRequestBase)
	rec.CloseTime = &req.CloseTime
	dur := req.ExecutionDuration
	rec.ExecutionDuration = &dur
	r.record(rec)
	return r.VisibilityManager.RecordWorkflowExecutionClosed(ctx, req)
}

func (r *visibilityRecorder) UpsertWorkflowExecution(ctx context.Context, req *manager.UpsertWorkflowExecutionRequest) error {
	r.record(baseToRecord("UpsertWorkflowExecution", req.VisibilityRequestBase))
	return r.VisibilityManager.UpsertWorkflowExecution(ctx, req)
}

func (r *visibilityRecorder) DeleteWorkflowExecution(ctx context.Context, req *manager.VisibilityDeleteWorkflowExecutionRequest) error {
	r.record(visibilityRecord{
		Operation:  "DeleteWorkflowExecution",
		WorkflowID: req.WorkflowID,
		RunID:      req.RunID,
	})
	return r.VisibilityManager.DeleteWorkflowExecution(ctx, req)
}
