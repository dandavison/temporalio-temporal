package tests

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/retrypolicy"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

// A standalone activity whose StartToClose or Heartbeat timeout type is listed in the retry policy's
// NonRetryableErrorTypes must fail terminally when that timeout fires, rather than retrying — matching
// workflow activities (service/history/workflow/retry.go isRetryable). The timeout-task handlers used
// to reschedule unconditionally, so the activity retried instead; TestTimeoutNonRetryable asserts the
// terminal TimedOut outcome, failing until the fix.

// saaActivity addresses one driven activity for follow-up RPCs.
type saaActivity struct {
	activityID string
	taskQueue  string
	runID      string
	token      []byte
}

const (
	saaReproTimeout = 2 * time.Second // the timeout under test, kept short so it fires within the test
	saaReproSettle  = 3 * time.Second // slack so the wait outlasts the timeout's firing instant
)

func (s *standaloneActivityTestSuite) TestTimeoutNonRetryable() {
	env := s.newTestEnv()
	t := s.T()

	t.Run("StartToClose", func(t *testing.T) {
		a := s.start_Poll_StartToCloseTimeoutElapses(t, env)
		require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT, s.describeActivity(t, env, a).GetInfo().GetStatus(),
			"a StartToClose timeout marked non-retryable must fail the activity, not retry it")
	})

	t.Run("Heartbeat", func(t *testing.T) {
		a := s.start_Poll_HeartbeatTimeoutElapses(t, env)
		require.Equal(t, enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT, s.describeActivity(t, env, a).GetInfo().GetStatus(),
			"a Heartbeat timeout marked non-retryable must fail the activity, not retry it")
	})
}

// start_Poll_StartToCloseTimeoutElapses starts an activity whose StartToClose timeout is short and
// marked non-retryable, polls it into STARTED, and waits for the StartToClose timeout to fire.
func (s *standaloneActivityTestSuite) start_Poll_StartToCloseTimeoutElapses(t *testing.T, env *standaloneActivityEnv) *saaActivity {
	a := s.startNonRetryableTimeout(t, env, enumspb.TIMEOUT_TYPE_START_TO_CLOSE)
	s.pollTask(t, env, a)
	time.Sleep(saaReproTimeout + saaReproSettle)
	return a
}

// start_Poll_HeartbeatTimeoutElapses starts an activity whose Heartbeat timeout is short and marked
// non-retryable, polls it into STARTED, and (sending no heartbeat) waits for the timeout to fire.
func (s *standaloneActivityTestSuite) start_Poll_HeartbeatTimeoutElapses(t *testing.T, env *standaloneActivityEnv) *saaActivity {
	a := s.startNonRetryableTimeout(t, env, enumspb.TIMEOUT_TYPE_HEARTBEAT)
	s.pollTask(t, env, a)
	time.Sleep(saaReproTimeout + saaReproSettle)
	return a
}

// startNonRetryableTimeout starts an activity with the given timeout short and listed in
// NonRetryableErrorTypes; every other timeout is long. Copied from the SAA driver's start.
func (s *standaloneActivityTestSuite) startNonRetryableTimeout(t *testing.T, env *standaloneActivityEnv, timeoutType enumspb.TimeoutType) *saaActivity {
	id := testcore.RandomizeStr(t.Name())
	req := &workflowservice.StartActivityExecutionRequest{
		Namespace:           env.Namespace().String(),
		ActivityId:          id,
		ActivityType:        env.Tv().ActivityType(),
		Identity:            "worker",
		Input:               defaultInput,
		TaskQueue:           &taskqueuepb.TaskQueue{Name: id},
		StartToCloseTimeout: durationpb.New(time.Hour),
		RetryPolicy: &commonpb.RetryPolicy{
			InitialInterval:        durationpb.New(200 * time.Millisecond),
			BackoffCoefficient:     1.0,
			MaximumInterval:        durationpb.New(200 * time.Millisecond),
			MaximumAttempts:        3,
			NonRetryableErrorTypes: []string{retrypolicy.TimeoutFailureTypePrefix + timeoutType.String()},
		},
		RequestId: uuid.NewString(),
	}
	switch timeoutType {
	case enumspb.TIMEOUT_TYPE_START_TO_CLOSE:
		req.StartToCloseTimeout = durationpb.New(saaReproTimeout)
	case enumspb.TIMEOUT_TYPE_HEARTBEAT:
		req.HeartbeatTimeout = durationpb.New(saaReproTimeout)
	}
	resp, err := env.FrontendClient().StartActivityExecution(s.Context(), req)
	require.NoError(t, err)
	return &saaActivity{activityID: id, taskQueue: id, runID: resp.RunId}
}

// pollTask dispatches the pending task to a poller, capturing its token (SCHEDULED -> STARTED).
// Copied from the SAA driver's pollForTask.
func (s *standaloneActivityTestSuite) pollTask(t *testing.T, env *standaloneActivityEnv, a *saaActivity) {
	ctx, cancel := context.WithTimeout(s.Context(), 10*time.Second)
	defer cancel()
	resp, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: a.taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:  "worker",
	})
	require.NoError(t, err)
	require.NotEmpty(t, resp.GetActivityId(), "no task was dispatched")
	a.token = resp.GetTaskToken()
}

// describeActivity returns the activity's public DescribeActivityExecution response, including the
// terminal outcome. Copied from the SAA driver's describe.
func (s *standaloneActivityTestSuite) describeActivity(t *testing.T, env *standaloneActivityEnv, a *saaActivity) *workflowservice.DescribeActivityExecutionResponse {
	resp, err := env.FrontendClient().DescribeActivityExecution(s.Context(), &workflowservice.DescribeActivityExecutionRequest{
		Namespace:          env.Namespace().String(),
		ActivityId:         a.activityID,
		RunId:              a.runID,
		IncludeOutcome:     true,
		IncludeLastFailure: true,
	})
	require.NoError(t, err)
	return resp
}
