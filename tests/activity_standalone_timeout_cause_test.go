package tests

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

// A standalone activity that exhausts its retries (or its schedule-to-close deadline) after a failed
// attempt closes as TimedOut. That terminal timeout must chain the underlying application failure that
// drove the retries as its Cause, matching workflow activities (mutable_state_impl.go
// AddActivityTaskTimedOutEvent, which sets timeoutFailure.Cause so SDKs can surface the real failure —
// see temporalio/temporal#3667). The timeout failures were built with no Cause, so
// TestTimeoutPreservesUnderlyingFailureCause fails until the fix.

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

func (s *standaloneActivityTestSuite) TestTimeoutPreservesUnderlyingFailureCause() {
	env := s.newTestEnv()
	t := s.T()

	// Retries are exhausted by a StartToClose timeout on the final attempt.
	t.Run("StartToClose", func(t *testing.T) {
		failure := s.start_Poll_FailRetryably_Poll_StartToCloseTimeoutElapses(t, env).GetOutcome().GetFailure()
		require.NotNil(t, failure.GetTimeoutFailureInfo(), "terminal failure should be a timeout")
		require.NotNil(t, failure.GetCause().GetApplicationFailureInfo(),
			"the terminal timeout must chain the underlying application failure as its Cause")
	})

	// The schedule-to-close deadline closes the activity while it is backing off to retry — a distinct
	// code path (recordScheduleToStartOrCloseTimeoutFailure) that must also chain the cause.
	t.Run("ScheduleToClose", func(t *testing.T) {
		failure := s.start_Poll_FailRetryably_ScheduleToCloseTimeoutElapses(t, env).GetOutcome().GetFailure()
		require.NotNil(t, failure.GetTimeoutFailureInfo(), "terminal failure should be a timeout")
		require.NotNil(t, failure.GetCause().GetApplicationFailureInfo(),
			"the terminal timeout must chain the underlying application failure as its Cause")
	})
}

// start_Poll_FailRetryably_Poll_StartToCloseTimeoutElapses: attempt 1 fails retryably with an
// application error; attempt 2 hangs into its StartToClose timeout, exhausting retries (maxAttempts 2).
func (s *standaloneActivityTestSuite) start_Poll_FailRetryably_Poll_StartToCloseTimeoutElapses(t *testing.T, env *standaloneActivityEnv) *workflowservice.DescribeActivityExecutionResponse {
	a := s.startWithShortTimeout(t, env, enumspb.TIMEOUT_TYPE_START_TO_CLOSE, 2)
	s.pollTask(t, env, a)
	s.respondFailedRetryably(t, env, a)
	s.pollTask(t, env, a)
	time.Sleep(saaReproTimeout + saaReproSettle)
	return s.describeActivity(t, env, a)
}

// start_Poll_FailRetryably_ScheduleToCloseTimeoutElapses: attempt 1 fails retryably; while it backs
// off to retry (retries unlimited), the schedule-to-close deadline elapses and closes the activity.
func (s *standaloneActivityTestSuite) start_Poll_FailRetryably_ScheduleToCloseTimeoutElapses(t *testing.T, env *standaloneActivityEnv) *workflowservice.DescribeActivityExecutionResponse {
	a := s.startWithShortTimeout(t, env, enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE, 0)
	s.pollTask(t, env, a)
	s.respondFailedRetryably(t, env, a)
	time.Sleep(saaReproTimeout + saaReproSettle)
	return s.describeActivity(t, env, a)
}

// startWithShortTimeout starts an activity with the given timeout short and every other timeout long,
// under the given retry cap (0 = unlimited). Copied from the SAA driver's start.
func (s *standaloneActivityTestSuite) startWithShortTimeout(t *testing.T, env *standaloneActivityEnv, timeoutType enumspb.TimeoutType, maxAttempts int32) *saaActivity {
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
			InitialInterval:    durationpb.New(200 * time.Millisecond),
			BackoffCoefficient: 1.0,
			MaximumInterval:    durationpb.New(200 * time.Millisecond),
			MaximumAttempts:    maxAttempts,
		},
		RequestId: uuid.NewString(),
	}
	switch timeoutType {
	case enumspb.TIMEOUT_TYPE_START_TO_CLOSE:
		req.StartToCloseTimeout = durationpb.New(saaReproTimeout)
	case enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE:
		req.ScheduleToCloseTimeout = durationpb.New(saaReproTimeout)
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

// respondFailedRetryably fails the current attempt with a retryable application error. Copied from the
// SAA driver's RespondFailed.
func (s *standaloneActivityTestSuite) respondFailedRetryably(t *testing.T, env *standaloneActivityEnv, a *saaActivity) {
	_, err := env.FrontendClient().RespondActivityTaskFailed(s.Context(), &workflowservice.RespondActivityTaskFailedRequest{
		Namespace: env.Namespace().String(),
		TaskToken: a.token,
		Identity:  "worker",
		Failure: &failurepb.Failure{
			Message:     "drive",
			FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: "drive", NonRetryable: false}},
		},
	})
	require.NoError(t, err)
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
