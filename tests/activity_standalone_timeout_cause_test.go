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

// TestTimeoutPreservesUnderlyingFailureCause verifies that when retries are exhausted by a
// StartToClose timeout, or the schedule-to-close deadline closes a backing-off activity, the
// terminal TimedOut failure chains the application failure that drove the retries as its Cause —
// matching workflow activities (mutable_state_impl.go AddActivityTaskTimedOutEvent).
func (s *standaloneActivityTestSuite) TestTimeoutPreservesUnderlyingFailureCause() {
	const (
		shortTimeout  = 2 * time.Second // the timeout under test, kept short so it fires within the test
		timeoutSettle = 3 * time.Second // slack so a wait outlasts the timeout's firing instant
	)

	// driven addresses one activity under test across the follow-up RPCs below.
	type driven struct {
		activityID string
		taskQueue  string
		runID      string
		token      []byte
	}

	startWithShortTimeout := func(s *standaloneActivityTestSuite, t *testing.T, env *standaloneActivityEnv, timeoutType enumspb.TimeoutType, maxAttempts int32) *driven {
		id := testcore.RandomizeStr(t.Name())
		req := &workflowservice.StartActivityExecutionRequest{
			Namespace:           env.Namespace().String(),
			ActivityId:          id,
			ActivityType:        env.Tv().ActivityType(),
			Identity:            defaultIdentity,
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
			req.StartToCloseTimeout = durationpb.New(shortTimeout)
		case enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE:
			req.ScheduleToCloseTimeout = durationpb.New(shortTimeout)
		}
		resp, err := env.FrontendClient().StartActivityExecution(s.Context(), req)
		require.NoError(t, err)
		return &driven{activityID: id, taskQueue: id, runID: resp.RunId}
	}

	pollTask := func(s *standaloneActivityTestSuite, t *testing.T, env *standaloneActivityEnv, a *driven) {
		ctx, cancel := context.WithTimeout(s.Context(), 10*time.Second)
		defer cancel()
		resp, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: a.taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  defaultIdentity,
		})
		require.NoError(t, err)
		require.NotEmpty(t, resp.GetActivityId(), "no task was dispatched")
		a.token = resp.GetTaskToken()
	}

	respondFailedRetryably := func(s *standaloneActivityTestSuite, t *testing.T, env *standaloneActivityEnv, a *driven) {
		_, err := env.FrontendClient().RespondActivityTaskFailed(s.Context(), &workflowservice.RespondActivityTaskFailedRequest{
			Namespace: env.Namespace().String(),
			TaskToken: a.token,
			Identity:  defaultIdentity,
			Failure: &failurepb.Failure{
				Message:     "drive",
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: "drive", NonRetryable: false}},
			},
		})
		require.NoError(t, err)
	}

	describeActivity := func(s *standaloneActivityTestSuite, t *testing.T, env *standaloneActivityEnv, a *driven) *workflowservice.DescribeActivityExecutionResponse {
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

	// Attempt 1 fails retryably with an application error; attempt 2 hangs into its StartToClose
	// timeout, exhausting retries (maxAttempts 2).
	start_Poll_FailRetryably_Poll_StartToCloseTimeoutElapses := func(s *standaloneActivityTestSuite, t *testing.T, env *standaloneActivityEnv) *workflowservice.DescribeActivityExecutionResponse {
		a := startWithShortTimeout(s, t, env, enumspb.TIMEOUT_TYPE_START_TO_CLOSE, 2)
		pollTask(s, t, env, a)
		respondFailedRetryably(s, t, env, a)
		pollTask(s, t, env, a)
		time.Sleep(shortTimeout + timeoutSettle)
		return describeActivity(s, t, env, a)
	}

	// Attempt 1 fails retryably; while it backs off to retry (retries unlimited), the
	// schedule-to-close deadline elapses and closes the activity.
	start_Poll_FailRetryably_ScheduleToCloseTimeoutElapses := func(s *standaloneActivityTestSuite, t *testing.T, env *standaloneActivityEnv) *workflowservice.DescribeActivityExecutionResponse {
		a := startWithShortTimeout(s, t, env, enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE, 0)
		pollTask(s, t, env, a)
		respondFailedRetryably(s, t, env, a)
		time.Sleep(shortTimeout + timeoutSettle)
		return describeActivity(s, t, env, a)
	}

	// Retries are exhausted by a StartToClose timeout on the final attempt.
	s.Run("StartToClose", func(s *standaloneActivityTestSuite) {
		t := s.T()
		env := s.newTestEnv()

		failure := start_Poll_FailRetryably_Poll_StartToCloseTimeoutElapses(s, t, env).GetOutcome().GetFailure()
		require.NotNil(t, failure.GetTimeoutFailureInfo(), "terminal failure should be a timeout")
		require.NotNil(t, failure.GetCause().GetApplicationFailureInfo(),
			"the terminal timeout must chain the underlying application failure as its Cause")
	})

	// The schedule-to-close deadline closes the activity while it is backing off to retry — a
	// distinct code path (recordScheduleToStartOrCloseTimeoutFailure) that must also chain the cause.
	s.Run("ScheduleToClose", func(s *standaloneActivityTestSuite) {
		t := s.T()
		env := s.newTestEnv()

		failure := start_Poll_FailRetryably_ScheduleToCloseTimeoutElapses(s, t, env).GetOutcome().GetFailure()
		require.NotNil(t, failure.GetTimeoutFailureInfo(), "terminal failure should be a timeout")
		require.NotNil(t, failure.GetCause().GetApplicationFailureInfo(),
			"the terminal timeout must chain the underlying application failure as its Cause")
	})
}
