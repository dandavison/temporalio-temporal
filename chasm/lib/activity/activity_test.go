package activity

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TestHandleStarted_ShouldBeIdempotent demonstrates that HandleStarted should be
// idempotent for retries, but currently isn't.
//
// When matching retries RecordActivityTaskStarted for an activity that is already STARTED
// (e.g., due to network timeout on the first attempt), it should return the existing
// response idempotently. Instead, the current implementation fails.
func TestHandleStarted_ShouldBeIdempotent(t *testing.T) {
	testTime := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

	ctx := &chasm.MockMutableContext{
		MockContext: chasm.MockContext{
			HandleNow: func(chasm.Component) time.Time { return testTime },
			HandleExecutionKey: func() chasm.ExecutionKey {
				return chasm.ExecutionKey{
					BusinessID: "test-activity-id",
					RunID:      "test-run-id",
				}
			},
		},
	}

	// Create an activity that is already in STARTED state
	// (simulating that a previous RecordActivityTaskStarted succeeded)
	attemptState := &activitypb.ActivityAttemptState{
		Count:       1,
		StartedTime: timestamppb.New(testTime.Add(-1 * time.Minute)),
	}

	activity := &Activity{
		ActivityState: &activitypb.ActivityState{
			ActivityType:           &commonpb.ActivityType{Name: "test-activity-type"},
			Status:                 activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
			TaskQueue:              &taskqueuepb.TaskQueue{Name: "test-task-queue"},
			ScheduleToCloseTimeout: durationpb.New(10 * time.Minute),
			ScheduleToStartTimeout: durationpb.New(2 * time.Minute),
			StartToCloseTimeout:    durationpb.New(3 * time.Minute),
			HeartbeatTimeout:       durationpb.New(1 * time.Minute),
			ScheduleTime:           timestamppb.New(testTime.Add(-30 * time.Second)),
		},
		LastAttempt: chasm.NewDataField(ctx, attemptState),
		RequestData: chasm.NewDataField(ctx, &activitypb.ActivityRequestData{
			Input: &commonpb.Payloads{
				Payloads: []*commonpb.Payload{{Data: []byte("test-input")}},
			},
		}),
		Outcome: chasm.NewDataField(ctx, &activitypb.ActivityOutcome{}),
	}

	// Simulate matching retrying RecordActivityTaskStarted with the same request ID
	request := &historyservice.RecordActivityTaskStartedRequest{
		RequestId: "original-request-id",
	}

	// This SHOULD succeed idempotently for a retry with the same request ID
	response, err := activity.HandleStarted(ctx, request)

	require.NoError(t, err, "HandleStarted should be idempotent for retries")
	require.NotNil(t, response)
	require.Equal(t, int32(1), response.Attempt)
}

// TestHandleStarted_SuccessfulTransition verifies that HandleStarted works correctly
// for the normal case of transitioning from SCHEDULED to STARTED.
func TestHandleStarted_SuccessfulTransition(t *testing.T) {
	testTime := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

	ctx := &chasm.MockMutableContext{
		MockContext: chasm.MockContext{
			HandleNow: func(chasm.Component) time.Time { return testTime },
			HandleExecutionKey: func() chasm.ExecutionKey {
				return chasm.ExecutionKey{
					BusinessID: "test-activity-id",
					RunID:      "test-run-id",
				}
			},
		},
	}

	attemptState := &activitypb.ActivityAttemptState{
		Count: 1,
	}

	activity := &Activity{
		ActivityState: &activitypb.ActivityState{
			ActivityType:           &commonpb.ActivityType{Name: "test-activity-type"},
			Status:                 activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
			TaskQueue:              &taskqueuepb.TaskQueue{Name: "test-task-queue"},
			ScheduleToCloseTimeout: durationpb.New(10 * time.Minute),
			ScheduleToStartTimeout: durationpb.New(2 * time.Minute),
			StartToCloseTimeout:    durationpb.New(3 * time.Minute),
			HeartbeatTimeout:       durationpb.New(1 * time.Minute),
			ScheduleTime:           timestamppb.New(testTime.Add(-30 * time.Second)),
		},
		LastAttempt: chasm.NewDataField(ctx, attemptState),
		RequestData: chasm.NewDataField(ctx, &activitypb.ActivityRequestData{
			Input: &commonpb.Payloads{
				Payloads: []*commonpb.Payload{{Data: []byte("test-input")}},
			},
		}),
		Outcome: chasm.NewDataField(ctx, &activitypb.ActivityOutcome{}),
	}

	request := &historyservice.RecordActivityTaskStartedRequest{
		RequestId: "test-request-id",
	}

	response, err := activity.HandleStarted(ctx, request)

	require.NoError(t, err)
	require.NotNil(t, response)
	require.Equal(t, int32(1), response.Attempt)
	require.Equal(t, activitypb.ACTIVITY_EXECUTION_STATUS_STARTED, activity.Status)
}
