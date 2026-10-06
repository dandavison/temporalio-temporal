package callback

import (
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/common/clock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TestProcessBackoffTask tests the backoff task execution that transitions
// a callback from BACKING_OFF to SCHEDULED state and adds an invocation task.
func TestProcessBackoffTask(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	timeSource := clock.NewEventTimeSource()
	timeSource.Update(time.Now())

	// Create callback in BACKING_OFF state
	callback := &Callback{
		CallbackState: &callbackspb.CallbackState{
			RequestId: "request-id",
			Callback: &callbackspb.Callback{
				Variant: &callbackspb.Callback_Nexus_{
					Nexus: &callbackspb.Callback_Nexus{
						Url: "http://localhost",
					},
				},
			},
			Status:                  callbackspb.CALLBACK_STATUS_BACKING_OFF,
			Attempt:                 1,
			NextAttemptScheduleTime: timestamppb.New(timeSource.Now().Add(time.Minute)),
		},
	}

	// Create mock mutable context
	mockCtx := &chasm.MockMutableContext{
		MockContext: chasm.MockContext{
			HandleNow: func(component chasm.Component) time.Time {
				return timeSource.Now()
			},
			HandleRef: func(component chasm.Component) ([]byte, error) {
				return []byte{}, nil
			},
		},
	}

	handler := backoffTaskHandler{}

	// Execute the backoff task
	task := &callbackspb.BackoffTask{Attempt: 1}
	attrs := chasm.TaskAttributes{Destination: "http://localhost"}
	err := handler.Execute(mockCtx, callback, attrs, task)

	// Verify no error
	require.NoError(t, err)

	// Verify callback transitioned to SCHEDULED state
	require.Equal(t, callbackspb.CALLBACK_STATUS_SCHEDULED, callback.Status)
	require.Nil(t, callback.NextAttemptScheduleTime)

	// Verify an invocation task was added
	require.Len(t, mockCtx.Tasks, 1)
	require.IsType(t, &callbackspb.InvocationTask{}, mockCtx.Tasks[0].Payload)
	invTask := mockCtx.Tasks[0].Payload.(*callbackspb.InvocationTask)
	require.Equal(t, int32(1), invTask.Attempt)
}
