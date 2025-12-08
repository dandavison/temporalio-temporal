package activity

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestHeartbeatTimeoutTask_RescheduledTaskShouldInvalidateOldTask(t *testing.T) {
	// This test demonstrates the bug in Design B without a high-water-mark:
	// When a heartbeat timeout task reschedules itself, the old task should
	// become invalid. Without the high-water-mark check in Validate, the old
	// task remains valid and would be re-executed, potentially causing
	// duplicate timeout processing or incorrect behavior.

	heartbeatTimeout := 10 * time.Second
	attemptStartTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	firstTaskScheduledTime := attemptStartTime.Add(heartbeatTimeout) // T1 scheduled at 12:00:10

	// Simulate a heartbeat arriving at 12:00:05
	heartbeatTime := attemptStartTime.Add(5 * time.Second)

	// T1 fires at 12:00:10, but there was a heartbeat at 12:00:05
	// So the new deadline is 12:00:05 + 10s = 12:00:15
	// T1 should reschedule to T2 at 12:00:15
	taskFireTime := firstTaskScheduledTime

	ctx := &chasm.MockMutableContext{
		MockContext: chasm.MockContext{
			HandleNow: func(chasm.Component) time.Time { return taskFireTime },
		},
	}

	attemptState := &activitypb.ActivityAttemptState{
		Count:       1,
		StartedTime: timestamppb.New(attemptStartTime),
	}
	heartbeatState := &activitypb.ActivityHeartbeatState{
		RecordedTime: timestamppb.New(heartbeatTime),
	}

	activity := &Activity{
		ActivityState: &activitypb.ActivityState{
			RetryPolicy: &commonpb.RetryPolicy{
				MaximumAttempts: 3,
			},
			HeartbeatTimeout:       durationpb.New(heartbeatTimeout),
			ScheduleToCloseTimeout: durationpb.New(1 * time.Hour),
			StartToCloseTimeout:    durationpb.New(30 * time.Minute),
			Status:                 activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
		},
		LastAttempt:   chasm.NewDataField(ctx, attemptState),
		LastHeartbeat: chasm.NewDataField(ctx, heartbeatState),
		Outcome:       chasm.NewDataField(ctx, &activitypb.ActivityOutcome{}),
	}

	executor := newHeartbeatTimeoutTaskExecutor()
	task := &activitypb.HeartbeatTimeoutTask{Attempt: 1}
	taskAttrs := chasm.TaskAttributes{ScheduledTime: firstTaskScheduledTime}

	// Step 1: Validate T1 - should be valid initially
	valid, err := executor.Validate(ctx, activity, taskAttrs, task)
	require.NoError(t, err)
	require.True(t, valid, "T1 should be valid before execution")

	// Step 2: Execute T1 - it should reschedule because deadline hasn't passed
	err = executor.Execute(ctx, activity, taskAttrs, task)
	require.NoError(t, err)

	// Verify T2 was scheduled
	require.Len(t, ctx.Tasks, 1, "should have scheduled exactly one new task (T2)")
	t2 := ctx.Tasks[0]
	expectedT2ScheduledTime := heartbeatTime.Add(heartbeatTimeout) // 12:00:15
	require.Equal(t, expectedT2ScheduledTime, t2.Attributes.ScheduledTime,
		"T2 should be scheduled at heartbeatTime + heartbeatTimeout")

	// Step 3: THE BUG - Validate T1 again after it has rescheduled
	// With the current implementation (no high-water-mark), T1 is STILL valid!
	// This is wrong - T1 should be invalid after it has executed and rescheduled.
	valid, err = executor.Validate(ctx, activity, taskAttrs, task)
	require.NoError(t, err)

	// This test asserts the DESIRED behavior.
	// It currently FAILS, demonstrating the bug in Design B without high-water-mark.
	//
	// The bug: After T1 executes and reschedules to T2, T1's validator still returns
	// true. This means T1 won't be cleaned up by closeTransactionCleanupInvalidTasks
	// and could re-execute on the next physical task fire.
	//
	// Fix: Add high-water-mark tracking (like scheduler's LastProcessedTime).
	// Update HeartbeatTaskLastScheduledTime in Execute, check it in Validate.
	require.False(t, valid,
		"BUG: Old task T1 is still valid after rescheduling to T2. "+
			"T1 should be invalid so it gets cleaned up by closeTransactionCleanupInvalidTasks. "+
			"Fix: add high-water-mark tracking like scheduler's validateTaskHighWaterMark.")
}

func TestHeartbeatTimeoutTask_ExecuteReschedulesWhenHeartbeatReceived(t *testing.T) {
	// Test the happy path: task fires, sees recent heartbeat, reschedules

	heartbeatTimeout := 10 * time.Second
	attemptStartTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	heartbeatTime := attemptStartTime.Add(5 * time.Second) // heartbeat at 12:00:05
	taskFireTime := attemptStartTime.Add(10 * time.Second) // task fires at 12:00:10

	ctx := &chasm.MockMutableContext{
		MockContext: chasm.MockContext{
			HandleNow: func(chasm.Component) time.Time { return taskFireTime },
		},
	}

	attemptState := &activitypb.ActivityAttemptState{
		Count:       1,
		StartedTime: timestamppb.New(attemptStartTime),
	}
	heartbeatState := &activitypb.ActivityHeartbeatState{
		RecordedTime: timestamppb.New(heartbeatTime),
	}

	activity := &Activity{
		ActivityState: &activitypb.ActivityState{
			RetryPolicy: &commonpb.RetryPolicy{
				MaximumAttempts: 3,
			},
			HeartbeatTimeout:       durationpb.New(heartbeatTimeout),
			ScheduleToCloseTimeout: durationpb.New(1 * time.Hour),
			StartToCloseTimeout:    durationpb.New(30 * time.Minute),
			Status:                 activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
		},
		LastAttempt:   chasm.NewDataField(ctx, attemptState),
		LastHeartbeat: chasm.NewDataField(ctx, heartbeatState),
		Outcome:       chasm.NewDataField(ctx, &activitypb.ActivityOutcome{}),
	}

	executor := newHeartbeatTimeoutTaskExecutor()
	task := &activitypb.HeartbeatTimeoutTask{Attempt: 1}

	err := executor.Execute(ctx, activity, chasm.TaskAttributes{}, task)
	require.NoError(t, err)

	// Should have rescheduled
	require.Len(t, ctx.Tasks, 1)
	newTask := ctx.Tasks[0]
	require.IsType(t, &activitypb.HeartbeatTimeoutTask{}, newTask.Payload)

	// New deadline = heartbeatTime + timeout = 12:00:05 + 10s = 12:00:15
	expectedDeadline := heartbeatTime.Add(heartbeatTimeout)
	require.Equal(t, expectedDeadline, newTask.Attributes.ScheduledTime)

	// Activity should still be in STARTED state
	require.Equal(t, activitypb.ACTIVITY_EXECUTION_STATUS_STARTED, activity.Status)
}

func TestHeartbeatTimeoutTask_ExecuteTimesOutWhenNoHeartbeat(t *testing.T) {
	// Test timeout: task fires after deadline with no recent heartbeat

	heartbeatTimeout := 10 * time.Second
	attemptStartTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	// No heartbeat - deadline is attemptStartTime + timeout = 12:00:10
	taskFireTime := attemptStartTime.Add(15 * time.Second) // task fires at 12:00:15 (after deadline)

	ctx := &chasm.MockMutableContext{
		MockContext: chasm.MockContext{
			HandleNow: func(chasm.Component) time.Time { return taskFireTime },
		},
	}

	attemptState := &activitypb.ActivityAttemptState{
		Count:       1,
		StartedTime: timestamppb.New(attemptStartTime),
	}

	activity := &Activity{
		ActivityState: &activitypb.ActivityState{
			RetryPolicy: &commonpb.RetryPolicy{
				MaximumAttempts:    1, // No retries
				InitialInterval:    durationpb.New(1 * time.Second),
				BackoffCoefficient: 2.0,
			},
			HeartbeatTimeout:       durationpb.New(heartbeatTimeout),
			ScheduleToCloseTimeout: durationpb.New(1 * time.Hour),
			StartToCloseTimeout:    durationpb.New(30 * time.Minute),
			Status:                 activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
		},
		LastAttempt: chasm.NewDataField(ctx, attemptState),
		Outcome:     chasm.NewDataField(ctx, &activitypb.ActivityOutcome{}),
	}

	executor := newHeartbeatTimeoutTaskExecutor()
	task := &activitypb.HeartbeatTimeoutTask{Attempt: 1}

	err := executor.Execute(ctx, activity, chasm.TaskAttributes{}, task)
	require.NoError(t, err)

	// Should NOT have rescheduled (deadline passed, no retries)
	require.Empty(t, ctx.Tasks)

	// Activity should be TIMED_OUT
	require.Equal(t, activitypb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT, activity.Status)
}

func TestHeartbeatTimeoutTask_ValidateRejectsWrongAttempt(t *testing.T) {
	ctx := &chasm.MockMutableContext{
		MockContext: chasm.MockContext{
			HandleNow: func(chasm.Component) time.Time { return time.Now() },
		},
	}

	attemptState := &activitypb.ActivityAttemptState{Count: 2} // Current attempt is 2

	activity := &Activity{
		ActivityState: &activitypb.ActivityState{
			Status: activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
		},
		LastAttempt: chasm.NewDataField(ctx, attemptState),
	}

	executor := newHeartbeatTimeoutTaskExecutor()
	task := &activitypb.HeartbeatTimeoutTask{Attempt: 1} // Task from attempt 1

	valid, err := executor.Validate(ctx, activity, chasm.TaskAttributes{}, task)
	require.NoError(t, err)
	require.False(t, valid, "task from old attempt should be invalid")
}

func TestHeartbeatTimeoutTask_ValidateRejectsWrongStatus(t *testing.T) {
	ctx := &chasm.MockMutableContext{
		MockContext: chasm.MockContext{
			HandleNow: func(chasm.Component) time.Time { return time.Now() },
		},
	}

	attemptState := &activitypb.ActivityAttemptState{Count: 1}

	activity := &Activity{
		ActivityState: &activitypb.ActivityState{
			Status: activitypb.ACTIVITY_EXECUTION_STATUS_COMPLETED, // Wrong status
		},
		LastAttempt: chasm.NewDataField(ctx, attemptState),
	}

	executor := newHeartbeatTimeoutTaskExecutor()
	task := &activitypb.HeartbeatTimeoutTask{Attempt: 1}

	valid, err := executor.Validate(ctx, activity, chasm.TaskAttributes{}, task)
	require.NoError(t, err)
	require.False(t, valid, "task should be invalid when activity is completed")
}
