package saaspec

import (
	enumspb "go.temporal.io/api/enums/v1"
)

// Spec for values the server returns to a caller (not persisted state), computed from the
// current AbstractState. The harness calls the matching function after the RPC and asserts the
// real response matches.
//
// Covered: heartbeat response flags (ExpectedHeartbeatFlags) and the Describe status/run-state
// projection (ExpectedDescribe). RecordActivityTaskStarted .Attempt is checked directly against
// Count. UpdateActivityExecutionOptions response values are left to dedicated update-options tests.

// HeartbeatFlags are the worker-facing flags on RecordActivityTaskHeartbeatResponse.
type HeartbeatFlags struct {
	CancelRequested bool
	ActivityPaused  bool
	ActivityReset   bool
}

// ExpectedHeartbeatFlags predicts the heartbeat-response flags for a token-valid status
// (Started, CancelRequested, PauseRequested, ResetRequested). Other statuses reject the
// heartbeat with NotFound and do not call this.
func ExpectedHeartbeatFlags(s AbstractState) HeartbeatFlags {
	switch s.Status {
	case Started:
		return HeartbeatFlags{
			ActivityPaused:  false,
			ActivityReset:   false,
			CancelRequested: false,
		}
	case CancelRequested:
		return HeartbeatFlags{
			ActivityPaused:  false,
			ActivityReset:   false,
			CancelRequested: true,
		}
	case ResetRequested:
		return HeartbeatFlags{
			ActivityPaused:  s.ResetKeepPaused, // TODO(dan): the implementation currently sets both flags; but is this a confusing message to the worker?
			ActivityReset:   true,
			CancelRequested: false,
		}
	case PauseRequested:
		// TODO(dan): our code honors a reset request while in PauseRequested; just want to
		// double-check that's intentional. If so need to decide on spec for heartbeat flags.
		return HeartbeatFlags{
			ActivityPaused:  true,
			ActivityReset:   false,
			CancelRequested: false,
		}
	default:
		panic("ExpectedHeartbeatFlags: not a token-valid status: " + s.Status.String())
	}
}

// ExpectedDescribe predicts the public execution status and pending-activity run state that
// DescribeActivityExecution reports for an activity in state s.
func ExpectedDescribe(s AbstractState) (enumspb.ActivityExecutionStatus, enumspb.PendingActivityState) {
	switch s.Status {
	case Scheduled:
		return enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING, enumspb.PENDING_ACTIVITY_STATE_SCHEDULED
	case Started:
		return enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING, enumspb.PENDING_ACTIVITY_STATE_STARTED
	case Completed:
		return enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED, enumspb.PENDING_ACTIVITY_STATE_UNSPECIFIED
	case Failed:
		return enumspb.ACTIVITY_EXECUTION_STATUS_FAILED, enumspb.PENDING_ACTIVITY_STATE_UNSPECIFIED
	case CancelRequested:
		return enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING, enumspb.PENDING_ACTIVITY_STATE_CANCEL_REQUESTED
	case Canceled:
		return enumspb.ACTIVITY_EXECUTION_STATUS_CANCELED, enumspb.PENDING_ACTIVITY_STATE_UNSPECIFIED
	case Terminated:
		return enumspb.ACTIVITY_EXECUTION_STATUS_TERMINATED, enumspb.PENDING_ACTIVITY_STATE_UNSPECIFIED
	case TimedOut:
		return enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT, enumspb.PENDING_ACTIVITY_STATE_UNSPECIFIED
	case PauseRequested:
		return enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING, enumspb.PENDING_ACTIVITY_STATE_PAUSE_REQUESTED
	case Paused:
		return enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING, enumspb.PENDING_ACTIVITY_STATE_PAUSED
	case ResetRequested:
		return enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING, enumspb.PENDING_ACTIVITY_STATE_STARTED
	default:
		panic("Unexpected status: " + s.Status.String())
	}
}
