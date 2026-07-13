package saaspec

import (
	enumspb "go.temporal.io/api/enums/v1"
)

// This file holds the parts of the spec that are NOT about the persisted state a
// transition leaves behind, but about VALUES the server returns to a caller, computed from
// the current state.
//
// Model() and AbstractState cover only the persisted state that the harness reads with
// ReadComponent and compares after each event. That misses a whole category of behavior:
// information the server hands back in an RPC response, or computes when a read API is
// called. Some of it is the ONLY external evidence of an internal state, so it cannot be
// checked by comparing persisted state alone.
//
// The functions here predict those returned values from AbstractState. The explorer calls
// the relevant one after issuing the corresponding RPC and asserts the real response
// matches.
//
// Category members and where each is verified:
//
//   - Heartbeat response flags (CancelRequested / ActivityPaused / ActivityReset):
//     ExpectedHeartbeatFlags, below. This is the only signal a running worker receives that
//     a pause/reset/cancel was requested. For RESET_REQUESTED it is the only external
//     observable at all, because Describe reports that status as STARTED.
//
//   - Describe public status + run state: ExpectedDescribe, below. The projection of the
//     internal status onto the public enums that users actually see. The mapping has real
//     logic and is only observable through Describe.
//
//   - RecordActivityTaskStarted response .Attempt: checked directly by the explorer as
//     equal to AbstractState.Count after a poll. No spec function is needed — it is just a
//     field we already model. Catches wrong attempt numbering after a reset.
//
//   - UpdateActivityExecutionOptions response option VALUES (merged/normalized timeouts,
//     retry policy, priority, task queue, start delay): deliberately NOT covered here.
//     Tracking option values would grow AbstractState from a small scalar tuple into the
//     full options and pull the merge/normalize logic into the spec. That correctness is
//     left to dedicated update-options tests that compare the response and the Describe
//     output field by field.

// HeartbeatFlags are the worker-facing flags on RecordActivityTaskHeartbeatResponse.
type HeartbeatFlags struct {
	CancelRequested bool
	ActivityPaused  bool
	ActivityReset   bool
}

// ExpectedHeartbeatFlags predicts the heartbeat-response flags for a status in which a
// heartbeat is accepted: the token-valid statuses Started, CancelRequested, PauseRequested,
// and ResetRequested. The explorer sends a heartbeat in one of those statuses and asserts
// the response flags equal this. In any other status it asserts the heartbeat is rejected
// with NotFound, and does not call this function.
//
// How the explorer will use it: after issuing a heartbeat in a token-valid status it builds
// the real HeartbeatFlags from the response and asserts equality with
// ExpectedHeartbeatFlags(currentState). To send a heartbeat it needs the task token from
// the poll that reached STARTED; that token stays valid through
// STARTED -> PauseRequested / ResetRequested / CancelRequested, because Count does not change.
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
