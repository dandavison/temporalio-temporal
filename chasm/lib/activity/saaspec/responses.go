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
// TODO(spec): fill this in. Each flag is a pure function of s.Status, and one case also
// depends on s.ResetKeepPaused. Decide the exact conditions and return the three booleans.
// For example, consider whether ActivityPaused should be true in ResetRequested only when
// the reset kept the activity paused, and whether both ActivityReset and ActivityPaused can
// be true at the same time.
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
			ActivityPaused:  false,
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
		panic("TODO(spec): ExpectedHeartbeatFlags for status " + s.Status.String())
	}
}

// ExpectedDescribe predicts the public execution status and pending-activity run state that
// DescribeActivityExecution reports for an activity in state s. The explorer calls Describe
// and asserts the reported status and run state equal these. This checks the internal ->
// public projection, which is user-facing and only observable through Describe.
//
// TODO(spec): fill this in — map each internal Status to the public execution status and
// run state you intend users to see. Write what SHOULD be shown; do not copy the code's
// mapping, or the check becomes circular. Note that several internal statuses collapse onto
// the same public value (for example, an activity that is running is reported the same way
// however it reached that point).
func ExpectedDescribe(s AbstractState) (enumspb.ActivityExecutionStatus, enumspb.PendingActivityState) {
	panic("TODO(spec): ExpectedDescribe for status " + s.Status.String())
}
