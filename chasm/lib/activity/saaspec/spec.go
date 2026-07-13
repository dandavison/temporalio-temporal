// Package saaspec is the executable behavior specification for standalone-activity
package saaspec

import (
	activitypb "go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
)

// Status mirrors activitypb.ActivityExecutionStatus but is defined locally so the spec
// reads independently of the proto. abstract() maps the observed enum onto it.
type Status int

const (
	Unspecified Status = iota
	Scheduled
	Started
	Completed
	Failed
	CancelRequested
	Canceled
	Terminated
	TimedOut
	PauseRequested
	Paused
	ResetRequested
)

func (s Status) String() string {
	switch s {
	case Unspecified:
		return "Unspecified"
	case Scheduled:
		return "Scheduled"
	case Started:
		return "Started"
	case CancelRequested:
		return "CancelRequested"
	case Completed:
		return "Completed"
	case Failed:
		return "Failed"
	case Canceled:
		return "Canceled"
	case Terminated:
		return "Terminated"
	case TimedOut:
		return "TimedOut"
	case Paused:
		return "Paused"
	case PauseRequested:
		return "PauseRequested"
	case ResetRequested:
		return "ResetRequested"
	default:
		return "Status(?)"
	}
}

// Terminal reports whether the activity has reached an absorbing state.
func (s Status) Terminal() bool {
	switch s {
	case Completed, Failed, Canceled, Terminated, TimedOut:
		return true
	default:
		return false
	}
}

// Dispatchability says whether a SCHEDULED attempt's next dispatch is available now, or still delayed
// by a start_delay or retry backoff. Not readable via ReadComponent (dispatch_time looks identical
// before and after it elapses), so it is excluded from SameObserved and verified by polling. Its zero
// value Dispatchable is also the value for any non-SCHEDULED status. Distinct from a "deferred"
// pause/reset (an operator command applied when the attempt ends; see ResetKeepPaused, applyDeferredReset).
type Dispatchability int

const (
	Dispatchable      Dispatchability = iota // pollable now: a worker poll returns a task
	StartDelayPending                        // first dispatch delayed until schedule_time + start_delay
	BackoffPending                           // retry dispatch delayed until complete_time + retry interval
)

func (d Dispatchability) String() string {
	switch d {
	case Dispatchable:
		return "Dispatchable"
	case StartDelayPending:
		return "StartDelayPending"
	case BackoffPending:
		return "BackoffPending"
	default:
		return "Dispatchability(?)"
	}
}

// AbstractState is the exact projection of observable internal state the spec predicts. Every field is
// replay-deterministic and readable via ReadComponent, so the oracle is exact equality. Keep every
// field a scalar so that `n := s` is an independent copy.
type AbstractState struct {
	Status               Status
	Count                int32 // attempt.count
	Stamp                int32 // attempt.stamp
	ScheduleToCloseStamp int32 // schedule_to_close_stamp
	ResetKeepPaused      bool
	ResetHeartbeats      bool
	ResetRestoreOptions  bool
	FirstAttemptStarted  bool
	// DispatchTimeSet is whether a dispatch time is recorded (attempt.dispatch_time != nil): the
	// existence of a dispatch, observable via ReadComponent. Distinct from Dispatchability, its readiness.
	DispatchTimeSet bool

	// Dispatchability is latent (poll-observable, not ReadComponent-observable), excluded from
	// SameObserved and verified by polling.
	Dispatchability Dispatchability
}

// SameObserved reports whether two states agree on every ReadComponent-readable field, excluding the
// latent Dispatchability.
func (s AbstractState) SameObserved(o AbstractState) bool {
	s.Dispatchability = Dispatchable
	o.Dispatchability = Dispatchable
	return s == o
}

// Config captures the start-time options that change transition behavior.
type Config struct {
	HasScheduleToClose bool
	HasScheduleToStart bool
	HasHeartbeat       bool
	HasStartDelay      bool
	MaxAttempts        int32 // 0 = unlimited
}

// EventKind enumerates the RPC-driven events the spec covers.
type EventKind int

const (
	Poll EventKind = iota // worker poll that transitions Scheduled -> Started
	Heartbeat
	RespondCompleted
	RespondFailed
	RespondCanceled
	RequestCancel
	Terminate
	Pause
	Unpause
	Reset
	UpdateOptions

	// Timeout deadlines elapsing. The event means the configured deadline window has passed in
	// wall-clock, not that the timer fired; Model() decides the effect per status. The harness
	// triggers one by configuring the timeout short and waiting.
	ScheduleToStartElapses
	ScheduleToCloseElapses
	StartToCloseElapses
	HeartbeatElapses

	// Dispatch-delay clocks elapsing. When elapsed the delayed dispatch becomes available
	// (Dispatchability -> Dispatchable); status is unchanged, so the only observable is a subsequent
	// Poll returning a task. The harness triggers one by configuring the delay/backoff short and waiting.
	StartDelayElapses
	BackoffElapses
)

// Event carries the variant flags that affect the outcome. Leave irrelevant flags zero.
type Event struct {
	Kind EventKind

	Retryable       bool // RespondFailed: the failure is retryable. Whether it actually retries also depends on cfg.MaxAttempts and s.Count.
	KeepPaused      bool // Reset
	RestoreOriginal bool // Reset / UpdateOptions
	ResetHeartbeat  bool // Reset / Unpause
	ResetAttempts   bool // Unpause
	SameRequestID   bool // Pause / Terminate / RequestCancel: repeat of the previous op's request id
	SetsStartDelay  bool // UpdateOptions: the update changes start_delay
	// SetsInvalidRetryPolicy: UpdateOptions whose retry-policy subfield merge yields an invalid policy
	// (e.g. initial_interval > maximum_interval). Rejected with InvalidArgument on any non-terminal state.
	SetsInvalidRetryPolicy bool
}

// ErrorKind is the API-level outcome the spec expects for a rejected or no-op call.
type ErrorKind int

const (
	NoError ErrorKind = iota
	FailedPrecondition
	NotFound
	InvalidArgument
)

// Outcome is what the spec says the API + resulting state should be. For a rejected or
// no-op call, Next == the input state (the call must not mutate).
type Outcome struct {
	Next   AbstractState
	Reject ErrorKind
}

// Observed is the internal state the harness reads back via ReadComponent. The reader
// returns Status as the internal proto enum; abstract() maps it onto the spec's Status.
type Observed struct {
	Status               activitypb.ActivityExecutionStatus
	Count                int32
	Stamp                int32
	ScheduleToCloseStamp int32
	ResetKeepPaused      bool
	ResetHeartbeats      bool
	ResetRestoreOptions  bool
	FirstAttemptStarted  bool
	DispatchTimeSet      bool
}

// Abstract maps an observed internal snapshot onto the spec's AbstractState.
func Abstract(o Observed) AbstractState {
	return AbstractState{
		Status:               mapStatus(o.Status),
		Count:                o.Count,
		Stamp:                o.Stamp,
		ScheduleToCloseStamp: o.ScheduleToCloseStamp,
		ResetKeepPaused:      o.ResetKeepPaused,
		ResetHeartbeats:      o.ResetHeartbeats,
		ResetRestoreOptions:  o.ResetRestoreOptions,
		FirstAttemptStarted:  o.FirstAttemptStarted,
		DispatchTimeSet:      o.DispatchTimeSet,
	}
}

func mapStatus(s activitypb.ActivityExecutionStatus) Status {
	switch s {
	case activitypb.ACTIVITY_EXECUTION_STATUS_UNSPECIFIED:
		return Unspecified
	case activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED:
		return Scheduled
	case activitypb.ACTIVITY_EXECUTION_STATUS_STARTED:
		return Started
	case activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED:
		return CancelRequested
	case activitypb.ACTIVITY_EXECUTION_STATUS_COMPLETED:
		return Completed
	case activitypb.ACTIVITY_EXECUTION_STATUS_FAILED:
		return Failed
	case activitypb.ACTIVITY_EXECUTION_STATUS_CANCELED:
		return Canceled
	case activitypb.ACTIVITY_EXECUTION_STATUS_TERMINATED:
		return Terminated
	case activitypb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT:
		return TimedOut
	case activitypb.ACTIVITY_EXECUTION_STATUS_PAUSED:
		return Paused
	case activitypb.ACTIVITY_EXECUTION_STATUS_PAUSE_REQUESTED:
		return PauseRequested
	case activitypb.ACTIVITY_EXECUTION_STATUS_RESET_REQUESTED:
		return ResetRequested
	default:
		return Unspecified
	}
}
