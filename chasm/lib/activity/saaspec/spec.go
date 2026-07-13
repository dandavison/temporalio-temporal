// Package saaspec is the executable behavior specification for standalone-activity
// operator commands (pause / unpause / reset / update-options) and the worker-driven
// transitions around them.
//
// It is only used to check the implementation and is never part of the code under test.
// Model() states what should happen; a separate harness drives the real server and
// asserts that the observed internal state equals what Model() predicts. See
// .task/saa-verification-plan.md.
//
// Authoring rule: write what the behavior SHOULD be, without consulting the
// implementation. Divergences between this spec and the code are the findings we want.
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

// Dispatchability says whether a SCHEDULED attempt's next dispatch is available to a worker now,
// or still delayed by a start_delay or a retry backoff. It is NOT readable via ReadComponent — dispatch_time
// is a wall-clock value that looks identical before and after it elapses — so it is excluded from
// SameObserved and the harness verifies it by polling (Dispatchable <=> a poll returns a task).
// Its zero value, Dispatchable, is also the canonical value for any non-SCHEDULED status.
//
// Not to be confused with a "deferred" pause/reset, which is an operator command received mid-attempt
// whose effect is applied when the attempt ends (see ResetKeepPaused, applyDeferredReset).
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

// AbstractState is the EXACT projection of observable internal state that the spec
// predicts. Every field is deterministic across replay-from-fresh (nothing here depends
// on wall-clock time or run IDs) and readable via ReadComponent, so the oracle is exact
// equality. Keep every field a scalar — no pointers/slices/maps — so that `n := s` is a
// true independent copy.
type AbstractState struct {
	Status               Status
	Count                int32 // attempt.count
	Stamp                int32 // attempt.stamp
	ScheduleToCloseStamp int32 // schedule_to_close_stamp
	ResetKeepPaused      bool
	ResetHeartbeats      bool
	ResetRestoreOptions  bool
	FirstAttemptStarted  bool
	// DispatchTimeSet is whether a dispatch time is recorded (attempt.dispatch_time != nil) — the
	// EXISTENCE of a dispatch, observable via ReadComponent. Distinct from Dispatchability below,
	// which is the READINESS of that dispatch: a start-delayed attempt has DispatchTimeSet=true and
	// Dispatchability=StartDelayPending.
	DispatchTimeSet bool

	// Dispatchability is latent (poll-observable, not ReadComponent-observable); see the
	// Dispatchability type. It is excluded from SameObserved and verified by polling.
	Dispatchability Dispatchability
}

// SameObserved reports whether two states agree on every field readable via ReadComponent. The
// latent Dispatchability field is excluded — a poll, not ReadComponent, reveals it — so the exact-equality
// oracle compares only what the server actually persists observably.
func (s AbstractState) SameObserved(o AbstractState) bool {
	s.Dispatchability = Dispatchable
	o.Dispatchability = Dispatchable
	return s == o
}

// Config captures the start-time options that change transition behavior. The graph traversal
// runs the full search once per template (see the plan's config-template table).
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

	// Timer firings, modeled as events so the timer tests are spec-driven: Model() defines the
	// outcome of each firing per status, exactly like an RPC event. The harness triggers a firing
	// by configuring the matching timeout short and waiting for it to elapse.
	ScheduleToStartFires
	ScheduleToCloseFires
	StartToCloseFires
	HeartbeatFires

	// Dispatch-delay clock firings, modeled as events like the timeouts: the harness triggers one by
	// configuring the matching delay/backoff short and waiting for it to elapse. When it fires the
	// delayed dispatch becomes available (Dispatchability -> Dispatchable); the status is unchanged, so the
	// only observable is that a subsequent Poll now returns a task.
	StartDelayElapses
	BackoffElapses
)

// Event carries the variant flags that affect the outcome. Leave irrelevant flags zero.
type Event struct {
	Kind EventKind

	Retryable       bool // RespondFailed: the failure sent is retryable (NonRetryable=false). Whether the activity actually retries also depends on cfg.MaxAttempts and s.Count, which Model() decides.
	KeepPaused      bool // Reset
	RestoreOriginal bool // Reset / UpdateOptions
	ResetHeartbeat  bool // Reset / Unpause
	ResetAttempts   bool // Unpause
	SameRequestID   bool // Pause / Terminate / RequestCancel: repeat of the previous op's request id
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

// Abstract maps the observed internal snapshot onto the spec's AbstractState. The harness
// calls it to convert what it reads from the server into the value it compares with Model().
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
