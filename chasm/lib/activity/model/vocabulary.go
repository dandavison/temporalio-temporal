// Package model is an implementation-independent vocabulary for specifying a sequence of events (a
// 'trace') in the lifetime of an activity. Drivers exist that can realize these events for both
// Standalone Activity and Workflow Activity.
package model

import (
	"fmt"
	"strings"

	activitypb "go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
)

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

// Dispatchability says whether a SCHEDULED attempt's next dispatch is available now, or still delayed
// by a start_delay or retry backoff.
type Dispatchability int

const (
	Dispatchable Dispatchability = iota // pollable now
	StartDelayPending
	BackoffPending
)

// AbstractState is the projection of observable internal state that the model predicts.
type AbstractState struct {
	Status              Status
	AttemptCount        int32
	FirstAttemptStarted bool
	Dispatchability     Dispatchability
	DispatchTimeSet     bool

	// Flags supporting deferred reset/update
	ResetKeepPaused     bool
	ResetHeartbeats     bool
	ResetRestoreOptions bool
}

// Config captures the start-time options that change transition behavior.
type Config struct {
	HasScheduleToClose bool
	HasScheduleToStart bool
	HasHeartbeat       bool
	HasStartDelay      bool
	MaxAttempts        int32 // 0 = unlimited
}

// EventType enumerates the events a driver can realize.
type EventType int

const (
	// RPC events
	PollType EventType = iota
	HeartbeatType
	RespondCompletedType
	RespondCompletedByIDType
	RespondFailedType
	RespondFailedByIDType
	RespondCanceledType
	RespondCanceledByIDType
	RequestCancelType
	TerminateType
	PauseType
	UnpauseType
	ResetType
	UpdateOptionsType

	// Timer events

	// Timeout task timers elapsing (a timer may or may not have actually fired)
	ScheduleToStartElapsesType
	ScheduleToCloseElapsesType
	StartToCloseElapsesType
	HeartbeatElapsesType

	// Dispatch-delay timers elapsing. On elapse the delayed dispatch becomes available.
	StartDelayElapsesType
	BackoffElapsesType
)

// Event carries the variant flags that affect the outcome.
type Event struct {
	Type EventType

	KeepPaused          bool     // Reset: a paused activity stays paused across the reset.
	ResetHeartbeat      bool     // Reset: discard the persisted heartbeat checkpoint instead of carrying it into the new attempt.
	HasHeartbeatDetails bool     // Failure response: attach last_heartbeat_details, to be stored as the activity's heartbeat progress.
	Failure             *Failure // RespondFailed: the failure to send, or nil to respond with no failure at all (as a worker may). A nil failure is retryable.
	RestoreOriginal     bool     // Reset / UpdateOptions
	SameRequestID       bool     // Pause / Terminate / RequestCancel: repeat of the previous op's request id
	SetsStartDelay      bool     // UpdateOptions
}

// Failure specifies the failure a RespondFailed event sends.
type Failure struct {
	Type         FailureType
	Retryable    bool // controls the non-retryable flag for application and server failures.
	LargeMessage bool // sends a failure message exceeding activityFailureSizeLimit
}

// FailureType identifies the kind of failure a RespondFailed event reports.
type FailureType int

const (
	ApplicationFailureType FailureType = iota
	ServerFailureType
	StartToCloseTimeoutFailureType
	HeartbeatTimeoutFailureType
	ScheduleToStartTimeoutFailureType
	ScheduleToCloseTimeoutFailureType
	UnknownFailureType
)

// String names the kind of failure.
func (t FailureType) String() string {
	switch t {
	case ApplicationFailureType:
		return "application"
	case ServerFailureType:
		return "server"
	case StartToCloseTimeoutFailureType:
		return "startToCloseTimeout"
	case HeartbeatTimeoutFailureType:
		return "heartbeatTimeout"
	case ScheduleToStartTimeoutFailureType:
		return "scheduleToStartTimeout"
	case ScheduleToCloseTimeoutFailureType:
		return "scheduleToCloseTimeout"
	case UnknownFailureType:
		return "unknown"
	default:
		return fmt.Sprintf("FailureType(%d)", int(t))
	}
}

// Retries reports whether the failure asks to be retried. An omitted failure does.
func (f *Failure) Retries() bool {
	return f == nil || f.Retryable
}

// Canonical Event values for the variants frequently used in traces
var (
	Poll               = Event{Type: PollType}
	Heartbeat          = Event{Type: HeartbeatType}
	Complete           = Event{Type: RespondCompletedType}
	CompleteByID       = Event{Type: RespondCompletedByIDType}
	FailRetryably      = Event{Type: RespondFailedType, Failure: &Failure{Retryable: true}}
	FailNonRetryably   = Event{Type: RespondFailedType, Failure: &Failure{Retryable: false}}
	FailWithoutFailure = Event{Type: RespondFailedType}
	// FailByIDRetryablyWithServerFailure reports a retryable ServerFailure through the by-ID API. Unlike
	// the by-token API, the by-ID API accepts a non-application failure, so only this variant can carry a
	// ServerFailure to the handler.
	FailByIDRetryablyWithServerFailure              = Event{Type: RespondFailedByIDType, Failure: &Failure{Type: ServerFailureType, Retryable: true}}
	FailByIDRetryablyWithStartToCloseTimeoutFailure = Event{Type: RespondFailedByIDType, Failure: &Failure{Type: StartToCloseTimeoutFailureType}}
	FailByIDRetryablyWithHeartbeatTimeoutFailure    = Event{Type: RespondFailedByIDType, Failure: &Failure{Type: HeartbeatTimeoutFailureType}}
	FailByIDWithScheduleToStartTimeoutFailure       = Event{Type: RespondFailedByIDType, Failure: &Failure{Type: ScheduleToStartTimeoutFailureType}}
	FailByIDWithScheduleToCloseTimeoutFailure       = Event{Type: RespondFailedByIDType, Failure: &Failure{Type: ScheduleToCloseTimeoutFailureType}}
	FailByIDRetryablyWithUnknownFailure             = Event{Type: RespondFailedByIDType, Failure: &Failure{Type: UnknownFailureType}}
	RespondCanceled                                 = Event{Type: RespondCanceledType}
	RespondCanceledByID                             = Event{Type: RespondCanceledByIDType}
	RequestCancel                                   = Event{Type: RequestCancelType}
	Terminate                                       = Event{Type: TerminateType}
	Pause                                           = Event{Type: PauseType}
	ResetKeepPaused                                 = Event{Type: ResetType, KeepPaused: true}
	Unpause                                         = Event{Type: UnpauseType}
	Reset                                           = Event{Type: ResetType}
	ResetClearingHeartbeat                          = Event{Type: ResetType, ResetHeartbeat: true}
	UpdateOptions                                   = Event{Type: UpdateOptionsType}
	StartToCloseElapses                             = Event{Type: StartToCloseElapsesType}
	ScheduleToCloseElapses                          = Event{Type: ScheduleToCloseElapsesType}
	ScheduleToStartElapses                          = Event{Type: ScheduleToStartElapsesType}
	HeartbeatElapses                                = Event{Type: HeartbeatElapsesType}
	StartDelayElapses                               = Event{Type: StartDelayElapsesType}
	BackoffElapses                                  = Event{Type: BackoffElapsesType}
)

// ErrorKind is the user facing error the model expects for a call.
type ErrorKind int

const (
	NoError ErrorKind = iota
	FailedPrecondition
	NotFound
	InvalidArgument
)

// Observed is the internal state the driver reads
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

// Terminal reports whether the activity has reached a terminal state.
func (s Status) Terminal() bool {
	switch s {
	case Completed, Failed, Canceled, Terminated, TimedOut:
		return true
	default:
		return false
	}
}

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

// SameObserved reports whether two states agree on every ReadComponent-readable field that is live in
// the expected status. It excludes the latent Dispatchability, and additionally drops any field that
// is not observable-meaningful in the status (see mask), so the oracle never asserts mechanism state
// where nobody observes it.
func (s AbstractState) SameObserved(o AbstractState) bool {
	return s.mask() == o.mask()
}

// mask zeroes fields that are not observable-meaningful in status s.Status, so the oracle only
// compares each field where it is live. Dispatchability is always latent (verified by polling).
func (s AbstractState) mask() AbstractState {
	s.Dispatchability = Dispatchable
	if s.Status != ResetRequested {
		// The pending-reset intent is only meaningful while a reset is deferred.
		s.ResetKeepPaused, s.ResetHeartbeats, s.ResetRestoreOptions = false, false, false
	}
	return s
}

// Abstract maps an observed internal snapshot onto the model's AbstractState.
func Abstract(o Observed) AbstractState {
	return AbstractState{
		Status:              mapStatus(o.Status),
		AttemptCount:        o.Count,
		ResetKeepPaused:     o.ResetKeepPaused,
		ResetHeartbeats:     o.ResetHeartbeats,
		ResetRestoreOptions: o.ResetRestoreOptions,
		FirstAttemptStarted: o.FirstAttemptStarted,
		DispatchTimeSet:     o.DispatchTimeSet,
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

// String is a stable label for an event type, for logs and failure reports.
func (t EventType) String() string {
	switch t {
	case PollType:
		return "Poll"
	case HeartbeatType:
		return "Heartbeat"
	case RespondCompletedType:
		return "RespondCompleted"
	case RespondCompletedByIDType:
		return "RespondCompletedByID"
	case RespondFailedType:
		return "RespondFailed"
	case RespondFailedByIDType:
		return "RespondFailedByID"
	case RespondCanceledType:
		return "RespondCanceled"
	case RespondCanceledByIDType:
		return "RespondCanceledByID"
	case RequestCancelType:
		return "RequestCancel"
	case TerminateType:
		return "Terminate"
	case PauseType:
		return "Pause"
	case UnpauseType:
		return "Unpause"
	case ResetType:
		return "Reset"
	case UpdateOptionsType:
		return "UpdateOptions"
	case ScheduleToStartElapsesType:
		return "ScheduleToStartElapses"
	case ScheduleToCloseElapsesType:
		return "ScheduleToCloseElapses"
	case StartToCloseElapsesType:
		return "StartToCloseElapses"
	case HeartbeatElapsesType:
		return "HeartbeatElapses"
	case StartDelayElapsesType:
		return "StartDelayElapses"
	case BackoffElapsesType:
		return "BackoffElapses"
	default:
		return fmt.Sprintf("EventType(%d)", int(t))
	}
}

// String names an event and appends the flags that affect its outcome.
func (e Event) String() string {
	var flags []string
	add := func(cond bool, name string) {
		if cond {
			flags = append(flags, name)
		}
	}
	switch e.Type {
	case RespondFailedType, RespondFailedByIDType:
		if e.Failure == nil {
			flags = append(flags, "omitted")
		} else {
			flags = append(flags, e.Failure.Type.String())
			if e.Failure.Type == ApplicationFailureType || e.Failure.Type == ServerFailureType {
				flags = append(flags, fmt.Sprintf("retryable=%v", e.Failure.Retryable))
			}
		}
		add(e.HasHeartbeatDetails, "heartbeatDetails")
	case ResetType:
		add(e.KeepPaused, "keepPaused")
		add(e.ResetHeartbeat, "resetHeartbeat")
		add(e.RestoreOriginal, "restoreOriginal")
	case PauseType, TerminateType, RequestCancelType:
		add(e.SameRequestID, "sameRequestID")
	case UpdateOptionsType:
		add(e.SetsStartDelay, "setsStartDelay")
		add(e.RestoreOriginal, "restoreOriginal")
	}
	if len(flags) == 0 {
		return e.Type.String()
	}
	return fmt.Sprintf("%s[%s]", e.Type, strings.Join(flags, ","))
}
