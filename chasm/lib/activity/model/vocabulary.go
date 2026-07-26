// Package model is an implementation-independent vocabulary for driving a CHASM activity: the events
// that can be applied to one, independently of which product surface exposes them.
//
// Drivers realize these events for Standalone Activity or for Workflow Activity, so one trace can be
// driven through both and their observable results compared.
package model

// EventType enumerates the events a driver can realize.
type EventType int

const (
	PollType EventType = iota
	RespondFailedType
	PauseType

	// Timeout deadlines elapsing: the configured deadline window has passed in wall-clock (a timer
	// may or may not have actually fired)
	ScheduleToStartElapsesType
	ScheduleToCloseElapsesType
	StartToCloseElapsesType
	HeartbeatElapsesType

	// Dispatch-delay clocks elapsing. On elapse the delayed dispatch becomes available.
	StartDelayElapsesType
	BackoffElapsesType
)

// Event carries the variant flags that affect the outcome.
type Event struct {
	Type EventType

	Retryable bool // RespondFailed: the failure is retryable. Whether it actually retries also depends on the retry policy.
}

// Canonical Event values for the variants frequently used in traces
var (
	Poll                   = Event{Type: PollType}
	FailRetryably          = Event{Type: RespondFailedType, Retryable: true}
	Pause                  = Event{Type: PauseType}
	StartToCloseElapses    = Event{Type: StartToCloseElapsesType}
	ScheduleToCloseElapses = Event{Type: ScheduleToCloseElapsesType}
	ScheduleToStartElapses = Event{Type: ScheduleToStartElapsesType}
	HeartbeatElapses       = Event{Type: HeartbeatElapsesType}
	StartDelayElapses      = Event{Type: StartDelayElapsesType}
	BackoffElapses         = Event{Type: BackoffElapsesType}
)

// EventTypeName is a stable label for an event type, for failure reports.
func EventTypeName(t EventType) string {
	switch t {
	case PollType:
		return "Poll"
	case RespondFailedType:
		return "RespondFailed"
	case PauseType:
		return "Pause"
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
		return "EventType(?)"
	}
}

// EventLabel names an event and appends the flags that affect its outcome.
func EventLabel(e Event) string {
	if e.Type == RespondFailedType {
		if e.Retryable {
			return "RespondFailed[retryable=true]"
		}
		return "RespondFailed[retryable=false]"
	}
	return EventTypeName(e.Type)
}
