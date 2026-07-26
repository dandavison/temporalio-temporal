package model

import (
	"fmt"
	"strings"
)

// Fingerprint identifies a state for graph exploration, bucketing the attempt count so retry loops
// converge to a finite reachable set.
func Fingerprint(s AbstractState) string {
	return fmt.Sprintf("%v|%d|%v|%v|%v|%v|%v|%v",
		s.Status, min(s.AttemptCount, 3), s.ResetKeepPaused, s.ResetHeartbeats,
		s.ResetRestoreOptions, s.FirstAttemptStarted, s.DispatchTimeSet, s.Dispatchability)
}

// CellKey identifies a (state, event type) cell at fingerprint granularity.
func CellKey(s AbstractState, k EventType) string {
	return Fingerprint(s) + " / " + EventTypeName(k)
}

// NeedsToken reports whether an event is a worker RPC that requires a dispatched task token.
func NeedsToken(k EventType) bool {
	switch k {
	case HeartbeatEvent, RespondCompletedEvent, RespondFailedEvent, RespondCanceledEvent:
		return true
	default:
		return false
	}
}

// CarriesReqID reports whether an operator command's server-side idempotency is keyed on its request id.
func CarriesReqID(k EventType) bool {
	switch k {
	case RequestCancelEvent, TerminateEvent, PauseEvent:
		return true
	default:
		return false
	}
}

// Reachable computes, purely from Transition (no driver), every (state, event) cell reachable from
// Initial(cfg) by following non-reject edges to fixpoint (states deduped by Fingerprint).
func Reachable(cfg Config, events []Event) map[string]bool {
	cells := map[string]bool{}
	start := Initial(cfg)
	visited := map[string]bool{Fingerprint(start): true}
	frontier := []AbstractState{start}
	for len(frontier) > 0 {
		var next []AbstractState
		for _, s := range frontier {
			for _, e := range events {
				out := Transition(cfg, s, e)
				cells[CellKey(s, e.Type)] = true
				if out.Reject != NoError {
					continue
				}
				fp := Fingerprint(out.Next)
				if !visited[fp] {
					visited[fp] = true
					next = append(next, out.Next)
				}
			}
		}
		frontier = next
	}
	return cells
}

// EventTypeName is a stable label for an event type, for logs and failure reports.
func EventTypeName(e EventType) string {
	switch e {
	case PollEvent:
		return "Poll"
	case HeartbeatEvent:
		return "Heartbeat"
	case RespondCompletedEvent:
		return "RespondCompleted"
	case RespondFailedEvent:
		return "RespondFailed"
	case RespondCanceledEvent:
		return "RespondCanceled"
	case RequestCancelEvent:
		return "RequestCancel"
	case TerminateEvent:
		return "Terminate"
	case PauseEvent:
		return "Pause"
	case UnpauseEvent:
		return "Unpause"
	case ResetEvent:
		return "Reset"
	case UpdateOptionsEvent:
		return "UpdateOptions"
	case ScheduleToStartElapsesEvent:
		return "ScheduleToStartElapses"
	case ScheduleToCloseElapsesEvent:
		return "ScheduleToCloseElapses"
	case StartToCloseElapsesEvent:
		return "StartToCloseElapses"
	case HeartbeatElapsesEvent:
		return "HeartbeatElapses"
	case StartDelayElapsesEvent:
		return "StartDelayElapses"
	case BackoffElapsesEvent:
		return "BackoffElapses"
	default:
		return fmt.Sprintf("EventType(%d)", e)
	}
}

// EventLabel names an event and appends the flags that affect its outcome.
func EventLabel(e Event) string {
	var flags []string
	add := func(cond bool, name string) {
		if cond {
			flags = append(flags, name)
		}
	}
	switch e.Type {
	case RespondFailedEvent:
		flags = append(flags, fmt.Sprintf("retryable=%v", e.Retryable))
	case ResetEvent:
		add(e.KeepPaused, "keepPaused")
		add(e.RestoreOriginal, "restoreOriginal")
	case UnpauseEvent:
		add(e.ResetAttempts, "resetAttempts")
		add(e.ResetHeartbeat, "resetHeartbeat")
	case PauseEvent, TerminateEvent, RequestCancelEvent:
		add(e.SameRequestID, "sameRequestID")
	case UpdateOptionsEvent:
		add(e.SetsStartDelay, "setsStartDelay")
		add(e.RestoreOriginal, "restoreOriginal")
	}
	if len(flags) == 0 {
		return EventTypeName(e.Type)
	}
	return fmt.Sprintf("%s[%s]", EventTypeName(e.Type), strings.Join(flags, ","))
}
