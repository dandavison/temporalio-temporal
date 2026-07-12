package conformance

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	activitypb "go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"

	"go.temporal.io/server/chasm/lib/activity"
	"go.temporal.io/server/chasm/lib/activity/saaspec"
)

// --- the domain we enumerate ---------------------------------------------------------------

var allSpecStatuses = []saaspec.Status{
	saaspec.Unspecified, saaspec.Scheduled, saaspec.Started, saaspec.CancelRequested,
	saaspec.Completed, saaspec.Failed, saaspec.Canceled, saaspec.Terminated, saaspec.TimedOut,
	saaspec.Paused, saaspec.PauseRequested, saaspec.ResetRequested,
}

var allEventKinds = []saaspec.EventKind{
	saaspec.Poll, saaspec.Heartbeat, saaspec.RespondCompleted, saaspec.RespondFailed,
	saaspec.RespondCanceled, saaspec.RequestCancel, saaspec.Terminate, saaspec.Pause,
	saaspec.Unpause, saaspec.Reset, saaspec.UpdateOptions,
}

var cfgs = []saaspec.Config{
	{}, // minimal: no schedule-to-close, unlimited attempts
	{HasScheduleToClose: true, HasScheduleToStart: true, HasHeartbeat: true, MaxAttempts: 3},
}

var countValues = []int32{1, 2}

// eventsFor returns a representative set of events for a kind, covering the flags that affect
// the outcome.
func eventsFor(k saaspec.EventKind) []saaspec.Event {
	bools := []bool{false, true}
	var out []saaspec.Event
	switch k {
	case saaspec.RespondFailed:
		for _, r := range bools {
			out = append(out, saaspec.Event{Kind: k, Retryable: r})
		}
	case saaspec.Reset:
		for _, kp := range bools {
			for _, ro := range bools {
				for _, rh := range bools {
					out = append(out, saaspec.Event{Kind: k, KeepPaused: kp, RestoreOriginal: ro, ResetHeartbeat: rh})
				}
			}
		}
	case saaspec.Unpause:
		for _, ra := range bools {
			for _, rh := range bools {
				out = append(out, saaspec.Event{Kind: k, ResetAttempts: ra, ResetHeartbeat: rh})
			}
		}
	case saaspec.Pause, saaspec.Terminate, saaspec.RequestCancel:
		for _, sr := range bools {
			out = append(out, saaspec.Event{Kind: k, SameRequestID: sr})
		}
	default:
		out = append(out, saaspec.Event{Kind: k})
	}
	return out
}

func srcState(cfg saaspec.Config, st saaspec.Status, keepPaused bool, count int32) saaspec.AbstractState {
	s := saaspec.AbstractState{Status: st, Count: count, Stamp: count, ResetKeepPaused: keepPaused}
	if cfg.HasScheduleToClose {
		s.STCStamp = 1
	}
	switch st {
	case saaspec.Unspecified, saaspec.Scheduled:
	default:
		s.FirstAttemptStarted = true
	}
	return s
}

// evalModel calls Model, turning the TODO(spec) and unreachable panics into classifications
// instead of crashing the test.
type verdict int

const (
	decided     verdict = iota // Model returned an Outcome
	todo                       // Model panicked with TODO(spec): still to be written
	unreachable                // Model panicked with "unreachable": author asserts this can't happen
	unexpected                 // Model panicked with something else
)

func evalModel(cfg saaspec.Config, s saaspec.AbstractState, e saaspec.Event) (out saaspec.Outcome, v verdict, panicMsg string) {
	defer func() {
		if r := recover(); r != nil {
			panicMsg = fmt.Sprint(r)
			switch {
			case strings.Contains(panicMsg, "TODO(spec)"):
				v = todo
			case strings.Contains(panicMsg, "unreachable"):
				v = unreachable
			default:
				v = unexpected
			}
		}
	}()
	out = saaspec.Model(cfg, s, e)
	v = decided
	return
}

// --- checks --------------------------------------------------------------------------------

// TestModelDecisionCoverage is informational: it reports which (status, event kind) cells the
// spec has decided and which still panic with TODO(spec). It fails only on an unexpected panic
// (one that is neither TODO(spec) nor unreachable), which indicates a bug in Model or here.
type cell struct {
	status saaspec.Status
	kind   saaspec.EventKind
}

func TestModelDecisionCoverage(t *testing.T) {
	todoCells := map[cell]bool{}
	decidedCells := map[cell]bool{}
	var counts [4]int

	for _, cfg := range cfgs {
		for _, st := range allSpecStatuses {
			for _, kp := range []bool{false, true} {
				for _, ct := range countValues {
					for _, k := range allEventKinds {
						for _, e := range eventsFor(k) {
							_, v, msg := evalModel(cfg, srcState(cfg, st, kp, ct), e)
							counts[v]++
							c := cell{st, k}
							switch v {
							case decided, unreachable:
								decidedCells[c] = true
							case todo:
								todoCells[c] = true
							case unexpected:
								t.Errorf("unexpected panic: status=%s kind=%s event=%+v: %s",
									st, kindName(k), e, msg)
							}
						}
					}
				}
			}
		}
	}

	t.Logf("cells evaluated: decided=%d todo=%d unreachable=%d unexpected=%d",
		counts[decided], counts[todo], counts[unreachable], counts[unexpected])
	t.Logf("distinct (status,event): decided=%d still-TODO=%d", len(decidedCells), len(todoCells))
	t.Logf("still to specify (status, event):\n%s", formatCells(todoCells))
}

func formatCells(cells map[cell]bool) string {
	var lines []string
	for c := range cells {
		lines = append(lines, fmt.Sprintf("  %-16s %s", c.status, kindName(c.kind)))
	}
	sort.Strings(lines)
	return strings.Join(lines, "\n")
}

// TestModelEdgesReachableInCode asserts that every status change the spec accepts can actually
// be produced by the code: if Model moves the activity from A to a different status B, then B
// must be reachable from A by following one or more declared transitions.
func TestModelEdgesReachableInCode(t *testing.T) {
	reach := codeReachability()

	// Deduplicate reported failures so one missing edge is not printed hundreds of times.
	reported := map[[2]saaspec.Status]bool{}

	for _, cfg := range cfgs {
		for _, st := range allSpecStatuses {
			for _, kp := range []bool{false, true} {
				for _, ct := range countValues {
					for _, k := range allEventKinds {
						for _, e := range eventsFor(k) {
							s := srcState(cfg, st, kp, ct)
							out, v, _ := evalModel(cfg, s, e)
							if v != decided || out.Reject != saaspec.NoError {
								continue
							}
							if out.Next.Status == st {
								continue // no status change, no structural claim
							}
							key := [2]saaspec.Status{st, out.Next.Status}
							if reported[key] {
								continue
							}
							src := specToProto(st)
							dst := specToProto(out.Next.Status)
							if !reach[src][dst] {
								reported[key] = true
								t.Errorf("spec accepts %s --%s--> %s, but the code cannot reach %s from %s via any transition path",
									st, kindName(k), out.Next.Status, out.Next.Status, st)
							}
						}
					}
				}
			}
		}
	}
}

// --- reading the code's transition graph ---------------------------------------------------

// codeTransition names an exported activity.Transition* for reporting.
type codeTransition struct {
	name string
	tr   any
}

var codeTransitions = []codeTransition{
	{"Scheduled", activity.TransitionScheduled},
	{"Rescheduled", activity.TransitionRescheduled},
	{"Started", activity.TransitionStarted},
	{"Completed", activity.TransitionCompleted},
	{"Failed", activity.TransitionFailed},
	{"Terminated", activity.TransitionTerminated},
	{"CancelRequested", activity.TransitionCancelRequested},
	{"Canceled", activity.TransitionCanceled},
	{"TimedOut", activity.TransitionTimedOut},
	{"Paused", activity.TransitionPaused},
	{"PauseRequested", activity.TransitionPauseRequested},
	{"Unpaused", activity.TransitionUnpaused},
	{"UnpausedWhilePauseRequested", activity.TransitionUnpausedWhilePauseRequested},
	{"AttemptFailedWhilePauseRequested", activity.TransitionAttemptFailedWhilePauseRequested},
	{"Reset", activity.TransitionReset},
	{"ResetRequested", activity.TransitionResetRequested},
	{"ResetAttemptFailedToPaused", activity.TransitionResetAttemptFailedToPaused},
	{"ResetAttemptFailedToScheduled", activity.TransitionResetAttemptFailedToScheduled},
}

// codeAdjacency reads Sources and Destination off each transition by reflection (the fields are
// exported; the transitions' event type parameters differ and many are unexported, so reflection
// is the uniform way to read them).
func codeAdjacency() map[activitypb.ActivityExecutionStatus][]activitypb.ActivityExecutionStatus {
	adj := map[activitypb.ActivityExecutionStatus][]activitypb.ActivityExecutionStatus{}
	for _, ct := range codeTransitions {
		v := reflect.ValueOf(ct.tr)
		dst := v.FieldByName("Destination").Interface().(activitypb.ActivityExecutionStatus)
		srcs := v.FieldByName("Sources")
		for i := 0; i < srcs.Len(); i++ {
			src := srcs.Index(i).Interface().(activitypb.ActivityExecutionStatus)
			adj[src] = append(adj[src], dst)
		}
	}
	return adj
}

// codeReachability returns, for each status, the set of statuses reachable via one or more
// declared transitions.
func codeReachability() map[activitypb.ActivityExecutionStatus]map[activitypb.ActivityExecutionStatus]bool {
	adj := codeAdjacency()
	reach := map[activitypb.ActivityExecutionStatus]map[activitypb.ActivityExecutionStatus]bool{}
	for _, start := range allProtoStatuses() {
		seen := map[activitypb.ActivityExecutionStatus]bool{}
		queue := append([]activitypb.ActivityExecutionStatus{}, adj[start]...)
		for len(queue) > 0 {
			cur := queue[0]
			queue = queue[1:]
			if seen[cur] {
				continue
			}
			seen[cur] = true
			queue = append(queue, adj[cur]...)
		}
		reach[start] = seen
	}
	return reach
}

func allProtoStatuses() []activitypb.ActivityExecutionStatus {
	out := make([]activitypb.ActivityExecutionStatus, 0, len(allSpecStatuses))
	for _, s := range allSpecStatuses {
		out = append(out, specToProto(s))
	}
	return out
}

func specToProto(s saaspec.Status) activitypb.ActivityExecutionStatus {
	switch s {
	case saaspec.Unspecified:
		return activitypb.ACTIVITY_EXECUTION_STATUS_UNSPECIFIED
	case saaspec.Scheduled:
		return activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED
	case saaspec.Started:
		return activitypb.ACTIVITY_EXECUTION_STATUS_STARTED
	case saaspec.CancelRequested:
		return activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED
	case saaspec.Completed:
		return activitypb.ACTIVITY_EXECUTION_STATUS_COMPLETED
	case saaspec.Failed:
		return activitypb.ACTIVITY_EXECUTION_STATUS_FAILED
	case saaspec.Canceled:
		return activitypb.ACTIVITY_EXECUTION_STATUS_CANCELED
	case saaspec.Terminated:
		return activitypb.ACTIVITY_EXECUTION_STATUS_TERMINATED
	case saaspec.TimedOut:
		return activitypb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT
	case saaspec.Paused:
		return activitypb.ACTIVITY_EXECUTION_STATUS_PAUSED
	case saaspec.PauseRequested:
		return activitypb.ACTIVITY_EXECUTION_STATUS_PAUSE_REQUESTED
	case saaspec.ResetRequested:
		return activitypb.ACTIVITY_EXECUTION_STATUS_RESET_REQUESTED
	default:
		panic(fmt.Sprintf("specToProto: unknown status %v", s))
	}
}

func kindName(k saaspec.EventKind) string {
	switch k {
	case saaspec.Poll:
		return "Poll"
	case saaspec.Heartbeat:
		return "Heartbeat"
	case saaspec.RespondCompleted:
		return "RespondCompleted"
	case saaspec.RespondFailed:
		return "RespondFailed"
	case saaspec.RespondCanceled:
		return "RespondCanceled"
	case saaspec.RequestCancel:
		return "RequestCancel"
	case saaspec.Terminate:
		return "Terminate"
	case saaspec.Pause:
		return "Pause"
	case saaspec.Unpause:
		return "Unpause"
	case saaspec.Reset:
		return "Reset"
	case saaspec.UpdateOptions:
		return "UpdateOptions"
	default:
		return fmt.Sprintf("EventKind(%d)", k)
	}
}
