// Package lifecycle is the shared, spec-derived view-model behind the standalone-activity
// projections — the diagram, animation, and prose renderers under ../cmd. Every value it exposes is
// computed by executing saaspec.Model / Initial / ExpectedDescribe; it encodes no product behavior of
// its own (it only names the outcomes the spec produces) and never touches the implementation. Each
// renderer turns this view-model into its own medium.
package lifecycle

import (
	"strings"

	"go.temporal.io/server/chasm/lib/activity/saaspec"
)

// Category classifies an operation's effect in a phase, independent of medium. It is derived from the
// outcome the spec returns, so it tracks the spec rather than restating it.
type Category int

const (
	NotPermitted Category = iota // the spec rejects the operation in this phase
	Immediate                    // takes effect immediately
	Deferred                     // accepted, but resolves at the next attempt boundary
)

// Cell is the classification of one operation in one phase: a short label and its category.
type Cell struct {
	Label    string
	Category Category
}

func notPermitted() Cell { return Cell{"—", NotPermitted} }

// Op is an operator action tabulated per phase by the diagram and prose projections. Classify
// evaluates the spec at a phase and names the result; it never decides behavior itself.
type Op struct {
	Name     string
	Classify func(cfg saaspec.Config, s saaspec.AbstractState) Cell
}

// Ops are the operator actions shown per phase, in display order.
var Ops = []Op{
	{"pause", classifyPause},
	{"unpause (if paused)", classifyUnpause},
	{"update start_delay", classifyUpdateStartDelay},
	{"cancel", classifyCancel},
	{"reset", classifyReset},
	{"terminate", classifyTerminate},
}

func classifyPause(cfg saaspec.Config, s saaspec.AbstractState) Cell {
	out := saaspec.Model(cfg, s, saaspec.Event{Kind: saaspec.Pause})
	if out.Reject != saaspec.NoError {
		return notPermitted()
	}
	switch out.Next.Status {
	case saaspec.Paused:
		return Cell{"pausable", Immediate}
	case saaspec.PauseRequested:
		return Cell{"pause requestable", Deferred}
	default:
		return notPermitted()
	}
}

// classifyUnpause reports what unpause does if the activity were paused in this phase, so it is
// evaluated on the phase's paused variant (the state pause lands in).
func classifyUnpause(cfg saaspec.Config, s saaspec.AbstractState) Cell {
	p := saaspec.Model(cfg, s, saaspec.Event{Kind: saaspec.Pause})
	if p.Reject != saaspec.NoError { // not pausable here, so "if paused" is vacuous
		return notPermitted()
	}
	n := saaspec.Model(cfg, p.Next, saaspec.Event{Kind: saaspec.Unpause}).Next
	switch {
	case n.Status == saaspec.Started:
		return Cell{"removes pause request", Immediate}
	case n.Status == saaspec.Scheduled && n.Dispatchability == saaspec.Dispatchable:
		return Cell{"dispatches immediately", Immediate}
	case n.Dispatchability == saaspec.StartDelayPending:
		return Cell{"dispatches at end of start delay", Deferred}
	case n.Dispatchability == saaspec.BackoffPending:
		return Cell{"dispatches at end of retry backoff", Deferred}
	default:
		return notPermitted()
	}
}

func classifyUpdateStartDelay(cfg saaspec.Config, s saaspec.AbstractState) Cell {
	out := saaspec.Model(cfg, s, saaspec.Event{Kind: saaspec.UpdateOptions, SetsStartDelay: true})
	if out.Reject != saaspec.NoError {
		return Cell{"not updateable", NotPermitted}
	}
	return Cell{"updateable", Immediate}
}

func classifyCancel(cfg saaspec.Config, s saaspec.AbstractState) Cell {
	out := saaspec.Model(cfg, s, saaspec.Event{Kind: saaspec.RequestCancel})
	if out.Reject != saaspec.NoError {
		return notPermitted()
	}
	switch out.Next.Status {
	case saaspec.Canceled:
		return Cell{"cancellable", Immediate}
	case saaspec.CancelRequested:
		return Cell{"cancel requestable", Deferred}
	default:
		return notPermitted()
	}
}

func classifyReset(cfg saaspec.Config, s saaspec.AbstractState) Cell {
	out := saaspec.Model(cfg, s, saaspec.Event{Kind: saaspec.Reset})
	if out.Reject != saaspec.NoError {
		return notPermitted()
	}
	if out.Next.Status == saaspec.ResetRequested {
		return Cell{"reset requestable", Deferred}
	}
	return Cell{"resettable", Immediate}
}

func classifyTerminate(cfg saaspec.Config, s saaspec.AbstractState) Cell {
	out := saaspec.Model(cfg, s, saaspec.Event{Kind: saaspec.Terminate})
	if out.Reject == saaspec.NoError && out.Next.Status == saaspec.Terminated {
		return Cell{"→ Terminated", Immediate}
	}
	return notPermitted()
}

// Phase is one column of the lifecycle timeline: an activity state plus the human labels for the
// milestone it sits at and the timer running there.
type Phase struct {
	State     saaspec.AbstractState
	Milestone string // SCHEDULED / dispatched / STARTED / COMPLETED / …
	Timer     string // start delay / schedule-to-start / start-to-close + heartbeat / retry backoff / ""
	Attempt   int32
	Terminal  bool
}

// canonicalAdvance drives the happy-path-with-one-retry the timeline projections visualize: start
// delay elapses → dispatch → run → retryable failure → backoff → dispatch → run, then completion.
var canonicalAdvance = []saaspec.Event{
	{Kind: saaspec.StartDelayElapses},
	{Kind: saaspec.Poll},
	{Kind: saaspec.RespondFailed, Retryable: true},
	{Kind: saaspec.BackoffElapses},
	{Kind: saaspec.Poll},
}

// CanonicalTrace returns the phases of the canonical trace for cfg (see canonicalAdvance), ending in
// the terminal state a final RespondCompleted reaches. It panics if the spec no longer completes that
// way, so a spec change that breaks the story fails the generator loudly.
func CanonicalTrace(cfg saaspec.Config) []Phase {
	states := []saaspec.AbstractState{saaspec.Initial(cfg)}
	for _, e := range canonicalAdvance {
		states = append(states, saaspec.Model(cfg, states[len(states)-1], e).Next)
	}
	end := saaspec.Model(cfg, states[len(states)-1], saaspec.Event{Kind: saaspec.RespondCompleted}).Next
	if end.Status != saaspec.Completed {
		panic("lifecycle: canonical trace does not complete (ended " + end.Status.String() + "); spec changed?")
	}
	states = append(states, end)

	phases := make([]Phase, len(states))
	for i, s := range states {
		phases[i] = Phase{State: s, Milestone: Milestone(s), Timer: Timer(s), Attempt: s.Count, Terminal: s.Status.Terminal()}
	}
	return phases
}

// Milestone is the human name for the point in the lifecycle a state sits at.
func Milestone(s saaspec.AbstractState) string {
	switch {
	case s.Status == saaspec.Scheduled && s.Dispatchability == saaspec.Dispatchable:
		return "dispatched"
	case s.Status == saaspec.Scheduled:
		return "SCHEDULED"
	case s.Status == saaspec.Started:
		return "STARTED"
	default:
		return strings.ToUpper(s.Status.String())
	}
}

// Timer is the timeout/delay window running in a state ("" if none).
func Timer(s saaspec.AbstractState) string {
	switch {
	case s.Status == saaspec.Scheduled && s.Dispatchability == saaspec.StartDelayPending:
		return "start delay"
	case s.Status == saaspec.Scheduled && s.Dispatchability == saaspec.BackoffPending:
		return "retry backoff"
	case s.Status == saaspec.Scheduled && s.Dispatchability == saaspec.Dispatchable:
		return "schedule-to-start"
	case s.Status == saaspec.Started:
		return "start-to-close + heartbeat"
	default:
		return ""
	}
}

// LabeledEvent is an event with a display label, for graph-style projections that name edges.
type LabeledEvent struct {
	Label string
	Event saaspec.Event
}

// Alphabet is the event set driven at every reachable state to explore the full graph. Flag variants
// that change the outcome are listed separately so both branches appear.
var Alphabet = []LabeledEvent{
	{"Poll", ev(saaspec.Poll)},
	{"Heartbeat", ev(saaspec.Heartbeat)},
	{"RespondCompleted", ev(saaspec.RespondCompleted)},
	{"RespondFailed(retryable)", saaspec.Event{Kind: saaspec.RespondFailed, Retryable: true}},
	{"RespondFailed(non-retryable)", saaspec.Event{Kind: saaspec.RespondFailed}},
	{"RespondCanceled", ev(saaspec.RespondCanceled)},
	{"RequestCancel", ev(saaspec.RequestCancel)},
	{"Terminate", ev(saaspec.Terminate)},
	{"Pause", ev(saaspec.Pause)},
	{"Unpause", ev(saaspec.Unpause)},
	{"Reset", ev(saaspec.Reset)},
	{"Reset(keepPaused)", saaspec.Event{Kind: saaspec.Reset, KeepPaused: true}},
	{"UpdateOptions", ev(saaspec.UpdateOptions)},
	{"ScheduleToStartElapses", ev(saaspec.ScheduleToStartElapses)},
	{"ScheduleToCloseElapses", ev(saaspec.ScheduleToCloseElapses)},
	{"StartToCloseElapses", ev(saaspec.StartToCloseElapses)},
	{"HeartbeatElapses", ev(saaspec.HeartbeatElapses)},
	{"StartDelayElapses", ev(saaspec.StartDelayElapses)},
	{"BackoffElapses", ev(saaspec.BackoffElapses)},
}

func ev(k saaspec.EventKind) saaspec.Event { return saaspec.Event{Kind: k} }

// Reachable returns every AbstractState reachable from Initial(cfg) under Alphabet, in BFS order.
func Reachable(cfg saaspec.Config) []saaspec.AbstractState {
	start := saaspec.Initial(cfg)
	seen := map[saaspec.AbstractState]bool{start: true}
	queue := []saaspec.AbstractState{start}
	var order []saaspec.AbstractState
	for len(queue) > 0 {
		s := queue[0]
		queue = queue[1:]
		order = append(order, s)
		for _, le := range Alphabet {
			n := saaspec.Model(cfg, s, le.Event).Next
			if !seen[n] {
				seen[n] = true
				queue = append(queue, n)
			}
		}
	}
	return order
}

// ErrorName is the display name for a rejection kind ("" for NoError).
func ErrorName(k saaspec.ErrorKind) string {
	switch k {
	case saaspec.NoError:
		return ""
	case saaspec.FailedPrecondition:
		return "FailedPrecondition"
	case saaspec.NotFound:
		return "NotFound"
	case saaspec.InvalidArgument:
		return "InvalidArgument"
	default:
		return "Error(?)"
	}
}
