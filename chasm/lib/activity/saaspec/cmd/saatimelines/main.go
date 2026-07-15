// Command saatimelines derives a per-trace TIMELINE data module from the SAA behavior spec.
//
// It is a sibling of saaanim: the graph animation projects the spec as a state machine; this one
// projects each trace as a horizontal timeline. It encodes no product behavior of its own. Every
// segment (a phase with its timer band, status, and attempt number), every transition, and every
// operator-probe classification (accepted / deferred / rejected) is obtained by executing
// saaspec.Model / Initial via the shared lifecycle view-model (Milestone, Timer). The only inputs it
// authors are the configs and the event sequences to drive — the analog of the test harness's
// directed traces — plus, per phase, which operator actions to *probe*. What each event and probe
// *does* comes entirely from the spec.
//
// Output is a TypeScript module consumed by the canvas-commons scene (saa-timelines.tsx).
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"go.temporal.io/server/chasm/lib/activity/saaspec"
	"go.temporal.io/server/chasm/lib/activity/saaspec/lifecycle"
)

func main() {
	out := flag.String("o", "", "output .ts path (default stdout)")
	flag.Parse()

	data := build()
	body, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		fatal(err)
	}
	src := header + "export const timelines: Timeline[] = " + string(body) + ";\n"

	w := os.Stdout
	if *out != "" {
		f, err := os.Create(*out)
		if err != nil {
			fatal(err)
		}
		defer f.Close()
		w = f
	}
	if _, err := fmt.Fprint(w, src); err != nil {
		fatal(err)
	}
}

func fatal(err error) { fmt.Fprintln(os.Stderr, "saatimelines:", err); os.Exit(1) }

// --- output shape ---

type configView struct {
	HasScheduleToClose bool  `json:"hasScheduleToClose"`
	HasScheduleToStart bool  `json:"hasScheduleToStart"`
	HasHeartbeat       bool  `json:"hasHeartbeat"`
	HasStartDelay      bool  `json:"hasStartDelay"`
	MaxAttempts        int32 `json:"maxAttempts"`
}

// segment is one phase of a trace: the state it sits in, the timer window running there, and the
// driving event (with its category) that opened it. Rendered as one band on the timeline.
type segment struct {
	Timer     string `json:"timer"`     // "start delay" / "schedule-to-start" / "start-to-close + heartbeat" / "retry backoff" / ""
	Milestone string `json:"milestone"` // dispatched / STARTED / PAUSED / …
	Status    string `json:"status"`
	Attempt   int32  `json:"attempt"`
	Terminal  bool   `json:"terminal"`
	Event     string `json:"event"`    // driving event that opened this segment ("" for the initial segment)
	Category  string `json:"category"` // classification of that opening event (see classify): start/progress/deferred/immediate
}

// probe is a read-only "what if you tried this operator action in this phase" evaluation. It never
// advances the trace; it exists to surface accepted / deferred / rejected at a point in time.
type probe struct {
	Segment  int    `json:"segment"`
	Op       string `json:"op"`
	Category string `json:"category"` // "immediate" | "deferred" | "rejected"
	Result   string `json:"result"`   // resulting status, or the rejection error
}

type timeline struct {
	Name     string     `json:"name"`
	Note     string     `json:"note"`
	Cfg      configView `json:"cfg"`
	Segments []segment  `json:"segments"`
	Probes   []probe    `json:"probes"`
}

// --- driving inputs (config + event sequences + probes); all outcomes come from Model ---

func e(k saaspec.EventKind) saaspec.Event { return saaspec.Event{Kind: k} }

type step struct {
	label string
	ev    saaspec.Event
}

// pr is a probe authored against a segment index in the driven walk.
type pr struct {
	seg int
	op  string
	ev  saaspec.Event
}

var failRetryable = saaspec.Event{Kind: saaspec.RespondFailed, Retryable: true}

var specs = []struct {
	name    string
	note    string
	cfg     saaspec.Config
	driving []step
	probes  []pr
}{
	// --- Progression timelines: show the timer bands and the happy lifecycle. ---
	{
		"Start delay → dispatch → run → complete",
		"first dispatch waits out the start delay; then the attempt runs and completes",
		saaspec.Config{HasStartDelay: true, HasScheduleToClose: true, MaxAttempts: 1},
		[]step{
			{"StartDelayElapses", e(saaspec.StartDelayElapses)},
			{"Poll", e(saaspec.Poll)},
			{"RespondCompleted", e(saaspec.RespondCompleted)},
		},
		[]pr{
			{0, "update start_delay", saaspec.Event{Kind: saaspec.UpdateOptions, SetsStartDelay: true}},
			{0, "pause", e(saaspec.Pause)},
			{2, "pause", e(saaspec.Pause)},
			{3, "pause", e(saaspec.Pause)}, // terminal: rejected
		},
	},
	{
		"Retryable failure → backoff → retry → complete",
		"a retryable failure schedules a new attempt after the retry backoff",
		saaspec.Config{HasScheduleToClose: true, MaxAttempts: 3},
		[]step{
			{"Poll", e(saaspec.Poll)},
			{"RespondFailed(retryable)", failRetryable},
			{"BackoffElapses", e(saaspec.BackoffElapses)},
			{"Poll", e(saaspec.Poll)},
			{"RespondCompleted", e(saaspec.RespondCompleted)},
		},
		nil,
	},

	// --- Attempt-boundary resolution: the centerpiece. Same running attempt, different pending
	// request (or none), then the same class of attempt-ending event; the spec resolves each
	// differently. Read down the sequence to see "pending request vs none". ---
	{
		"No pending request: failure → retry",
		"baseline — nothing pending, so a retryable failure just schedules the next attempt",
		saaspec.Config{MaxAttempts: 3},
		[]step{
			{"Poll", e(saaspec.Poll)},
			{"RespondFailed(retryable)", failRetryable},
		},
		nil,
	},
	{
		"Pending PAUSE: failure → retries, comes back Paused",
		"the pause is pending during the attempt; it resolves at the attempt boundary",
		saaspec.Config{MaxAttempts: 3},
		[]step{
			{"Poll", e(saaspec.Poll)},
			{"Pause", e(saaspec.Pause)},
			{"RespondFailed(retryable)", failRetryable},
		},
		[]pr{
			{2, "unpause", e(saaspec.Unpause)}, // undoes the pending pause request
			{2, "cancel", e(saaspec.RequestCancel)},
		},
	},
	{
		"Pending CANCEL: failure → Failed (retry suppressed)",
		"a pending cancel suppresses the retry: the failure is terminal",
		saaspec.Config{MaxAttempts: 3},
		[]step{
			{"Poll", e(saaspec.Poll)},
			{"RequestCancel", e(saaspec.RequestCancel)},
			{"RespondFailed(retryable)", failRetryable},
		},
		[]pr{
			{2, "pause", e(saaspec.Pause)}, // rejected while cancel pending
			{2, "reset", e(saaspec.Reset)}, // rejected while cancel pending
			{2, "terminate", e(saaspec.Terminate)},
		},
	},
	{
		"Pending CANCEL: timeout → TimedOut (not Canceled)",
		"the attempt times out before the cancel is acknowledged: TimedOut wins",
		saaspec.Config{MaxAttempts: 3},
		[]step{
			{"Poll", e(saaspec.Poll)},
			{"RequestCancel", e(saaspec.RequestCancel)},
			{"StartToCloseElapses", e(saaspec.StartToCloseElapses)},
		},
		nil,
	},
	{
		"Pending CANCEL: RespondCanceled → Canceled",
		"the worker acknowledges the cancel: the attempt ends Canceled",
		saaspec.Config{MaxAttempts: 3},
		[]step{
			{"Poll", e(saaspec.Poll)},
			{"RequestCancel", e(saaspec.RequestCancel)},
			{"RespondCanceled", e(saaspec.RespondCanceled)},
		},
		nil,
	},
	{
		"Pending RESET: failure → reset applies (attempt 1)",
		"the reset is deferred until the attempt ends, then rewinds to attempt 1",
		saaspec.Config{MaxAttempts: 3},
		[]step{
			{"Poll", e(saaspec.Poll)},
			{"Reset", e(saaspec.Reset)},
			{"RespondFailed(retryable)", failRetryable},
		},
		nil,
	},
	{
		"Pending RESET: completion → Completed (completion wins)",
		"a successful completion beats the pending reset: the activity is done",
		saaspec.Config{MaxAttempts: 3},
		[]step{
			{"Poll", e(saaspec.Poll)},
			{"Reset", e(saaspec.Reset)},
			{"RespondCompleted", e(saaspec.RespondCompleted)},
		},
		nil,
	},
}

// --- construction ---

func build() []timeline {
	out := make([]timeline, 0, len(specs))
	for _, ts := range specs {
		segs, states := walk(ts.cfg, ts.driving)
		probes := make([]probe, 0, len(ts.probes))
		for _, p := range ts.probes {
			cat, res := classify(ts.cfg, states[p.seg], p.ev)
			probes = append(probes, probe{Segment: p.seg, Op: p.op, Category: cat, Result: res})
		}
		out = append(out, timeline{
			Name:     ts.name,
			Note:     ts.note,
			Cfg:      cfgView(ts.cfg),
			Segments: segs,
			Probes:   probes,
		})
	}
	return out
}

// walk drives the event sequence, emitting one segment per state (initial + one per event) and
// returning the state at the start of each segment for probing.
func walk(cfg saaspec.Config, driving []step) ([]segment, []saaspec.AbstractState) {
	s := saaspec.Initial(cfg)
	states := []saaspec.AbstractState{s}
	segs := []segment{segFor(s, "", "start")}
	for _, st := range driving {
		out := saaspec.Model(cfg, s, st.ev)
		cat, _ := classify(cfg, s, st.ev)
		s = out.Next
		states = append(states, s)
		segs = append(segs, segFor(s, st.label, cat))
	}
	return segs, states
}

func segFor(s saaspec.AbstractState, event, category string) segment {
	return segment{
		Timer:     lifecycle.Timer(s),
		Milestone: lifecycle.Milestone(s),
		Status:    s.Status.String(),
		Attempt:   s.Count,
		Terminal:  s.Status.Terminal(),
		Event:     event,
		Category:  category,
	}
}

// classify names the outcome the spec produces for event e in state s. It decides nothing itself:
// rejected iff Model rejects; deferred iff the accepted event lands in a *Requested status (resolves
// at the next attempt boundary); otherwise immediate. Mirrors lifecycle.Category.
func classify(cfg saaspec.Config, s saaspec.AbstractState, ev saaspec.Event) (string, string) {
	out := saaspec.Model(cfg, s, ev)
	if out.Reject != saaspec.NoError {
		return "rejected", lifecycle.ErrorName(out.Reject)
	}
	switch out.Next.Status {
	case saaspec.PauseRequested, saaspec.CancelRequested, saaspec.ResetRequested:
		return "deferred", out.Next.Status.String()
	default:
		return "immediate", out.Next.Status.String()
	}
}

func cfgView(c saaspec.Config) configView {
	return configView{c.HasScheduleToClose, c.HasScheduleToStart, c.HasHeartbeat, c.HasStartDelay, c.MaxAttempts}
}

const header = `// AUTO-GENERATED by saatimelines from saaspec.Model — do not edit.
// Every value below is a projection of the executable spec, obtained by running the spec functions.
// Segments, transition categories, and probe classifications all come from saaspec.Model / Initial
// via the lifecycle view-model. The scene (saa-timelines.tsx) contains no product behavior.

export interface Config {
  hasScheduleToClose: boolean;
  hasScheduleToStart: boolean;
  hasHeartbeat: boolean;
  hasStartDelay: boolean;
  maxAttempts: number;
}
export interface Segment {
  timer: string;
  milestone: string;
  status: string;
  attempt: number;
  terminal: boolean;
  event: string;
  category: string;
}
export interface Probe {
  segment: number;
  op: string;
  category: string;
  result: string;
}
export interface Timeline {
  name: string;
  note: string;
  cfg: Config;
  segments: Segment[];
  probes: Probe[];
}

`
