// Command saaanim derives an animation-data module from the SAA behavior spec.
//
// It encodes no product behavior of its own: every status, edge, rejection, and trace step is
// obtained by executing saaspec.Model / Initial / ExpectedDescribe / ExpectedHeartbeatFlags. The
// only inputs it authors are the choice of configs and the event sequences to drive (the analog of
// the test harness's directed traces); what each event *does* comes entirely from the spec.
//
// Output is a TypeScript module consumed by the canvas-commons scene.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"

	"go.temporal.io/server/chasm/lib/activity/saaspec"
)

func main() {
	out := flag.String("o", "", "output .ts path (default stdout)")
	flag.Parse()

	spec := build()
	body, err := json.MarshalIndent(spec, "", "  ")
	if err != nil {
		fatal(err)
	}
	src := header + "export const spec: Spec = " + string(body) + ";\n"

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

func fatal(err error) { fmt.Fprintln(os.Stderr, "saaanim:", err); os.Exit(1) }

// --- output shape ---

type statusInfo struct {
	Name            string          `json:"name"`
	Terminal        bool            `json:"terminal"`
	DescribeStatus  string          `json:"describeStatus"`
	DescribePending string          `json:"describePending"`
	HeartbeatFlags  *heartbeatFlags `json:"heartbeatFlags,omitempty"`
}

type heartbeatFlags struct {
	CancelRequested bool `json:"cancelRequested"`
	ActivityPaused  bool `json:"activityPaused"`
	ActivityReset   bool `json:"activityReset"`
}

type edge struct {
	From                       string `json:"from"`
	To                         string `json:"to"`
	Event                      string `json:"event"`
	InvalidatesAttemptTasks    bool   `json:"invalidatesAttemptTasks"`
	InvalidatesScheduleToClose bool   `json:"invalidatesScheduleToClose"`
}

type rejection struct {
	Status string `json:"status"`
	Event  string `json:"event"`
	Error  string `json:"error"`
}

type traceStep struct {
	Event                      string `json:"event"`
	Error                      string `json:"error"`
	Status                     string `json:"status"`
	Count                      int32  `json:"count"`
	Dispatchability            string `json:"dispatchability"`
	InvalidatesAttemptTasks    bool   `json:"invalidatesAttemptTasks"`
	InvalidatesScheduleToClose bool   `json:"invalidatesScheduleToClose"`
}

type configView struct {
	HasScheduleToClose bool  `json:"hasScheduleToClose"`
	HasScheduleToStart bool  `json:"hasScheduleToStart"`
	HasHeartbeat       bool  `json:"hasHeartbeat"`
	HasStartDelay      bool  `json:"hasStartDelay"`
	MaxAttempts        int32 `json:"maxAttempts"`
}

type trace struct {
	Name  string      `json:"name"`
	Cfg   configView  `json:"cfg"`
	Steps []traceStep `json:"steps"`
}

type specData struct {
	Statuses   []statusInfo `json:"statuses"`
	Edges      []edge       `json:"edges"`
	Rejections []rejection  `json:"rejections"`
	Traces     []trace      `json:"traces"`
}

// --- driving inputs (config + event sequences); outcomes come from Model ---

// diagramCfg enables every feature and a small attempt cap so the BFS reaches every status,
// including both the retry edge (Count < Max) and the terminal-failure edge (Count == Max).
var diagramCfg = saaspec.Config{
	HasScheduleToClose: true, HasScheduleToStart: true, HasHeartbeat: true, HasStartDelay: true,
	MaxAttempts: 3,
}

var allStatuses = []saaspec.Status{
	saaspec.Scheduled, saaspec.Started, saaspec.Completed, saaspec.Failed,
	saaspec.CancelRequested, saaspec.Canceled, saaspec.Terminated, saaspec.TimedOut,
	saaspec.PauseRequested, saaspec.Paused, saaspec.ResetRequested,
}

type labeledEvent struct {
	label string
	ev    saaspec.Event
}

// diagramEvents is the event alphabet driven at every reachable state during BFS. Flag variants that
// change the outcome are listed separately so the graph shows both branches.
var diagramEvents = []labeledEvent{
	{"Poll", e(saaspec.Poll)},
	{"Heartbeat", e(saaspec.Heartbeat)},
	{"RespondCompleted", e(saaspec.RespondCompleted)},
	{"RespondFailed(retryable)", saaspec.Event{Kind: saaspec.RespondFailed, Retryable: true}},
	{"RespondFailed(non-retryable)", saaspec.Event{Kind: saaspec.RespondFailed}},
	{"RespondCanceled", e(saaspec.RespondCanceled)},
	{"RequestCancel", e(saaspec.RequestCancel)},
	{"Terminate", e(saaspec.Terminate)},
	{"Pause", e(saaspec.Pause)},
	{"Unpause", e(saaspec.Unpause)},
	{"Reset", e(saaspec.Reset)},
	{"Reset(keepPaused)", saaspec.Event{Kind: saaspec.Reset, KeepPaused: true}},
	{"UpdateOptions", e(saaspec.UpdateOptions)},
	{"ScheduleToStartElapses", e(saaspec.ScheduleToStartElapses)},
	{"ScheduleToCloseElapses", e(saaspec.ScheduleToCloseElapses)},
	{"StartToCloseElapses", e(saaspec.StartToCloseElapses)},
	{"HeartbeatElapses", e(saaspec.HeartbeatElapses)},
	{"StartDelayElapses", e(saaspec.StartDelayElapses)},
	{"BackoffElapses", e(saaspec.BackoffElapses)},
}

func e(k saaspec.EventKind) saaspec.Event { return saaspec.Event{Kind: k} }

// traceSpecs are directed stories. Each names a config and a sequence of events; the resulting state
// after each event is computed by Model, never asserted here.
var traceSpecs = []struct {
	name   string
	cfg    saaspec.Config
	events []labeledEvent
}{
	{
		"Happy path",
		saaspec.Config{MaxAttempts: 1},
		[]labeledEvent{{"Poll", e(saaspec.Poll)}, {"RespondCompleted", e(saaspec.RespondCompleted)}},
	},
	{
		"Start delay defers the first dispatch",
		saaspec.Config{HasStartDelay: true, MaxAttempts: 1},
		[]labeledEvent{
			{"Poll", e(saaspec.Poll)}, // no task yet: dispatch still pending
			{"StartDelayElapses", e(saaspec.StartDelayElapses)},
			{"Poll", e(saaspec.Poll)},
			{"RespondCompleted", e(saaspec.RespondCompleted)},
		},
	},
	{
		"Retryable failure, then success",
		saaspec.Config{MaxAttempts: 3},
		[]labeledEvent{
			{"Poll", e(saaspec.Poll)},
			{"RespondFailed(retryable)", saaspec.Event{Kind: saaspec.RespondFailed, Retryable: true}},
			{"BackoffElapses", e(saaspec.BackoffElapses)},
			{"Poll", e(saaspec.Poll)},
			{"RespondCompleted", e(saaspec.RespondCompleted)},
		},
	},
	{
		"Retries exhausted",
		saaspec.Config{MaxAttempts: 2},
		[]labeledEvent{
			{"Poll", e(saaspec.Poll)},
			{"RespondFailed(retryable)", saaspec.Event{Kind: saaspec.RespondFailed, Retryable: true}},
			{"BackoffElapses", e(saaspec.BackoffElapses)},
			{"Poll", e(saaspec.Poll)},
			{"RespondFailed(retryable)", saaspec.Event{Kind: saaspec.RespondFailed, Retryable: true}},
		},
	},
	{
		"Pause while scheduled, then unpause",
		saaspec.Config{MaxAttempts: 1},
		[]labeledEvent{
			{"Pause", e(saaspec.Pause)},
			{"Unpause", e(saaspec.Unpause)},
			{"Poll", e(saaspec.Poll)},
			{"RespondCompleted", e(saaspec.RespondCompleted)},
		},
	},
	{
		"Pause takes effect on the retry",
		saaspec.Config{MaxAttempts: 3},
		[]labeledEvent{
			{"Poll", e(saaspec.Poll)},
			{"Pause", e(saaspec.Pause)},
			{"RespondFailed(retryable)", saaspec.Event{Kind: saaspec.RespondFailed, Retryable: true}},
			{"Unpause", e(saaspec.Unpause)},
		},
	},
	{
		"Cancel a running attempt",
		saaspec.Config{MaxAttempts: 1},
		[]labeledEvent{
			{"Poll", e(saaspec.Poll)},
			{"RequestCancel", e(saaspec.RequestCancel)},
			{"RespondCanceled", e(saaspec.RespondCanceled)},
		},
	},
	{
		"Reset applies when the attempt ends",
		saaspec.Config{MaxAttempts: 3},
		[]labeledEvent{
			{"Poll", e(saaspec.Poll)},
			{"Reset", e(saaspec.Reset)},
			{"RespondFailed(retryable)", saaspec.Event{Kind: saaspec.RespondFailed, Retryable: true}},
		},
	},
	{
		"Start-to-close timeout retries",
		saaspec.Config{MaxAttempts: 3},
		[]labeledEvent{
			{"Poll", e(saaspec.Poll)},
			{"StartToCloseElapses", e(saaspec.StartToCloseElapses)},
		},
	},
	{
		"Terminate",
		saaspec.Config{MaxAttempts: 1},
		[]labeledEvent{
			{"Poll", e(saaspec.Poll)},
			{"Terminate", e(saaspec.Terminate)},
		},
	},
}

// --- construction ---

func build() specData {
	return specData{
		Statuses:   buildStatuses(),
		Edges:      buildEdges(),
		Rejections: buildRejections(),
		Traces:     buildTraces(),
	}
}

func buildStatuses() []statusInfo {
	out := make([]statusInfo, 0, len(allStatuses))
	for _, st := range allStatuses {
		s := saaspec.AbstractState{Status: st}
		ds, dp := saaspec.ExpectedDescribe(s)
		info := statusInfo{
			Name:            st.String(),
			Terminal:        st.Terminal(),
			DescribeStatus:  ds.String(),
			DescribePending: dp.String(),
		}
		if hf, ok := heartbeatFlagsFor(st); ok {
			info.HeartbeatFlags = hf
		}
		out = append(out, info)
	}
	return out
}

func heartbeatFlagsFor(st saaspec.Status) (*heartbeatFlags, bool) {
	switch st {
	case saaspec.Started, saaspec.CancelRequested, saaspec.ResetRequested, saaspec.PauseRequested:
		f := saaspec.ExpectedHeartbeatFlags(saaspec.AbstractState{Status: st})
		return &heartbeatFlags{f.CancelRequested, f.ActivityPaused, f.ActivityReset}, true
	default:
		return nil, false
	}
}

func buildEdges() []edge {
	reached := bfs(diagramCfg)
	seen := map[edge]bool{}
	var edges []edge
	for _, s := range reached {
		for _, le := range diagramEvents {
			out := saaspec.Model(diagramCfg, s, le.ev)
			if out.Reject != saaspec.NoError || out.Next.Status == s.Status {
				continue // status-preserving (rejections/no-ops) are shown elsewhere
			}
			ed := edge{
				From:                       s.Status.String(),
				To:                         out.Next.Status.String(),
				Event:                      le.label,
				InvalidatesAttemptTasks:    out.AttemptTasksInvalidated,
				InvalidatesScheduleToClose: out.ScheduleToCloseTaskInvalidated,
			}
			if !seen[ed] {
				seen[ed] = true
				edges = append(edges, ed)
			}
		}
	}
	sort.Slice(edges, func(i, j int) bool {
		a, b := edges[i], edges[j]
		if a.From != b.From {
			return a.From < b.From
		}
		if a.To != b.To {
			return a.To < b.To
		}
		return a.Event < b.Event
	})
	return edges
}

func buildRejections() []rejection {
	reached := bfs(diagramCfg)
	seen := map[rejection]bool{}
	var rej []rejection
	for _, s := range reached {
		for _, le := range diagramEvents {
			out := saaspec.Model(diagramCfg, s, le.ev)
			if out.Reject == saaspec.NoError {
				continue
			}
			r := rejection{Status: s.Status.String(), Event: le.label, Error: errorName(out.Reject)}
			if !seen[r] {
				seen[r] = true
				rej = append(rej, r)
			}
		}
	}
	sort.Slice(rej, func(i, j int) bool {
		if rej[i].Status != rej[j].Status {
			return rej[i].Status < rej[j].Status
		}
		return rej[i].Event < rej[j].Event
	})
	return rej
}

func buildTraces() []trace {
	out := make([]trace, 0, len(traceSpecs))
	for _, ts := range traceSpecs {
		s := saaspec.Initial(ts.cfg)
		steps := []traceStep{stepFrom("(start)", saaspec.Outcome{Next: s})}
		for _, le := range ts.events {
			o := saaspec.Model(ts.cfg, s, le.ev)
			steps = append(steps, stepFrom(le.label, o))
			s = o.Next
		}
		out = append(out, trace{Name: ts.name, Cfg: cfgView(ts.cfg), Steps: steps})
	}
	return out
}

func stepFrom(label string, o saaspec.Outcome) traceStep {
	return traceStep{
		Event:                      label,
		Error:                      errorName(o.Reject),
		Status:                     o.Next.Status.String(),
		Count:                      o.Next.Count,
		Dispatchability:            o.Next.Dispatchability.String(),
		InvalidatesAttemptTasks:    o.AttemptTasksInvalidated,
		InvalidatesScheduleToClose: o.ScheduleToCloseTaskInvalidated,
	}
}

// bfs returns every non-terminal and terminal AbstractState reachable from Initial(cfg) under the
// diagram event alphabet.
func bfs(cfg saaspec.Config) []saaspec.AbstractState {
	start := saaspec.Initial(cfg)
	seen := map[saaspec.AbstractState]bool{start: true}
	queue := []saaspec.AbstractState{start}
	var order []saaspec.AbstractState
	for len(queue) > 0 {
		s := queue[0]
		queue = queue[1:]
		order = append(order, s)
		for _, le := range diagramEvents {
			n := saaspec.Model(cfg, s, le.ev).Next
			if !seen[n] {
				seen[n] = true
				queue = append(queue, n)
			}
		}
	}
	return order
}

func cfgView(c saaspec.Config) configView {
	return configView{c.HasScheduleToClose, c.HasScheduleToStart, c.HasHeartbeat, c.HasStartDelay, c.MaxAttempts}
}

func errorName(k saaspec.ErrorKind) string {
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

const header = `// AUTO-GENERATED by saaanim from saaspec.Model — do not edit.
// Every value below is a projection of the executable spec, obtained by running the spec functions.

export interface HeartbeatFlags {
  cancelRequested: boolean;
  activityPaused: boolean;
  activityReset: boolean;
}
export interface StatusInfo {
  name: string;
  terminal: boolean;
  describeStatus: string;
  describePending: string;
  heartbeatFlags?: HeartbeatFlags;
}
export interface Edge {
  from: string;
  to: string;
  event: string;
  invalidatesAttemptTasks: boolean;
  invalidatesScheduleToClose: boolean;
}
export interface Rejection {
  status: string;
  event: string;
  error: string;
}
export interface TraceStep {
  event: string;
  error: string;
  status: string;
  count: number;
  dispatchability: string;
  invalidatesAttemptTasks: boolean;
  invalidatesScheduleToClose: boolean;
}
export interface Config {
  hasScheduleToClose: boolean;
  hasScheduleToStart: boolean;
  hasHeartbeat: boolean;
  hasStartDelay: boolean;
  maxAttempts: number;
}
export interface Trace {
  name: string;
  cfg: Config;
  steps: TraceStep[];
}
export interface Spec {
  statuses: StatusInfo[];
  edges: Edge[];
  rejections: Rejection[];
  traces: Trace[];
}

`
