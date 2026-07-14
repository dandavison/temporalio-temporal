package tests

// Presentation and plumbing for the spec harness: error classification and failure-report
// formatting. The verification logic lives in activity_standalone_spec_rpc_graph_traversal_test.go.

import (
	"errors"
	"fmt"
	"strings"
	"time"

	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm/lib/activity/saaspec"
	"google.golang.org/protobuf/types/known/durationpb"
)

// --- error / outcome classification --------------------------------------------------------

func saaRejectKind(err error) saaspec.ErrorKind {
	if err == nil {
		return saaspec.NoError
	}
	// The FrontendClient returns Temporal serviceerror types, so classify by type rather than by
	// gRPC status code.
	var nf *serviceerror.NotFound
	var fp *serviceerror.FailedPrecondition
	var ia *serviceerror.InvalidArgument
	switch {
	case errors.As(err, &nf):
		return saaspec.NotFound
	case errors.As(err, &fp):
		return saaspec.FailedPrecondition
	case errors.As(err, &ia):
		return saaspec.InvalidArgument
	default:
		return saaspec.ErrorKind(-1) // unrecognized: will not match any predicted kind
	}
}

func saaFailure(retryable bool, nextRetryDelay time.Duration) *failurepb.Failure {
	info := &failurepb.ApplicationFailureInfo{Type: "traverse", NonRetryable: !retryable}
	if nextRetryDelay > 0 {
		info.NextRetryDelay = durationpb.New(nextRetryDelay)
	}
	return &failurepb.Failure{
		Message:     "traverse",
		FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: info},
	}
}

// --- failure reporting ---------------------------------------------------------------------
//
// A failure means the real server ("observed") disagreed with saaspec.Model ("expected") after one
// event. Each report opens with a one-line summary of what diverged, then the path to the edge, then
// a field-aligned diff.

// edge names the event and the status it was driven from, e.g. "RespondFailed[retryable=true] from
// Started".
func (a *saaActor) edge(e saaspec.Event, src saaspec.Status) string {
	return fmt.Sprintf("%s from %s", saaEventLabel(e), src)
}

func (a *saaActor) pathLine() string {
	return "  path: " + saaPathString(a.path)
}

// stateFailure reports that the persisted state after an event disagreed with the model.
func (a *saaActor) stateFailure(e saaspec.Event, src saaspec.Status, observed, expected saaspec.AbstractState) string {
	var summary string
	if observed.Status != expected.Status {
		summary = fmt.Sprintf("model expected %s, server saw %s", expected.Status, observed.Status)
	} else {
		summary = fmt.Sprintf("status %s agrees but persisted state differs", observed.Status)
	}
	return fmt.Sprintf("%s: %s\n%s\n%s", a.edge(e, src), summary, a.pathLine(), saaStateDiff(observed, expected))
}

// rejectFailure reports that the RPC's accept/reject outcome disagreed with the model.
func (a *saaActor) rejectFailure(e saaspec.Event, src saaspec.Status, got, want saaspec.ErrorKind, err error) string {
	msg := fmt.Sprintf("%s: server %s, model expected %s\n%s",
		a.edge(e, src), saaOutcomeDesc(got), saaOutcomeDesc(want), a.pathLine())
	if err != nil {
		msg += fmt.Sprintf("\n  server error: %v", err)
	}
	return msg
}

// flagsFailure reports that the worker-facing heartbeat response flags disagreed with the model.
func (a *saaActor) flagsFailure(e saaspec.Event, src saaspec.Status, observed, expected saaspec.HeartbeatFlags) string {
	rows, agree := saaFlagRows(observed, expected)
	return fmt.Sprintf("%s: heartbeat response flags disagree\n%s\n%s",
		a.edge(e, src), a.pathLine(), saaDiffBlock(rows, agree))
}

// saaPathString renders the event sequence that reached an edge, e.g.
// "Schedule → Poll → RespondFailed[retryable=false]". The origin is labeled Schedule (the status the
// StartActivityExecution RPC lands in), to avoid confusion with Started.
func saaPathString(path []saaspec.Event) string {
	parts := make([]string, 0, len(path)+1)
	parts = append(parts, "Schedule")
	for _, e := range path {
		parts = append(parts, saaEventLabel(e))
	}
	return strings.Join(parts, " → ")
}

// saaEventLabel names an event and appends the flags that affect its outcome.
func saaEventLabel(e saaspec.Event) string {
	var flags []string
	add := func(cond bool, name string) {
		if cond {
			flags = append(flags, name)
		}
	}
	switch e.Kind {
	case saaspec.RespondFailed:
		flags = append(flags, fmt.Sprintf("retryable=%v", e.Retryable))
	case saaspec.Reset:
		add(e.KeepPaused, "keepPaused")
		add(e.RestoreOriginal, "restoreOriginal")
		add(e.ResetHeartbeat, "resetHeartbeat")
	case saaspec.Unpause:
		add(e.ResetAttempts, "resetAttempts")
		add(e.ResetHeartbeat, "resetHeartbeat")
	case saaspec.Pause, saaspec.Terminate, saaspec.RequestCancel:
		add(e.SameRequestID, "sameRequestID")
	case saaspec.UpdateOptions:
		add(e.SetsStartDelay, "setsStartDelay")
	}
	if len(flags) == 0 {
		return saaKindName(e.Kind)
	}
	return fmt.Sprintf("%s[%s]", saaKindName(e.Kind), strings.Join(flags, ","))
}

// saaStateDiff renders the AbstractState fields that differ in aligned columns, with the agreeing
// fields listed as field=value beneath.
func saaStateDiff(observed, expected saaspec.AbstractState) string {
	b2s := func(b bool) string { return fmt.Sprint(b) }
	fields := [][3]string{
		{"Status", observed.Status.String(), expected.Status.String()},
		{"Count", fmt.Sprint(observed.Count), fmt.Sprint(expected.Count)},
		{"ResetKeepPaused", b2s(observed.ResetKeepPaused), b2s(expected.ResetKeepPaused)},
		{"ResetHeartbeats", b2s(observed.ResetHeartbeats), b2s(expected.ResetHeartbeats)},
		{"ResetRestoreOptions", b2s(observed.ResetRestoreOptions), b2s(expected.ResetRestoreOptions)},
		{"FirstAttemptStarted", b2s(observed.FirstAttemptStarted), b2s(expected.FirstAttemptStarted)},
		{"DispatchTimeSet", b2s(observed.DispatchTimeSet), b2s(expected.DispatchTimeSet)},
	}
	return saaDiffBlock(saaSplit(fields))
}

func saaFlagRows(observed, expected saaspec.HeartbeatFlags) (rows [][3]string, agree []string) {
	fields := [][3]string{
		{"CancelRequested", fmt.Sprint(observed.CancelRequested), fmt.Sprint(expected.CancelRequested)},
		{"ActivityPaused", fmt.Sprint(observed.ActivityPaused), fmt.Sprint(expected.ActivityPaused)},
		{"ActivityReset", fmt.Sprint(observed.ActivityReset), fmt.Sprint(expected.ActivityReset)},
	}
	return saaSplit(fields)
}

// saaSplit partitions (field, observed, expected) triples into the ones that differ (rows) and the
// ones that agree, the latter rendered as "field=value" so the report shows every field's value.
func saaSplit(fields [][3]string) (rows [][3]string, agree []string) {
	for _, f := range fields {
		if f[1] != f[2] {
			rows = append(rows, f)
		} else {
			agree = append(agree, f[0]+"="+f[1])
		}
	}
	return rows, agree
}

// saaDiffBlock formats differing (field, observed, expected) rows as an aligned three-column table
// and lists the agreeing "field=value" pairs beneath it.
func saaDiffBlock(rows [][3]string, agree []string) string {
	const hName, hObs, hExp = "field", "observed (server)", "expected (model)"
	nameW, obsW := len(hName), len(hObs)
	for _, r := range rows {
		nameW = max(nameW, len(r[0]))
		obsW = max(obsW, len(r[1]))
	}
	var b strings.Builder
	fmt.Fprintf(&b, "  %-*s   %-*s   %s\n", nameW, hName, obsW, hObs, hExp)
	for _, r := range rows {
		fmt.Fprintf(&b, "  %-*s   %-*s   %s\n", nameW, r[0], obsW, r[1], r[2])
	}
	if len(agree) > 0 {
		fmt.Fprintf(&b, "  agree: %s", strings.Join(agree, " "))
	}
	return b.String()
}

func saaOutcomeDesc(k saaspec.ErrorKind) string {
	if k == saaspec.NoError {
		return "accepted"
	}
	return "rejected with " + saaRejectKindName(k)
}

func saaRejectKindName(k saaspec.ErrorKind) string {
	switch k {
	case saaspec.NoError:
		return "NoError"
	case saaspec.FailedPrecondition:
		return "FailedPrecondition"
	case saaspec.NotFound:
		return "NotFound"
	case saaspec.InvalidArgument:
		return "InvalidArgument"
	default:
		return fmt.Sprintf("unrecognized(%d)", int(k))
	}
}

func saaKindName(k saaspec.EventKind) string {
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
	case saaspec.ScheduleToStartElapses:
		return "ScheduleToStartElapses"
	case saaspec.ScheduleToCloseElapses:
		return "ScheduleToCloseElapses"
	case saaspec.StartToCloseElapses:
		return "StartToCloseElapses"
	case saaspec.HeartbeatElapses:
		return "HeartbeatElapses"
	case saaspec.StartDelayElapses:
		return "StartDelayElapses"
	case saaspec.BackoffElapses:
		return "BackoffElapses"
	default:
		return fmt.Sprintf("EventKind(%d)", k)
	}
}
