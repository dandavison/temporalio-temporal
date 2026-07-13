package tests

// Timeout traces for the standalone-activity behavior spec. A timeout firing is modeled as an event
// (saaspec.ScheduleToCloseFires, etc.); this harness triggers it by configuring the matching timeout
// short and waiting. Each is a trace — RPCs to reach a source state, then the timeout event — driven
// by driveTrace, so the resulting state is checked against Model() exactly like every other event.
// The harness never encodes the intended outcome; Model() does. Model must handle every timeout event;
// a trace for a (state, timeout-event) that Model() does not handle panics, failing the run.
//
// How a trace runs (each step below is one apply() call inside driveTrace):
//  1. TestSpecTimeouts builds the trace = path ++ [timeout] and sets ex.shortTimeout, which makes
//     startRequest configure that one timeout short (saaShortTimeout, 2s) and all others long (1h),
//     so nothing else fires mid-trace.
//  2. driveTrace starts one fresh activity and drives each event, comparing the observed state to
//     Model() at every step.
//  3. The RPC path steps advance to the source state; the final timeout event routes to
//     applyWallClock, which sleeps out the 2s timeout, then asserts the resulting state == Model.Next.
//
// Worked example — "startToClose/started-retry" (trace [Poll, StartToCloseFires], start-to-close made
// short): driveTrace starts an activity; Poll drives a real poll (SCHEDULED -> STARTED); then
// StartToCloseFires is driven by sleeping until the 2s start-to-close fires on the server, which
// retries the attempt (back to SCHEDULED, attempt 2). The S2S/* traces instead assert a timeout does
// NOT fire — Model returns a no-op, applyWallClock waits the same and the state is asserted unchanged.

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm/lib/activity/saaspec"
)

type saaTimeoutTrace struct {
	name       string
	timeout    saaspec.EventKind // made short at Start (ex.shortTimeout) and appended as the trace's last event
	cfg        saaspec.Config
	path       []saaspec.Event // RPCs to reach the source state (other timeouts stay long)
	startDelay time.Duration   // StartActivityExecutionRequest.StartDelay (0 => none)
}

// saaLongStartDelay keeps a first attempt in its start-delay window for the whole trace, so a short
// timeout under test fires while the activity is still SCHEDULED and pending dispatch.
const saaLongStartDelay = time.Hour

var saaTimeoutTraces = []saaTimeoutTrace{
	// The schedule-to-close deadline keeps running while paused (intended: SAA departs from the
	// workflow-activity behavior here).
	{name: "STC/paused", timeout: saaspec.ScheduleToCloseFires, cfg: saaspec.Config{HasScheduleToClose: true}, path: []saaspec.Event{{Kind: saaspec.Pause}}},
	{name: "S2S/scheduled", timeout: saaspec.ScheduleToStartFires, cfg: saaspec.Config{HasScheduleToStart: true}},
	// Stale-task no-op: pausing a SCHEDULED activity bumps the stamp, invalidating the pending
	// schedule-to-start task; it must not fire. Model(Paused, ScheduleToStartFires) should be a
	// no-op, so the harness waits and asserts the activity is still PAUSED.
	{name: "S2S/paused-stale", timeout: saaspec.ScheduleToStartFires, cfg: saaspec.Config{HasScheduleToStart: true}, path: []saaspec.Event{{Kind: saaspec.Pause}}},
	{name: "startToClose/started-retry", timeout: saaspec.StartToCloseFires, cfg: saaspec.Config{}, path: []saaspec.Event{{Kind: saaspec.Poll}}},
	{name: "startToClose/started-exhausted", timeout: saaspec.StartToCloseFires, cfg: saaspec.Config{MaxAttempts: 1}, path: []saaspec.Event{{Kind: saaspec.Poll}}},
	{name: "heartbeat/started-retry", timeout: saaspec.HeartbeatFires, cfg: saaspec.Config{HasHeartbeat: true}, path: []saaspec.Event{{Kind: saaspec.Poll}}},
	{name: "heartbeat/started-exhausted", timeout: saaspec.HeartbeatFires, cfg: saaspec.Config{HasHeartbeat: true, MaxAttempts: 1}, path: []saaspec.Event{{Kind: saaspec.Poll}}},

	// Dispatch-delay interaction with the timeouts: a long start_delay keeps the first attempt pending
	// for the whole trace, and both schedule-to-start and schedule-to-close are anchored to the
	// first-dispatch time (schedule_time + start_delay). So neither may fire while the dispatch is
	// still delayed — the activity stays SCHEDULED — i.e. Model(StartDelayPending, *) is a no-op.
	{name: "S2S/pushed-back-by-start-delay", timeout: saaspec.ScheduleToStartFires, cfg: saaspec.Config{HasStartDelay: true, HasScheduleToStart: true}, startDelay: saaLongStartDelay},
	{name: "S2S/pushed-back-by-start-delay", timeout: saaspec.ScheduleToCloseFires, cfg: saaspec.Config{HasStartDelay: true, HasScheduleToClose: true}, startDelay: saaLongStartDelay},
}

func (s *standaloneActivityTestSuite) TestSpecTimeouts() {
	env := s.newTestEnv()
	t := s.T()
	ctx := s.Context()

	chasmCtx, err := env.GetTestCluster().Host().ChasmContext(ctx)
	require.NoError(t, err)

	// Each trace is an independent subtest, so `-run 'TestSpecTimeouts/heartbeat'` selects by
	// timeout/scenario (names embed a "/" hierarchy: e.g. "heartbeat/started-retry").
	for i, p := range saaTimeoutTraces {
		t.Run(p.name, func(t *testing.T) {
			ex := &saaExplorer{
				env: env, ctx: ctx, chasmCtx: chasmCtx, nsID: env.NamespaceID().String(),
				cfg: p.cfg, cfgIdx: i, shortTimeout: p.timeout, startDelay: p.startDelay,
			}
			// The trace reaches the source state via the RPC path, then fires the timeout under test.
			ex.driveTrace(t, append(append([]saaspec.Event{}, p.path...), saaspec.Event{Kind: p.timeout}))
		})
	}
}
