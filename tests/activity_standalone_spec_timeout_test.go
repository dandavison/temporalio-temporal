package tests

// Timeout traces for the standalone-activity behavior spec. A timeout firing is modeled as an event
// (saaspec.ScheduleToCloseFires, etc.); this harness triggers it by configuring the matching timeout
// short and waiting. Each is a trace — RPCs to reach a source state, then the timeout event — driven
// by driveTrace, so the resulting state is checked against Model() exactly like every other event.
// The harness never encodes the intended outcome; Model() does. Model must handle every timeout event;
// a trace for a (state, timeout-event) that Model() does not handle panics, failing the run.

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

	// Dispatch-delay interaction with a timeout (a long start_delay keeps the first attempt pending
	// the whole trace, so the short timeout fires during the start-delay window): schedule-to-start is
	// pushed back behind the start delay -> it must NOT fire during the window, so the activity stays
	// SCHEDULED (Model(StartDelayPending, ScheduleToStartFires) is a no-op).
	//
	// The schedule-to-close/start-delay interaction (req 1) is a pending spec decision — see
	// TestSpecKnownGaps — so no trace for it yet.
	{name: "S2S/pushed-back-by-start-delay", timeout: saaspec.ScheduleToStartFires, cfg: saaspec.Config{HasStartDelay: true, HasScheduleToStart: true}, startDelay: saaLongStartDelay},
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
