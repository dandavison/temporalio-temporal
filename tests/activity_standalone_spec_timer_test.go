package tests

// Timer probes for the standalone-activity behavior spec. A timer firing is modeled as an event
// (saaspec.ScheduleToCloseFires, etc.); this harness triggers it by configuring the matching
// timeout short and waiting, then asserts the resulting internal state equals what Model()
// predicts for that (state, timer-event). The harness never encodes the intended outcome — it
// comes entirely from Model(). Model must handle every timer event; a probe for a (state,
// timer-event) that Model() does not handle panics, failing the probe.

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm/lib/activity/saaspec"
)

type saaTimerProbe struct {
	name       string
	timer      saaspec.EventKind // one of the *Fires kinds
	cfg        saaspec.Config    // the timer under test is made short via ex.shortTimer
	path       []saaspec.Event   // RPCs to reach the source state (other timers stay long)
	wait       time.Duration     // how long to wait for the timer to fire
	startDelay time.Duration     // StartActivityExecutionRequest.StartDelay (0 => none)
}

var saaTimerWait = saaShortTimer + 2*time.Second

// saaLongStartDelay keeps a first attempt in its start-delay window for the whole probe, so a short
// timer under test fires while the activity is still SCHEDULED and pending dispatch.
const saaLongStartDelay = time.Hour

var saaTimerProbes = []saaTimerProbe{
	// The schedule-to-close deadline keeps running while paused (intended: SAA departs from the
	// workflow-activity behavior here).
	{name: "STC/paused", timer: saaspec.ScheduleToCloseFires, cfg: saaspec.Config{HasScheduleToClose: true}, path: []saaspec.Event{{Kind: saaspec.Pause}}, wait: saaTimerWait},
	{name: "S2S/scheduled", timer: saaspec.ScheduleToStartFires, cfg: saaspec.Config{HasScheduleToStart: true}, wait: saaTimerWait},
	// Stale-task no-op: pausing a SCHEDULED activity bumps the stamp, invalidating the pending
	// schedule-to-start task; it must not fire. Model(Paused, ScheduleToStartFires) should be a
	// no-op, so the harness waits and asserts the activity is still PAUSED.
	{name: "S2S/paused-stale", timer: saaspec.ScheduleToStartFires, cfg: saaspec.Config{HasScheduleToStart: true}, path: []saaspec.Event{{Kind: saaspec.Pause}}, wait: saaTimerWait},
	{name: "startToClose/started-retry", timer: saaspec.StartToCloseFires, cfg: saaspec.Config{}, path: []saaspec.Event{{Kind: saaspec.Poll}}, wait: saaTimerWait},
	{name: "startToClose/started-exhausted", timer: saaspec.StartToCloseFires, cfg: saaspec.Config{MaxAttempts: 1}, path: []saaspec.Event{{Kind: saaspec.Poll}}, wait: saaTimerWait},
	{name: "heartbeat/started-retry", timer: saaspec.HeartbeatFires, cfg: saaspec.Config{HasHeartbeat: true}, path: []saaspec.Event{{Kind: saaspec.Poll}}, wait: saaTimerWait},
	{name: "heartbeat/started-exhausted", timer: saaspec.HeartbeatFires, cfg: saaspec.Config{HasHeartbeat: true, MaxAttempts: 1}, path: []saaspec.Event{{Kind: saaspec.Poll}}, wait: saaTimerWait},

	// Deferred-dispatch interaction with a timeout (a long start_delay keeps the first attempt pending
	// the whole probe, so the short timer fires during the start-delay window): schedule-to-start is
	// pushed back behind the start delay -> it must NOT fire during the window, so the activity stays
	// SCHEDULED (Model(StartDelayPending, ScheduleToStartFires) is a no-op).
	//
	// The schedule-to-close/start-delay interaction (req 1) is a pending spec decision — see
	// TestSpecKnownGaps — so no probe for it yet.
	{name: "S2S/pushed-back-by-start-delay", timer: saaspec.ScheduleToStartFires, cfg: saaspec.Config{HasStartDelay: true, HasScheduleToStart: true}, wait: saaTimerWait, startDelay: saaLongStartDelay},
}

func (s *standaloneActivityTestSuite) TestSpecTimerProbes() {
	env := s.newTestEnv()
	t := s.T()
	ctx := s.Context()

	chasmCtx, err := env.GetTestCluster().Host().ChasmContext(ctx)
	require.NoError(t, err)

	// Each probe is an independent subtest, so `-run 'TestSpecTimerProbes/heartbeat'` selects by
	// timer/scenario (names embed a "/" hierarchy: e.g. "heartbeat/started-retry").
	for i, p := range saaTimerProbes {
		t.Run(p.name, func(t *testing.T) {
			ex := &saaExplorer{
				env: env, ctx: ctx, chasmCtx: chasmCtx, nsID: env.NamespaceID().String(),
				cfg: p.cfg, cfgIdx: i, shortTimer: p.timer, startDelay: p.startDelay,
			}
			a := ex.start(t)
			cur := saaspec.Initial(p.cfg)

			obs, err := a.observed()
			require.NoError(t, err)
			if !cur.SameObserved(obs) {
				t.Errorf("after Start, state got %+v want %+v", obs, cur)
				return
			}

			for _, e := range p.path {
				out := saaspec.Model(p.cfg, cur, e)
				if a.apply(t, e, cur, out, false) != saaVerified {
					t.Errorf("could not reach source state (diverged driving %s)", saaKindName(e.Kind))
					return
				}
				cur = out.Next
			}

			out := saaspec.Model(p.cfg, cur, saaspec.Event{Kind: p.timer})

			src := cur.Status
			time.Sleep(p.wait)
			obs, err = a.observed()
			require.NoError(t, err)
			if !out.Next.SameObserved(obs) {
				t.Errorf("%s from %s: state got %+v want %+v", saaKindName(p.timer), src, obs, out.Next)
			}
		})
	}
}
