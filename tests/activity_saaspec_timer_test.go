package tests

// Timer probes for the standalone-activity behavior spec. A timer firing is modeled as an event
// (saaspec.ScheduleToCloseFires, etc.); this harness triggers it by configuring the matching
// timeout short and waiting, then asserts the resulting internal state equals what Model()
// predicts for that (state, timer-event). The harness never encodes the intended outcome — it
// comes entirely from Model() — so filling in the timer cases in Model() is what activates each
// probe. Probes whose (state, timer-event) Model() has not decided are skipped.

import (
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm/lib/activity/saaspec"
)

type saaTimerProbe struct {
	name  string
	timer saaspec.EventKind // one of the *Fires kinds
	cfg   saaspec.Config    // the timer under test is made short via ex.shortTimer
	path  []saaspec.Event   // RPCs to reach the source state (other timers stay long)
	wait  time.Duration     // how long to wait for the timer to fire
}

var saaTimerWait = saaShortTimer + 2*time.Second

var saaTimerProbes = []saaTimerProbe{
	// The schedule-to-close deadline keeps running while paused (intended: SAA departs from the
	// workflow-activity behavior here).
	{"STC/paused", saaspec.ScheduleToCloseFires, saaspec.Config{HasScheduleToClose: true}, []saaspec.Event{{Kind: saaspec.Pause}}, saaTimerWait},
	{"S2S/scheduled", saaspec.ScheduleToStartFires, saaspec.Config{HasScheduleToStart: true}, nil, saaTimerWait},
	{"startToClose/started-retry", saaspec.StartToCloseFires, saaspec.Config{}, []saaspec.Event{{Kind: saaspec.Poll}}, saaTimerWait},
	{"startToClose/started-exhausted", saaspec.StartToCloseFires, saaspec.Config{MaxAttempts: 1}, []saaspec.Event{{Kind: saaspec.Poll}}, saaTimerWait},
	{"heartbeat/started-retry", saaspec.HeartbeatFires, saaspec.Config{HasHeartbeat: true}, []saaspec.Event{{Kind: saaspec.Poll}}, saaTimerWait},
	{"heartbeat/started-exhausted", saaspec.HeartbeatFires, saaspec.Config{HasHeartbeat: true, MaxAttempts: 1}, []saaspec.Event{{Kind: saaspec.Poll}}, saaTimerWait},
}

func (s *standaloneActivityTestSuite) TestSpecTimerProbes() {
	env := s.newTestEnv()
	t := s.T()
	ctx := s.Context()

	chasmCtx, err := env.GetTestCluster().Host().ChasmContext(ctx)
	require.NoError(t, err)

	verified, skipped := 0, 0
	for i, p := range saaTimerProbes {
		ex := &saaExplorer{
			env: env, ctx: ctx, chasmCtx: chasmCtx, nsID: env.NamespaceID().String(),
			cfg: p.cfg, cfgIdx: i, shortTimer: p.timer,
		}
		a := ex.start(t)
		cur := saaspec.Initial(p.cfg)

		obs, err := a.observed()
		require.NoError(t, err)
		if obs != cur {
			t.Errorf("probe %s: after Start, state got %+v want %+v", p.name, obs, cur)
			continue
		}

		aborted := false
		for _, e := range p.path {
			out := saaspec.Model(p.cfg, cur, e)
			if !a.apply(t, e, cur, out, false) {
				t.Errorf("probe %s: could not reach source state (diverged driving %s)", p.name, saaKindName(e.Kind))
				aborted = true
				break
			}
			cur = out.Next
		}
		if aborted {
			continue
		}

		out, decided := saaEvalModel(p.cfg, cur, saaspec.Event{Kind: p.timer})
		if !decided {
			t.Logf("probe %s: Model has not decided %s from %s; skipping", p.name, saaKindName(p.timer), cur.Status)
			skipped++
			continue
		}

		src := cur.Status
		time.Sleep(p.wait)
		obs, err = a.observed()
		require.NoError(t, err)
		if obs != out.Next {
			t.Errorf("probe %s: %s from %s: state got %+v want %+v", p.name, saaKindName(p.timer), src, obs, out.Next)
		} else {
			verified++
		}
	}
	t.Logf("timer probes: verified=%d skipped(undecided)=%d", verified, skipped)
}
