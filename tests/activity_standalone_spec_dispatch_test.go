package tests

// Dispatch-timing probes for the standalone-activity behavior spec.
//
// A deferred dispatch — a start_delay before the first attempt, or a retry backoff (policy-derived
// or a worker-supplied next_retry_delay) before a later attempt — holds a SCHEDULED activity
// non-dispatchable until a wall-clock instant. This is pure timing: the persisted state does not
// change when the deferral elapses (the status stays SCHEDULED; only a Poll observes the
// difference), so it is outside saaspec.Model's exact-state oracle. These probes observe it the one
// way it is observable — by polling — and assert an ordering: no task is dispatched during the
// deferral window, and a task with the expected attempt number is dispatched once it elapses. The
// SCHEDULED->STARTED state after the successful poll still comes from Model.
//
// A valid long poll must run at least common.MinLongPollTimeout (2s), so the deferral windows here
// are several seconds; each probe costs roughly the window length.

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm/lib/activity/saaspec"
)

const (
	// saaDispatchWindow is the deferral length configured for probes that assert "not dispatchable
	// until it elapses". It must exceed saaNegativePollTimeout so the negative poll sits inside the
	// window.
	saaDispatchWindow = 6 * time.Second
	// saaDispatchSettle is slack added after the window before the positive poll, absorbing the gap
	// between the deferral's true origin (schedule time / attempt complete time) and when the probe
	// captures it.
	saaDispatchSettle = 2 * time.Second
)

type saaDispatchProbe struct {
	name           string
	cfg            saaspec.Config
	startDelay     time.Duration   // StartActivityExecutionRequest.StartDelay
	retryInterval  time.Duration   // RetryPolicy InitialInterval (0 => harness default)
	nextRetryDelay time.Duration   // worker-supplied ApplicationFailureInfo.NextRetryDelay on RespondFailed
	path           []saaspec.Event // RPCs to reach the pre-dispatch SCHEDULED state
	window         time.Duration   // deferral expected from the end of path; 0 => must dispatch immediately
	expectAttempt  int32           // attempt number the eventual poll must return
}

var saaDispatchProbes = []saaDispatchProbe{
	// A start_delay holds the first dispatch until schedule_time + start_delay.
	{
		name: "startDelay/first-dispatch", cfg: saaspec.Config{HasStartDelay: true},
		startDelay: saaDispatchWindow, window: saaDispatchWindow, expectAttempt: 1,
	},
	// Reset while still in the start-delay window preserves the remaining delay (reset discards a
	// retry backoff but not a pending start_delay), so the first dispatch stays deferred.
	{
		name: "startDelay/reset-preserves", cfg: saaspec.Config{HasStartDelay: true},
		startDelay: saaDispatchWindow, path: []saaspec.Event{{Kind: saaspec.Reset}},
		window: saaDispatchWindow, expectAttempt: 1,
	},
	// A retryable failure defers the next attempt by the policy backoff interval
	// (complete_time + interval).
	{
		name: "backoff/policy", retryInterval: saaDispatchWindow,
		path:   []saaspec.Event{{Kind: saaspec.Poll}, {Kind: saaspec.RespondFailed, Retryable: true}},
		window: saaDispatchWindow, expectAttempt: 2,
	},
	// A worker-supplied next_retry_delay overrides the (short, default) policy interval: the retry is
	// deferred by the override. If the override were ignored the retry would dispatch within the
	// negative-poll window and the probe would catch it.
	{
		name: "backoff/next-retry-delay", nextRetryDelay: saaDispatchWindow,
		path:   []saaspec.Event{{Kind: saaspec.Poll}, {Kind: saaspec.RespondFailed, Retryable: true}},
		window: saaDispatchWindow, expectAttempt: 2,
	},
	// Reset while a retry backoff is pending discards the backoff, so the reset attempt dispatches
	// immediately rather than at complete_time + interval.
	{
		name: "backoff/reset-discards", retryInterval: saaDispatchWindow,
		path:   []saaspec.Event{{Kind: saaspec.Poll}, {Kind: saaspec.RespondFailed, Retryable: true}, {Kind: saaspec.Reset}},
		window: 0, expectAttempt: 1,
	},
}

func (s *standaloneActivityTestSuite) TestSpecDispatchProbes() {
	env := s.newTestEnv()
	t := s.T()
	ctx := s.Context()

	chasmCtx, err := env.GetTestCluster().Host().ChasmContext(ctx)
	require.NoError(t, err)

	// Each probe is an independent subtest, so `-run 'TestSpecDispatchProbes/backoff'` selects by
	// scenario (names embed a "/" hierarchy: e.g. "backoff/next-retry-delay").
	for i, p := range saaDispatchProbes {
		t.Run(p.name, func(t *testing.T) {
			ex := &saaExplorer{
				env: env, ctx: ctx, chasmCtx: chasmCtx, nsID: env.NamespaceID().String(),
				cfg: p.cfg, cfgIdx: i,
				startDelay: p.startDelay, retryInterval: p.retryInterval, nextRetryDelay: p.nextRetryDelay,
			}
			p.run(t, ex)
		})
	}
}

func (p saaDispatchProbe) run(t *testing.T, ex *saaExplorer) {
	a := ex.start(t)
	a.path = p.path
	cur := saaspec.Initial(p.cfg)

	obs, err := a.observed()
	require.NoError(t, err)
	if obs != cur {
		t.Errorf("after Start, state got %+v want %+v", obs, cur)
		return
	}

	for _, e := range p.path {
		out := saaspec.Model(p.cfg, cur, e)
		if a.apply(t, e, cur, out, false) != saaVerified {
			t.Errorf("could not reach pre-dispatch state (diverged driving %s)", saaKindName(e.Kind))
			return
		}
		cur = out.Next
	}
	if cur.Status != saaspec.Scheduled {
		t.Fatalf("probe %q reaches %s, not a pre-dispatch SCHEDULED state", p.name, cur.Status)
	}

	// origin approximates the deferral's start: schedule time (start_delay) or the failed attempt's
	// complete time (backoff), both essentially "now" once the path has run.
	origin := time.Now()
	if p.window > 0 {
		if resp := a.pollForTask(t, saaNegativePollTimeout); resp != nil {
			t.Errorf("a task was dispatched during the %s deferral window (attempt=%d); expected none until it elapsed\n%s",
				p.window, resp.GetAttempt(), a.pathLine())
			return
		}
		time.Sleep(time.Until(origin.Add(p.window + saaDispatchSettle)))
	}

	resp := a.pollForTask(t, saaNegativePollTimeout)
	if resp == nil {
		t.Errorf("no task dispatched once the deferral elapsed (window=%s)\n%s", p.window, a.pathLine())
		return
	}
	a.token = resp.GetTaskToken()
	if resp.GetAttempt() != p.expectAttempt {
		t.Errorf("dispatched attempt number: server saw %d, expected %d\n%s", resp.GetAttempt(), p.expectAttempt, a.pathLine())
	}

	out := saaspec.Model(p.cfg, cur, saaspec.Event{Kind: saaspec.Poll})
	obs, err = a.observed()
	require.NoError(t, err)
	if obs != out.Next {
		t.Errorf("after dispatch + poll:\n%s", saaStateDiff(obs, out.Next))
	}
}
