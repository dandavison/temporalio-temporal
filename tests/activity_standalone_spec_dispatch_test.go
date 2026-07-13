package tests

// Delayed-dispatch verification for the standalone-activity behavior spec: it checks the
// implementation against the delayed-dispatch behavior that saaspec.Model() specifies via the
// Dispatchability field (start_delay / retry backoff) and the *Elapses events.
//
// It drives model-generated traces: each trace is an event sequence run once on a single activity,
// and at every step the harness asserts the observed state and pollability against Model(). The
// harness carries no product logic — Model() alone decides, at each step, whether a poll should find
// a task (Dispatchability == Dispatchable) or nothing (a delay still pending), and what state results; a
// *Elapses event is realized by waiting for the real timer. The trace list is only coverage
// selection.
//
// Why traces rather than the full breadth-first walk TestSpecExplorer runs: each pending-delay
// step costs a real multi-second wait or long poll (a delay must outlast the long-poll minimum to
// be observable), and the explorer replays every path from a fresh activity, which would re-incur
// every prefix wait. A trace incurs each wait once, so representative orderings fit the test's time
// budget; an exhaustive walk of the delay dimension does not, absent clock control.

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm/lib/activity/saaspec"
)

// The delay windows are long enough to outlast a valid negative long poll (> the long-poll
// minimum), so "not dispatchable yet" is observable.
const saaDispatchWindow = 5 * time.Second

type saaDispatchTrace struct {
	name           string
	cfg            saaspec.Config
	startDelay     time.Duration
	retryInterval  time.Duration
	nextRetryDelay time.Duration // worker-supplied override of the policy backoff
	trace          []saaspec.Event
}

var (
	saaPoll      = saaspec.Event{Kind: saaspec.Poll}
	saaFailRetry = saaspec.Event{Kind: saaspec.RespondFailed, Retryable: true}
	saaSDElapse  = saaspec.Event{Kind: saaspec.StartDelayElapses}
	saaBOElapse  = saaspec.Event{Kind: saaspec.BackoffElapses}
)

var saaDispatchTraces = []saaDispatchTrace{
	// start_delay delays the first dispatch: a poll finds no task until the delay elapses.
	{
		name: "start-delay/first-dispatch", cfg: saaspec.Config{HasStartDelay: true}, startDelay: saaDispatchWindow,
		trace: []saaspec.Event{saaPoll, saaSDElapse, saaPoll},
	},
	// pause during the start delay, then unpause: still delayed (poll finds nothing) until it elapses.
	{
		name: "start-delay/pause-then-unpause", cfg: saaspec.Config{HasStartDelay: true}, startDelay: saaDispatchWindow,
		trace: []saaspec.Event{{Kind: saaspec.Pause}, {Kind: saaspec.Unpause}, saaPoll, saaSDElapse, saaPoll},
	},
	// reset during the start delay: still delayed (behaves like unpause).
	{
		name: "start-delay/reset", cfg: saaspec.Config{HasStartDelay: true}, startDelay: saaDispatchWindow,
		trace: []saaspec.Event{{Kind: saaspec.Reset}, saaPoll, saaSDElapse, saaPoll},
	},
	// a retry is delayed by the policy backoff: a poll finds no task until the backoff elapses.
	{
		name: "backoff/retry-dispatch", cfg: saaspec.Config{MaxAttempts: 3}, retryInterval: saaDispatchWindow,
		trace: []saaspec.Event{saaPoll, saaFailRetry, saaPoll, saaBOElapse, saaPoll},
	},
	// a worker-supplied next_retry_delay overrides the (short, default) policy interval: the retry is
	// delayed by the override. The policy backoff is ~200ms, so if the override were ignored the
	// retry would dispatch during the negative poll and the trace would catch it.
	{
		name: "backoff/next-retry-delay-override", cfg: saaspec.Config{MaxAttempts: 3}, nextRetryDelay: saaDispatchWindow,
		trace: []saaspec.Event{saaPoll, saaFailRetry, saaPoll, saaBOElapse, saaPoll},
	},
	// pause during the backoff, then unpause: still delayed until the backoff elapses.
	{
		name: "backoff/pause-then-unpause", cfg: saaspec.Config{MaxAttempts: 3}, retryInterval: saaDispatchWindow,
		trace: []saaspec.Event{saaPoll, saaFailRetry, {Kind: saaspec.Pause}, {Kind: saaspec.Unpause}, saaPoll, saaBOElapse, saaPoll},
	},
	// reset during the backoff discards it: the reset attempt dispatches immediately.
	{
		name: "backoff/reset", cfg: saaspec.Config{MaxAttempts: 3}, retryInterval: saaDispatchWindow,
		trace: []saaspec.Event{saaPoll, saaFailRetry, {Kind: saaspec.Reset}, saaPoll},
	},
}

func (s *standaloneActivityTestSuite) TestSpecDispatchDelays() {
	env := s.newTestEnv()

	for i, tr := range saaDispatchTraces {
		s.T().Run(tr.name, func(t *testing.T) {
			// Fresh context per trace: each carries the default 90s deadline, so the multi-second
			// waits do not accumulate against a single shared budget.
			ctx := s.Context()
			chasmCtx, err := env.GetTestCluster().Host().ChasmContext(ctx)
			require.NoError(t, err)
			ex := &saaExplorer{
				env: env, ctx: ctx, chasmCtx: chasmCtx, nsID: env.NamespaceID().String(),
				cfg: tr.cfg, cfgIdx: i,
				startDelay: tr.startDelay, retryInterval: tr.retryInterval, nextRetryDelay: tr.nextRetryDelay,
				// "Dispatchable" must mean "dispatches promptly", so bound the positive poll below the
				// delay window — that is how reset-discards (immediate) is told from still-delayed.
				positivePollTimeout: saaNegativePollTimeout,
			}
			ex.driveTrace(t, tr.trace)
		})
	}
}
