package tests

// Delayed-dispatch verification for the standalone-activity spec: checks the impl against the
// Dispatchability behavior Model() specifies (start_delay / retry backoff) and the *Elapses events.
// Each trace is an event sequence run once on one activity; at every step the harness asserts the
// observed state and pollability against Model(). A *Elapses event is realized by waiting for the
// real timer.
//
// Traces rather than the breadth-first TestSpecRPCGraphTraversal because each pending-delay step costs
// a real multi-second wait, and the traversal replays every path from scratch (re-incurring every
// prefix wait). A trace pays each wait once.

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
	// update start_delay to a long value during the delay window, then UpdateOptions(RestoreOriginal):
	// the dispatch window must return to the original start_delay. If restore-original did not restore
	// start_delay, the window would stay long and the final poll (after the original delay elapses)
	// would find no task. This gives UpdateOptions(RestoreOriginal) genuine state-level coverage.
	{
		name: "start-delay/update-then-restore-original", cfg: saaspec.Config{HasStartDelay: true}, startDelay: saaDispatchWindow,
		trace: []saaspec.Event{
			{Kind: saaspec.UpdateOptions, SetsStartDelay: true},
			{Kind: saaspec.UpdateOptions, RestoreOriginal: true},
			saaPoll, saaSDElapse, saaPoll,
		},
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
	// pause/unpause during the backoff, then an unrelated options update: the pending backoff must
	// survive the update and not re-dispatch early (regression guard for the unpause path clearing
	// CurrentRetryInterval so a later re-dispatch loses the retry deadline).
	{
		name: "backoff/pause-unpause-then-update", cfg: saaspec.Config{MaxAttempts: 3}, retryInterval: saaDispatchWindow,
		trace: []saaspec.Event{saaPoll, saaFailRetry, {Kind: saaspec.Pause}, {Kind: saaspec.Unpause}, {Kind: saaspec.UpdateOptions}, saaPoll, saaBOElapse, saaPoll},
	},
	// a worker next_retry_delay override followed by an unrelated options update: the override must be
	// preserved (not recalculated to the short policy interval), so the retry stays delayed. The policy
	// backoff is ~200ms, so if the update dropped the override the retry would dispatch during the
	// negative poll and the trace would catch it.
	{
		name: "backoff/next-retry-delay-override-then-update", cfg: saaspec.Config{MaxAttempts: 3}, nextRetryDelay: saaDispatchWindow,
		trace: []saaspec.Event{saaPoll, saaFailRetry, {Kind: saaspec.UpdateOptions}, saaPoll, saaBOElapse, saaPoll},
	},
	// reset during the backoff discards it: the reset attempt dispatches immediately.
	{
		name: "backoff/reset", cfg: saaspec.Config{MaxAttempts: 3}, retryInterval: saaDispatchWindow,
		trace: []saaspec.Event{saaPoll, saaFailRetry, {Kind: saaspec.Reset}, saaPoll},
	},
}

func (s *standaloneActivityTestSuite) TestSpecDispatchDelayPaths() {
	env := s.newTestEnv()

	for i, tr := range saaDispatchTraces {
		s.T().Run(tr.name, func(t *testing.T) {
			// Fresh context per trace: each carries the default 90s deadline, so the multi-second
			// waits do not accumulate against a single shared budget.
			ctx := s.Context()
			chasmCtx, err := env.GetTestCluster().Host().ChasmContext(ctx)
			require.NoError(t, err)
			h := &saaHarness{
				env: env, ctx: ctx, chasmCtx: chasmCtx, nsID: env.NamespaceID().String(),
				cfg: tr.cfg, cfgIdx: i,
				startDelay: tr.startDelay, retryInterval: tr.retryInterval, nextRetryDelay: tr.nextRetryDelay,
				// "Dispatchable" must mean "dispatches promptly", so bound the positive poll below the
				// delay window — that is how reset-discards (immediate) is told from still-delayed.
				positivePollTimeout: saaNegativePollTimeout,
			}
			h.driveTrace(t, tr.trace)
		})
	}
}
