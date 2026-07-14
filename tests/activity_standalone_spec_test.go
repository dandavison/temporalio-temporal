package tests

// Executable behavior spec for the standalone-activity (SAA) product surface. saaspec.Model() is the
// spec — a total function Model(cfg, state, event) -> Outcome — and these tests drive a real onebox
// server through the same event alphabet, asserting the server agrees with Model() at every step.
//
// This file holds the parts intended to be edited: the traversal configs and the traces (top),
// followed by the three test entry points, all subtests of TestStandaloneActivityTestSuite/TestSpec.
// The harness that drives and checks each event lives in activity_standalone_spec_utils_test.go.

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm/lib/activity/saaspec"
	"go.temporal.io/server/common/testing/testcontext"
)

// ---------------------------------------------------------------------------------------------
// Configs and traces (edit these)
// ---------------------------------------------------------------------------------------------

// saaTraversalConfigs are the activity configurations the graph traversal and random walk explore.
var saaTraversalConfigs = []saaspec.Config{
	{}, // no schedule-to-close, unlimited attempts
	{HasScheduleToClose: true, HasScheduleToStart: true, HasHeartbeat: true, MaxAttempts: 3},
	// Retries exhaust after the first attempt, so the RespondFailed exhaustion boundary
	// (retryable failure with no retries left -> Failed) is reached at depth 2 rather than
	// past the depth bound. See the completeness check.
	{MaxAttempts: 1},
	// Start-delay window: the activity stays StartDelayPending for the whole traversal (no RPC event
	// leaves that state), so this crosses the operator commands (pause/unpause/reset/update/cancel/
	// terminate) with the start-delay window and verifies via the per-Poll negative poll that none of
	// them dispatches early. The second adds schedule-to-close so its window-invalidation is exercised.
	{HasStartDelay: true},
	{HasStartDelay: true, HasScheduleToClose: true},
}

// A trace is an event sequence run once on one fresh activity; the harness checks the observed state
// against Model() after every event. Reach for a trace (rather than the graph traversal) when a step
// needs a real wall-clock wait — a timeout firing or a start-delay/backoff window elapsing — because
// the traversal replays each path from scratch and would re-incur every wait; a trace pays each wait
// once. The timing knobs configure the activity and how long the harness waits; the trace is the
// script. Writing a timeout's *Elapses event into the script is what makes the harness configure that
// timeout short so it actually fires.
type saaTrace struct {
	name           string
	trace          []saaspec.Event
	maxAttempts    int32         // RetryPolicy MaximumAttempts (0 = unlimited); the rest of the Config is derived (see config)
	startDelay     time.Duration // StartActivityExecution start_delay (also how long the harness waits for StartDelayElapses)
	retryInterval  time.Duration // RetryPolicy interval; how long the harness waits for BackoffElapses
	nextRetryDelay time.Duration // worker-supplied next_retry_delay override of the policy backoff
}

// config derives the model Config from the trace. Only MaxAttempts is a free parameter; everything
// else is implied by what the trace uses — a start-delay window (startDelay > 0) or a timeout it fires
// (that timeout's *Elapses event). This is exact because saaspec.Model reads only HasStartDelay,
// HasScheduleToClose and MaxAttempts, and HasScheduleToStart/HasHeartbeat merely tell the harness
// which timeouts to configure — which is precisely the set of timeouts the trace fires. (A future
// trace that needs schedule-to-close configured without firing it — e.g. to check reset/restore
// STC-task invalidation — would need an explicit field; none does today.)
func (tr saaTrace) config() saaspec.Config {
	cfg := saaspec.Config{MaxAttempts: tr.maxAttempts, HasStartDelay: tr.startDelay > 0}
	for _, e := range tr.trace {
		switch e.Kind {
		case saaspec.ScheduleToStartElapses:
			cfg.HasScheduleToStart = true
		case saaspec.ScheduleToCloseElapses:
			cfg.HasScheduleToClose = true
		case saaspec.HeartbeatElapses:
			cfg.HasHeartbeat = true
		}
	}
	return cfg
}

// saaDelayWindow is long enough to outlast a valid negative long poll (> the long-poll minimum), so
// "not dispatchable yet" is observable during a start-delay or backoff window.
const saaDelayWindow = 5 * time.Second

// saaLongStartDelay keeps a first attempt in its start-delay window for the whole trace, so a short
// timeout under test fires while the activity is still SCHEDULED and pending dispatch.
const saaLongStartDelay = time.Hour

var (
	saaPoll               = saaspec.Event{Kind: saaspec.Poll}
	saaFailRetryably      = saaspec.Event{Kind: saaspec.RespondFailed, Retryable: true}
	saaStartDelayElapse   = saaspec.Event{Kind: saaspec.StartDelayElapses}
	saaBackoffDelayElapse = saaspec.Event{Kind: saaspec.BackoffElapses}
)

var saaTraces = []saaTrace{
	// --- start-delay window ---
	// start_delay delays the first dispatch: a poll finds no task until the delay elapses.
	{
		name:       "start-delay/first-dispatch",
		trace:      []saaspec.Event{saaPoll, saaStartDelayElapse, saaPoll},
		startDelay: saaDelayWindow,
	},
	// pause during the start delay, then unpause: still delayed (poll finds nothing) until it elapses.
	{
		name:       "start-delay/pause-then-unpause",
		trace:      []saaspec.Event{{Kind: saaspec.Pause}, {Kind: saaspec.Unpause}, saaPoll, saaStartDelayElapse, saaPoll},
		startDelay: saaDelayWindow,
	},
	// reset during the start delay: still delayed (behaves like unpause).
	{
		name:       "start-delay/reset",
		trace:      []saaspec.Event{{Kind: saaspec.Reset}, saaPoll, saaStartDelayElapse, saaPoll},
		startDelay: saaDelayWindow,
	},
	// update start_delay to a long value during the delay window, then UpdateOptions(RestoreOriginal):
	// the dispatch window must return to the original start_delay. If restore-original did not restore
	// start_delay, the window would stay long and the final poll (after the original delay elapses)
	// would find no task. This gives UpdateOptions(RestoreOriginal) genuine state-level coverage.
	{
		name: "start-delay/update-then-restore-original",
		trace: []saaspec.Event{
			{Kind: saaspec.UpdateOptions, SetsStartDelay: true},
			{Kind: saaspec.UpdateOptions, RestoreOriginal: true},
			saaPoll, saaStartDelayElapse, saaPoll,
		},
		startDelay: saaDelayWindow,
	},

	// --- retry backoff window ---
	// a retry is delayed by the policy backoff: a poll finds no task until the backoff elapses.
	{
		name:          "backoff/retry-dispatch",
		trace:         []saaspec.Event{saaPoll, saaFailRetryably, saaPoll, saaBackoffDelayElapse, saaPoll},
		maxAttempts:   3,
		retryInterval: saaDelayWindow,
	},
	// a worker-supplied next_retry_delay overrides the (short, default) policy interval: the retry is
	// delayed by the override. The policy backoff is ~200ms, so if the override were ignored the
	// retry would dispatch during the negative poll and the trace would catch it.
	{
		name:           "backoff/next-retry-delay-override",
		trace:          []saaspec.Event{saaPoll, saaFailRetryably, saaPoll, saaBackoffDelayElapse, saaPoll},
		maxAttempts:    3,
		nextRetryDelay: saaDelayWindow,
	},
	// pause during the backoff, then unpause: still delayed until the backoff elapses.
	{
		name:          "backoff/pause-then-unpause",
		trace:         []saaspec.Event{saaPoll, saaFailRetryably, {Kind: saaspec.Pause}, {Kind: saaspec.Unpause}, saaPoll, saaBackoffDelayElapse, saaPoll},
		maxAttempts:   3,
		retryInterval: saaDelayWindow,
	},
	// pause/unpause during the backoff, then an unrelated options update: the pending backoff must
	// survive the update and not re-dispatch early (regression guard for the unpause path clearing
	// CurrentRetryInterval so a later re-dispatch loses the retry deadline).
	{
		name:          "backoff/pause-unpause-then-update",
		trace:         []saaspec.Event{saaPoll, saaFailRetryably, {Kind: saaspec.Pause}, {Kind: saaspec.Unpause}, {Kind: saaspec.UpdateOptions}, saaPoll, saaBackoffDelayElapse, saaPoll},
		maxAttempts:   3,
		retryInterval: saaDelayWindow,
	},
	// a worker next_retry_delay override followed by an unrelated options update: the override must be
	// preserved (not recalculated to the short policy interval), so the retry stays delayed. The policy
	// backoff is ~200ms, so if the update dropped the override the retry would dispatch during the
	// negative poll and the trace would catch it.
	{
		name:           "backoff/next-retry-delay-override-then-update",
		trace:          []saaspec.Event{saaPoll, saaFailRetryably, {Kind: saaspec.UpdateOptions}, saaPoll, saaBackoffDelayElapse, saaPoll},
		maxAttempts:    3,
		nextRetryDelay: saaDelayWindow,
	},
	// reset during the backoff discards it: the reset attempt dispatches immediately.
	{
		name:          "backoff/reset",
		trace:         []saaspec.Event{saaPoll, saaFailRetryably, {Kind: saaspec.Reset}, saaPoll},
		maxAttempts:   3,
		retryInterval: saaDelayWindow,
	},

	// --- timeout firing ---
	{
		name:  "schedule-to-close/elapses-while-paused",
		trace: []saaspec.Event{{Kind: saaspec.Pause}, {Kind: saaspec.ScheduleToCloseElapses}},
	},
	{
		name:  "schedule-to-start/elapses-while-scheduled",
		trace: []saaspec.Event{{Kind: saaspec.ScheduleToStartElapses}},
	},
	{
		name:  "schedule-to-start/elapses-while-paused",
		trace: []saaspec.Event{{Kind: saaspec.Pause}, {Kind: saaspec.ScheduleToStartElapses}},
	},
	{
		name:  "start-to-close/elapses-while-started/retries-remain",
		trace: []saaspec.Event{saaPoll, {Kind: saaspec.StartToCloseElapses}},
	},
	{
		name:        "start-to-close/elapses-while-started/last-attempt",
		trace:       []saaspec.Event{saaPoll, {Kind: saaspec.StartToCloseElapses}},
		maxAttempts: 1,
	},
	// A worker that ignores a cancellation request must still time out: even with retries remaining,
	// a per-attempt timeout in CANCEL_REQUESTED ends the activity as TimedOut (not a retry).
	{
		name:  "start-to-close/elapses-while-cancel-requested",
		trace: []saaspec.Event{saaPoll, {Kind: saaspec.RequestCancel}, {Kind: saaspec.StartToCloseElapses}},
	},
	{
		name:  "heartbeat/elapses-while-started/retries-remain",
		trace: []saaspec.Event{saaPoll, {Kind: saaspec.HeartbeatElapses}},
	},
	{
		name:        "heartbeat/elapses-while-started/last-attempt",
		trace:       []saaspec.Event{saaPoll, {Kind: saaspec.HeartbeatElapses}},
		maxAttempts: 1,
	},
	// timeout while still in the start-delay window (activity SCHEDULED, first dispatch pending).
	{
		name:       "schedule-to-start/elapses-within-start-delay",
		trace:      []saaspec.Event{{Kind: saaspec.ScheduleToStartElapses}},
		startDelay: saaLongStartDelay,
	},
	{
		name:       "schedule-to-close/elapses-within-start-delay",
		trace:      []saaspec.Event{{Kind: saaspec.ScheduleToCloseElapses}},
		startDelay: saaLongStartDelay,
	},
}

// ---------------------------------------------------------------------------------------------
// Test entry points
// ---------------------------------------------------------------------------------------------

// TestSpec runs the spec explorers as subtests, so `-run 'TestStandaloneActivityTestSuite/TestSpec'`
// selects them all and each is addressable by name (e.g. .../TestSpec/Traces). Each subtest builds
// its own env (fresh namespace) so activity ids do not collide across explorers.
func (s *standaloneActivityTestSuite) TestSpec() {
	// The explorers run back to back, so TestSpec's combined wall-clock far exceeds the default
	// single-test budget. Raise the suite context deadline before the first newTestEnv fixes it at the
	// default; a larger TEMPORAL_TEST_TIMEOUT (for deep walks) still wins. Each explorer additionally
	// takes its own subtest-scoped context (see specRPCGraphTraversal).
	testcontext.For(s.T(), testcontext.WithTimeout(saaSpecContextBudget()))
	s.T().Run("RPCGraphTraversal", s.specRPCGraphTraversal)
	s.T().Run("RandomWalk", s.specRandomWalk)
	s.T().Run("Traces", s.specTraces)
}

// saaSpecContextBudget is TestSpec's overall context deadline. DefaultTimeout already reflects
// TEMPORAL_TEST_TIMEOUT, so take the larger of it and a floor generous enough for the combined
// explorers at their default depths.
func saaSpecContextBudget() time.Duration {
	const floor = 8 * time.Minute
	if d := testcontext.DefaultTimeout(); d > floor {
		return d
	}
	return floor
}

// specRPCGraphTraversal walks the transition graph that saaspec.Model() describes and verifies every
// edge against a real onebox server. From each reachable state it tries every event, replays the path
// from a fresh activity, drives the event as a real RPC, reads the internal state back with
// ReadComponent, and asserts:
//   - the resulting internal state equals Model().Next exactly (all fields);
//   - each stamp's change across the edge matches Model()'s AttemptTasksInvalidated / ScheduleToCloseTaskInvalidated;
//   - the RPC's accept/reject outcome matches Model().Reject;
//   - for a heartbeat, the response flags equal ExpectedHeartbeatFlags.
//
// Model() is total over the RPC event alphabet; a cell it does not handle panics and fails the run.
// Timeouts are configured long (hours) so none fires mid-scenario; retry backoff is short so retries
// can be traversed. Timeout timing is checked by the traces, not here.
func (s *standaloneActivityTestSuite) specRPCGraphTraversal(t *testing.T) {
	env := s.newTestEnv()
	// testcontext.For(t), not s.Context(): the suite context is memoized once per suite test, so all
	// TestSpec subtests would otherwise share a single 90s budget. Anchoring on the subtest t gives
	// each explorer its own budget.
	ctx := testcontext.For(t)

	chasmCtx, err := env.GetTestCluster().Host().ChasmContext(ctx)
	require.NoError(t, err)

	for i, cfg := range saaTraversalConfigs {
		h := &saaHarness{
			env:      env,
			ctx:      ctx,
			chasmCtx: chasmCtx,
			nsID:     env.NamespaceID().String(),
			cfg:      cfg,
			cfgIdx:   i,
		}
		if cfg.HasStartDelay {
			// Keep the first-dispatch window open for the whole traversal so the activity stays
			// StartDelayPending (the model never leaves that state via an RPC event), letting the BFS
			// cross the operator commands with the start-delay window.
			h.startDelay = time.Hour
		}
		h.traverse(t)
	}
}

// specRandomWalk drives one activity forward through randomly chosen events — no replay, no
// backtracking, no state dedup. Where the graph traversal is exhaustive but depth-bounded, this
// reaches deep, long interaction sequences the bounded BFS structurally never visits, at ~one RPC per
// step. Every step is checked against Model() (via the same apply()), so a divergence is caught the
// same way; the walk is deterministic in its seed (logged), so any failure reproduces exactly with
// TEMPORAL_SAASPEC_WALK_SEED. Deep runs need a raised budget: TEMPORAL_TEST_TIMEOUT and the go test
// -timeout. Set TEMPORAL_SAASPEC_NO_NEGATIVE_POLL=1 to skip the ~3s Paused negative poll.
func (s *standaloneActivityTestSuite) specRandomWalk(t *testing.T) {
	env := s.newTestEnv()
	ctx := testcontext.For(t) // subtest-scoped budget; see specRPCGraphTraversal

	chasmCtx, err := env.GetTestCluster().Host().ChasmContext(ctx)
	require.NoError(t, err)

	seed, steps := saaWalkSeed(), saaWalkSteps()
	t.Logf("random walk: seed=%d steps=%d/cfg (override TEMPORAL_SAASPEC_WALK_SEED / _WALK_STEPS)", seed, steps)

	for i, cfg := range saaTraversalConfigs {
		h := &saaHarness{
			env: env, ctx: ctx, chasmCtx: chasmCtx, nsID: env.NamespaceID().String(),
			cfg: cfg, cfgIdx: i,
		}
		if cfg.HasStartDelay {
			// Keep the first-dispatch window open for the whole walk so the activity stays
			// StartDelayPending (no RPC event leaves it); the walk explores operator commands in the
			// window and, unlike the BFS, re-polls post-operation states (catching early re-dispatch).
			h.startDelay = time.Hour
		}
		// Independent, reproducible RNG stream per config.
		h.randomWalk(t, rand.New(rand.NewSource(seed+int64(i))), steps)
	}
}

// specTraces runs the traces: each reaches its scenario on one fresh activity, paying every real
// wall-clock wait (a start-delay/backoff window or a firing timeout) exactly once.
func (s *standaloneActivityTestSuite) specTraces(t *testing.T) {
	env := s.newTestEnv()

	for i, tr := range saaTraces {
		t.Run(tr.name, func(t *testing.T) {
			// A fresh context per trace, each with its own 90s deadline, so the multi-second waits do
			// not accumulate against a single shared budget. testcontext.For(t) anchors on the subtest;
			// s.Context() is memoized once per suite test and would be shared across every trace.
			ctx := testcontext.For(t)
			chasmCtx, err := env.GetTestCluster().Host().ChasmContext(ctx)
			require.NoError(t, err)
			h := &saaHarness{
				env: env, ctx: ctx, chasmCtx: chasmCtx, nsID: env.NamespaceID().String(),
				cfg: tr.config(), cfgIdx: i,
				startDelay: tr.startDelay, retryInterval: tr.retryInterval, nextRetryDelay: tr.nextRetryDelay,
				// A timeout's *Elapses event in the script is the signal to configure that timeout short.
				shortTimeout: saaTimeoutIn(tr.trace),
				// "Dispatchable" must mean "dispatches promptly", so bound the positive poll below the
				// delay window — that is how reset-discards (immediate) is told from still-delayed.
				positivePollTimeout: saaNegativePollTimeout,
			}
			h.driveTrace(t, tr.trace)
		})
	}
}
