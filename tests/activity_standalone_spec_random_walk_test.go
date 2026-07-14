package tests

// Random-walk explorer for the standalone-activity spec: a forward-only stochastic conformance
// fuzzer. Where TestSpecRPCGraphTraversal is exhaustive but depth-bounded (it replays every path
// from a fresh activity, so cost forces a shallow bound), this drives ONE activity forward through
// randomly chosen events — no replay, no backtracking, no state dedup. So it reaches deep, long
// interaction sequences the bounded BFS structurally never visits, at ~one RPC per step. It trades
// exhaustive coverage for depth: no completeness guarantee, but it wanders far.
//
// Every step is checked against Model() (via the same apply() the traversal uses), so a divergence
// is caught the same way. The whole walk is deterministic in its seed, which is logged, so any
// failure reproduces exactly with TEMPORAL_SAASPEC_WALK_SEED.
//
// Deep runs need a raised budget: TEMPORAL_TEST_TIMEOUT (the per-test context, default 90s) and the
// go test -timeout. Set TEMPORAL_SAASPEC_NO_NEGATIVE_POLL=1 to skip the ~3s Paused negative poll.

import (
	"math/rand"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm/lib/activity/saaspec"
)

func saaWalkSteps() int {
	if v := os.Getenv("TEMPORAL_SAASPEC_WALK_STEPS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return 200 // a bare `go test` (90s per-test context) smoke; raise it with TEMPORAL_TEST_TIMEOUT for real exploration
}

func saaWalkSeed() int64 {
	if v := os.Getenv("TEMPORAL_SAASPEC_WALK_SEED"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
	}
	return 1 // deterministic default; override for a different walk
}

func (s *standaloneActivityTestSuite) TestSpecRandomWalk() {
	env := s.newTestEnv()
	t := s.T()
	ctx := s.Context()

	chasmCtx, err := env.GetTestCluster().Host().ChasmContext(ctx)
	require.NoError(t, err)

	seed, steps := saaWalkSeed(), saaWalkSteps()
	t.Logf("random walk: seed=%d steps=%d/cfg (override TEMPORAL_SAASPEC_WALK_SEED / _WALK_STEPS)", seed, steps)

	for i, cfg := range saaTraversalConfigs {
		h := &saaHarness{
			env: env, ctx: ctx, chasmCtx: chasmCtx, nsID: env.NamespaceID().String(),
			cfg: cfg, cfgIdx: i,
		}
		// Independent, reproducible RNG stream per config.
		h.randomWalk(t, rand.New(rand.NewSource(seed+int64(i))), steps)
	}
}

// randomWalk drives one activity, picking a random applicable event each step and checking it against
// Model(); on reaching a terminal state (or after a divergence) it starts a fresh activity and keeps
// going until the step budget is spent. It reports the distinct states (by fingerprint) it covered.
func (h *saaHarness) randomWalk(t *testing.T, rng *rand.Rand, maxSteps int) {
	seen := map[string]bool{}
	walks := 0

	a, cur := h.walkStart(t)
	var trace []saaspec.Event // events driven since the last (re)start, so a divergence prints its path
	seen[saaFingerprint(cur)] = true
	walks++

	for step := 0; step < maxSteps; step++ {
		if cur.Status.Terminal() {
			a, cur = h.walkStart(t)
			trace = nil
			seen[saaFingerprint(cur)] = true
			walks++
			continue
		}
		e := h.pickWalkEvent(rng, a, cur)
		trace = append(trace, e)
		a.path = trace
		out := saaspec.Model(h.cfg, cur, e)
		switch a.apply(t, e, cur, out, true) {
		case saaVerified:
			cur = out.Next
			seen[saaFingerprint(cur)] = true
		case saaSkippedNoToken:
			trace = trace[:len(trace)-1] // event not driven; drop it from the segment trace
		case saaMismatch:
			// apply() already reported the divergence (with a.path = this trace); restart from a known
			// state so the walk keeps exploring rather than compounding from a suspect one.
			a, cur = h.walkStart(t)
			trace = nil
			walks++
		}
	}
	t.Logf("cfg %d: random walk done — %d steps, %d walks, %d distinct states covered",
		h.cfgIdx, maxSteps, walks, len(seen))
}

// walkStart begins a fresh activity and asserts it matches Initial(cfg).
func (h *saaHarness) walkStart(t *testing.T) (*saaActor, saaspec.AbstractState) {
	a := h.start(t)
	cur := saaspec.Initial(h.cfg)
	obs, err := a.observed()
	require.NoError(t, err)
	if !cur.SameObserved(obs) {
		t.Fatalf("cfg %d: fresh activity disagrees with Initial(cfg)\n%s", h.cfgIdx, saaStateDiff(obs, cur))
	}
	return a, cur
}

// pickWalkEvent chooses the next event. It strongly prefers non-terminal progress so the walk
// wanders deep instead of restarting every few steps (terminal-reaching events like Terminate /
// RespondCompleted end a walk immediately); it still occasionally takes any changing edge (maybe
// terminal) or a reject/no-op so those are exercised in deep contexts too. Terminal edges are
// covered exhaustively by the BFS; here depth is the goal. Events needing a task token we do not
// hold are skipped (they would be un-drivable no-ops).
func (h *saaHarness) pickWalkEvent(rng *rand.Rand, a *saaActor, cur saaspec.AbstractState) saaspec.Event {
	var applicable, changing, deep []saaspec.Event
	for _, e := range saaCandidateEvents() {
		if saaNeedsToken(e.Kind) && a.token == nil {
			continue
		}
		applicable = append(applicable, e)
		if out := saaspec.Model(h.cfg, cur, e); out.Reject == saaspec.NoError && !out.Next.SameObserved(cur) {
			changing = append(changing, e)
			if !out.Next.Status.Terminal() {
				deep = append(deep, e)
			}
		}
	}
	switch {
	case len(deep) > 0 && rng.Float64() < 0.85:
		return deep[rng.Intn(len(deep))]
	case len(changing) > 0 && rng.Float64() < 0.5:
		return changing[rng.Intn(len(changing))]
	case len(applicable) > 0:
		return applicable[rng.Intn(len(applicable))]
	default:
		return saaspec.Event{Kind: saaspec.Poll} // always applicable (needs no token)
	}
}
