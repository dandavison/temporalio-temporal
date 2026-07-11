package tests

// TestSpecKnownGaps fails on purpose. It enumerates verification work that is understood but not
// yet implemented, so it cannot be silently forgotten (a t.Skip would be forgettable). Remove each
// line as it is implemented; delete the test when the list is empty.
//
// These are the items that the model-driven harness (static check + explorer + timer probes) does
// not cover, either because they are about timing precision (when something happens, not what state
// results) or because they need a scenario the explorer does not construct.

func (s *standaloneActivityTestSuite) TestSpecKnownGaps() {
	gaps := []string{
		"pure-timing: a start_delay defers the first dispatch but does NOT extend start-to-close " +
			"(needs a real-time test: short start_delay + short start-to-close, assert start-to-close " +
			"is measured from STARTED, not from schedule)",
		"pure-timing: the first dispatch happens at schedule-time + start_delay and not before " +
			"(poll returns no task before the delay, a task after it)",
		"pure-timing: retry backoff dispatches the next attempt at complete-time + interval and not " +
			"before",
		"stale-token: a worker RPC (Complete/Fail/Cancel/Heartbeat) against a never-polled " +
			"SCHEDULED/PAUSED activity returns NotFound. The explorer verifies the token-validation " +
			"reject whenever a stale token exists (via a retry path); the never-polled variant is " +
			"reported by the explorer's coverage ledger and is not otherwise exercised.",
		"config coverage: the explorer runs 2 templates; add a StartDelay template and the full " +
			"Reset (keepPaused x restoreOriginal x resetHeartbeat) and Unpause (resetHeartbeat) combos",
	}
	for _, g := range gaps {
		s.T().Errorf("TODO(saaspec verification): %s", g)
	}
}
