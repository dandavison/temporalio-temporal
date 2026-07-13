package tests

// TestSpecKnownGaps fails on purpose. It enumerates verification work that is understood but not
// yet implemented, so it cannot be silently forgotten (a t.Skip would be forgettable). Remove each
// line as it is implemented; delete the test when the list is empty.
//
// These are the items that the model-driven harness (static check + graph traversal + timeout traces) does
// not cover, either because they are about timing precision (when something happens, not what state
// results) or because they need a scenario the harness does not construct.

func (s *standaloneActivityTestSuite) TestSpecKnownGaps() {
	gaps := []string{
		"spec decision (req 1): the impl measures schedule-to-close from first-dispatch-time " +
			"(ScheduleTime + start_delay), so a start_delay pushes the schedule-to-close deadline back, " +
			"like it does schedule-to-start. The model (scheduleToCloseFires) currently encodes the " +
			"opposite — that schedule-to-close runs from schedule and fires during the start delay. " +
			"Decide which is intended, then update the model to match and add the timeout trace " +
			"(TestSpecDispatchDelayPaths / TestSpecTimeoutPaths) that exercises it against the server.",
		"pure-timing: start-to-close is measured from STARTED, not from schedule, so a start_delay " +
			"must not eat into the running attempt's start-to-close budget (needs short start_delay + " +
			"short start-to-close: poll after the delay, then assert the attempt times out one " +
			"start-to-close AFTER it started).",
		"stale-token: a worker RPC (Complete/Fail/Cancel/Heartbeat) against a never-polled " +
			"SCHEDULED/PAUSED activity returns NotFound. The graph traversal verifies the token-validation " +
			"reject whenever a stale token exists (via a retry path); the never-polled variant is " +
			"reported by the graph traversal's coverage ledger and is not otherwise exercised.",
		"config coverage: broaden the Reset (keepPaused x restoreOriginal x resetHeartbeat) and " +
			"Unpause (resetHeartbeat) flag combinations the graph traversal crosses",
	}
	for _, g := range gaps {
		s.T().Errorf("TODO(saaspec verification): %s", g)
	}
}
