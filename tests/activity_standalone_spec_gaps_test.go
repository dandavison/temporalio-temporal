package tests

// TestSpecKnownGaps fails on purpose.
func (s *standaloneActivityTestSuite) TestSpecKnownGaps() {
	gaps := []string{
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
