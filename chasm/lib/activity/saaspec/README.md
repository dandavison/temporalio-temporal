# saaspec — model-based tests for standalone activities

`saaspec` is the human-authored spec of intended SAA behavior. The harness that checks it against a
real onebox server lives in `tests/` (`activity_standalone_spec_*_test.go`). Design and status:
`docs/development/saa-model-tests-plan.md`.

Run the tests (`-count=1` skips the test cache; `-v` shows the per-cell logs):

```bash
# No server, ~1s — spec smoke tests + static Model↔code checks:
go test ./chasm/lib/activity/saaspec/...
#   model_test.go                              — Model/Initial unit assertions
#   conformance.TestModelDecisionCoverage      — which (status,event) cells are decided vs TODO(spec)
#   conformance.TestModelEdgesReachableInCode  — every model edge is reachable in the code's transitions

# Explorer (onebox) — drives every decided edge against the server and checks state, reject kind,
# heartbeat flags, Describe projection, and poll attempt; then a completeness check for
# reachable-but-unexercised cells. Expected red today (WIP model + completeness gaps).
go test -tags test_dep -run 'TestStandaloneActivityTestSuite/TestSpecExplorer' -count=1 -v ./tests/

# Explorer focused on event kind(s) (SAASPEC_EVENT, comma-separated, case-insensitive) — reports only
# those events; the full graph is still traversed. Poll,RespondCompleted is the happy path (green).
SAASPEC_EVENT=Reset,Pause go test -tags test_dep -run 'TestStandaloneActivityTestSuite/TestSpecExplorer' -count=1 -v ./tests/

# Timer probes (onebox, real timers) — shrink one timeout, wait past it, check state vs Model.
# Each subtest SKIPs until its timer is decided in Model.
go test -tags test_dep -run 'TestStandaloneActivityTestSuite/TestSpecTimerProbes' -count=1 -v ./tests/
# one scenario (slash-hierarchy): also /STC  /S2S  /startToClose  /heartbeat/started-retry ...
go test -tags test_dep -run 'TestStandaloneActivityTestSuite/TestSpecTimerProbes/heartbeat' -count=1 -v ./tests/

# Known gaps — deliberately fails, listing verification work not yet implemented.
go test -tags test_dep -run 'TestStandaloneActivityTestSuite/TestSpecKnownGaps' -count=1 -v ./tests/

# All three spec tests at once, no other suites:
go test -tags test_dep -run 'TestStandaloneActivityTestSuite/TestSpec' -count=1 -v ./tests/
```
