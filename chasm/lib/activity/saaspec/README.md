# saaspec — model-based tests for standalone activities

`saaspec` is the human-authored spec of intended SAA behavior. The harness that checks it against a
real onebox server lives in `tests/` (`activity_standalone_spec_*_test.go`).

In every command below, `-count=1` skips the test cache and `-v` shows the per-cell logs.

## No server (~1s) — spec smoke tests + static Model↔code checks

Runs the `model_test.go` `Model`/`Initial` unit assertions, `conformance.TestModelDecisionCoverage`
(`Model` is total over the RPC domain — no unexpected panics), and
`conformance.TestModelEdgesReachableInCode` (every model edge is reachable in the code's
transitions).

```bash
go test ./chasm/lib/activity/saaspec/...
```

## Explorer (onebox)

Drives every decided edge against the server and checks state, reject kind, heartbeat flags,
Describe projection, and poll attempt. Expected red today (WIP model).

```bash
go test -tags test_dep -run 'TestStandaloneActivityTestSuite/TestSpecExplorer' -count=1 -v ./tests/
```

Same, plus the type-(A) completeness report (reachable-but-unexercised cells; off by default):

```bash
SAASPEC_COMPLETENESS=1 go test -tags test_dep -run 'TestStandaloneActivityTestSuite/TestSpecExplorer' -count=1 -v ./tests/
```

Focused on event kind(s) via `SAASPEC_EVENT` (comma-separated, case-insensitive) — reports only
those events; the full graph is still traversed. `Poll,RespondCompleted` is the happy path (green):

```bash
SAASPEC_EVENT=Reset,Pause go test -tags test_dep -run 'TestStandaloneActivityTestSuite/TestSpecExplorer' -count=1 -v ./tests/
```

## Timer probes (onebox, real timers)

Shrink one timeout, wait past it, check state vs `Model`. Each subtest SKIPs until its timer is
decided in `Model`.

```bash
go test -tags test_dep -run 'TestStandaloneActivityTestSuite/TestSpecTimerProbes' -count=1 -v ./tests/
```

One scenario (slash-hierarchy; also `/STC`, `/S2S`, `/startToClose`, `/heartbeat/started-retry`, …):

```bash
go test -tags test_dep -run 'TestStandaloneActivityTestSuite/TestSpecTimerProbes/heartbeat' -count=1 -v ./tests/
```

## Dispatch probes (onebox, deferred dispatch)

A deferred dispatch — a `start_delay` before the first attempt, or a retry backoff (policy-derived
or a worker `next_retry_delay` override) before a later attempt — holds a `SCHEDULED` activity
non-dispatchable until a wall-clock instant. That is invisible to the persisted-state oracle (status
stays `SCHEDULED`), so these probes observe it by polling: no task during the window, a task (with
the expected attempt number) once it elapses.

```bash
go test -tags test_dep -run 'TestStandaloneActivityTestSuite/TestSpecDispatchProbes' -count=1 -v ./tests/
```

One scenario (`/startDelay/first-dispatch`, `/startDelay/reset-preserves`, `/backoff/policy`,
`/backoff/next-retry-delay`, `/backoff/reset-discards`):

```bash
go test -tags test_dep -run 'TestStandaloneActivityTestSuite/TestSpecDispatchProbes/backoff' -count=1 -v ./tests/
```

## Known gaps

Deliberately fails, listing verification work not yet implemented.

```bash
go test -tags test_dep -run 'TestStandaloneActivityTestSuite/TestSpecKnownGaps' -count=1 -v ./tests/
```

## All three spec tests at once, no other suites

```bash
go test -tags test_dep -run 'TestStandaloneActivityTestSuite/TestSpec' -count=1 -v ./tests/
```
