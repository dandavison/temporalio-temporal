# saaspec — model-based tests for standalone activities

`saaspec` is the human-authored spec of intended SAA behavior: `Model(cfg, state, event) -> Outcome`
plus the state-derived predictions in `responses.go`. The harness that checks it against a real
onebox server lives in `tests/` (`activity_standalone_spec_*_test.go`) and contains no product logic
of its own — every assertion comes from `Model`.

In every command below, `-count=1` skips the test cache and `-v` shows the per-subtest logs.

## No server (~1s) — spec unit tests + static Model↔code checks

Runs the `model_test.go` assertions (including the dispatch-delay requirements for start_delay
and retry backoff), `conformance.TestModelDecisionCoverage` (`Model` is total over the RPC domain —
no unexpected panics), and `conformance.TestModelEdgesReachableInCode` (every status change the
model accepts is reachable in the code's declared transitions).

```bash
go test ./chasm/lib/activity/saaspec/...
```

## Explorer (onebox) — the RPC transition graph

Breadth-first walk of the model's reachable states (deduped by fingerprint, depth-bounded), driving
every decided edge against the server and checking the internal state, reject kind, heartbeat flags,
Describe projection, and poll attempt.

```bash
go test -tags test_dep -run 'TestStandaloneActivityTestSuite/TestSpecExplorer' -count=1 -v ./tests/
```

Tunable via env vars:
- `SAASPEC_MAX_DEPTH=N` — raise the BFS depth cap (default 4) for deeper local runs.
- `SAASPEC_NO_NEGATIVE_POLL=1` — skip the ~3s "a PAUSED activity must not dispatch" long poll (the
  dominant cost of deep walks); the per-edge state check still runs.
- `SAASPEC_COMPLETENESS=1` — also print the reachable-but-unexercised cells (informational).
- `SAASPEC_EVENT=Reset,Pause` (comma-separated, case-insensitive) — report only on those event
  kinds; the full graph is still traversed to reach every state.

```bash
SAASPEC_MAX_DEPTH=6 SAASPEC_COMPLETENESS=1 go test -tags test_dep -run 'TestStandaloneActivityTestSuite/TestSpecExplorer' -count=1 -v ./tests/
```

## Directed traces (onebox, real timers)

Where the explorer walks the graph exhaustively, the wall-clock behavior is checked with directed
**traces**: a trace is a list of events run once on a single activity, and at every step the harness
drives the event and checks the state against `Model` — same driver (`driveTrace`) for both groups
below. RPC/poll events fire synchronously; a wall-clock event (a timeout or a dispatch-delay clock)
is realized by configuring it short and waiting. Outcomes are never hard-coded — they come from
`Model`. The two groups differ only in how the result is observed.

### Timer probes — observed via ReadComponent

A timeout firing changes the status, so the trace reaches a source state via RPCs, fires one timeout,
and reads the resulting state.

```bash
go test -tags test_dep -run 'TestStandaloneActivityTestSuite/TestSpecTimerProbes' -count=1 -v ./tests/
```

One scenario (slash-hierarchy; e.g. `/STC/paused`, `/S2S/paused-stale`, `/startToClose/started-retry`,
`/heartbeat/started-retry`, `/S2S/pushed-back-by-start-delay`):

```bash
go test -tags test_dep -run 'TestStandaloneActivityTestSuite/TestSpecTimerProbes/heartbeat' -count=1 -v ./tests/
```

### Dispatch-delay traces — observed by polling

A `start_delay` (first attempt) or a retry backoff (policy-derived, or a worker `next_retry_delay`
override) holds a SCHEDULED activity non-dispatchable until a wall-clock instant. That is latent —
the status stays SCHEDULED, so `ReadComponent` can't see it (the model tracks it as the `Dispatch`
field, excluded from the state oracle); the trace observes it by **polling**: no task while the
dispatch is delayed, a task once the matching `*Elapses` event (driven by waiting) fires.

```bash
go test -tags test_dep -run 'TestStandaloneActivityTestSuite/TestSpecDispatchDelay' -count=1 -v ./tests/
```

One scenario (`/start-delay/first-dispatch`, `/start-delay/unpause-keeps-waiting`,
`/start-delay/reset-keeps-waiting`, `/backoff/delayed`, `/backoff/next-retry-delay-override`,
`/backoff/unpause-keeps-waiting`, `/backoff/reset-discards`):

```bash
go test -tags test_dep -run 'TestStandaloneActivityTestSuite/TestSpecDispatchDelay/backoff' -count=1 -v ./tests/
```

## Known gaps

Deliberately fails, listing verification work and spec decisions not yet resolved.

```bash
go test -tags test_dep -run 'TestStandaloneActivityTestSuite/TestSpecKnownGaps' -count=1 -v ./tests/
```

## All the spec tests at once, no other suites

`TestSpec` matches the explorer, timer probes, dispatch-delay traces, and known-gaps. (Known-gaps
fails by design.)

```bash
go test -tags test_dep -run 'TestStandaloneActivityTestSuite/TestSpec' -count=1 -v ./tests/
```
