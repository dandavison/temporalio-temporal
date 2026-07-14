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

## RPC graph traversal (onebox)

Breadth-first walk of the model's reachable states (deduped by fingerprint, depth-bounded), driving
every decided edge against the server and checking the internal state, reject kind, heartbeat flags,
Describe projection, and poll attempt.

```bash
go test -tags test_dep -run 'TestStandaloneActivityTestSuite/TestSpecRPCGraphTraversal' -count=1 -v ./tests/
```

Tunable via env vars:
- `TEMPORAL_SAASPEC_MAX_DEPTH=N` — raise the BFS depth cap (default 4) for deeper local runs.
- `TEMPORAL_SAASPEC_NO_NEGATIVE_POLL=1` — skip the ~3s "a PAUSED activity must not dispatch" long
  poll (the dominant cost of deep walks); the per-edge state check still runs.
- `TEMPORAL_SAASPEC_COMPLETENESS=1` — also print the reachable-but-unexercised cells (informational).

```bash
TEMPORAL_SAASPEC_MAX_DEPTH=6 TEMPORAL_SAASPEC_COMPLETENESS=1 go test -tags test_dep -run 'TestStandaloneActivityTestSuite/TestSpecRPCGraphTraversal' -count=1 -v ./tests/
```

## Directed traces (onebox, real timers)

Where the RPC graph traversal is exhaustive, the wall-clock behavior is checked with directed
**traces**: a trace is a list of events run once on a single activity, and at every step the harness
drives the event and checks the state against `Model` — same driver (`driveTrace`) for both groups
below. RPC/poll events fire synchronously; a wall-clock event (a timeout or a dispatch-delay clock)
is realized by configuring it short and waiting. Outcomes are never hard-coded — they come from
`Model`. The two groups differ only in how the result is observed.

### Timeout traces — observed via ReadComponent

Cover the four activity timeout types (schedule-to-start, schedule-to-close, start-to-close,
heartbeat). A timeout firing changes the status, so the trace reaches a source state via RPCs, fires
one timeout, and reads the resulting state.

```bash
go test -tags test_dep -run 'TestStandaloneActivityTestSuite/TestSpecTimeoutPaths/$scenario' -count=1 -v ./tests/
```

Scenarios (drop `/$scenario` to run all): `schedule-to-close/elapses-while-paused`, `schedule-to-start/elapses-while-scheduled`, `schedule-to-start/elapses-while-paused`,
`start-to-close/elapses-while-started/retries-remain`, `start-to-close/elapses-while-started/last-attempt`, `heartbeat/elapses-while-started/retries-remain`,
`heartbeat/elapses-while-started/last-attempt`, `schedule-to-start/elapses-within-start-delay`,
`schedule-to-close/elapses-within-start-delay`.

### Dispatch-delay traces — observed by polling

A `start_delay` (first attempt) or a retry backoff (policy-derived, or a worker `next_retry_delay`
override) holds a SCHEDULED activity non-dispatchable until a wall-clock instant. That is latent —
the status stays SCHEDULED, so `ReadComponent` can't see it (the model tracks it as the `Dispatch`
field, excluded from the state oracle); the trace observes it by **polling**: no task while the
dispatch is delayed, a task once the matching `*Elapses` event (driven by waiting) fires.

```bash
go test -tags test_dep -run 'TestStandaloneActivityTestSuite/TestSpecDispatchDelayPaths/$scenario' -count=1 -v ./tests/
```

Scenarios (drop `/$scenario` to run all): `start-delay/first-dispatch`,
`start-delay/pause-then-unpause`, `start-delay/reset`, `backoff/retry-dispatch`,
`backoff/next-retry-delay-override`, `backoff/pause-then-unpause`, `backoff/reset`.

## Random walk (onebox, forward-only exploration)

Where the RPC graph traversal is exhaustive but depth-bounded (it replays every path from a fresh
activity, so cost caps the depth), the random walk drives **one activity forward** through randomly
chosen events — no replay, no backtracking, no dedup — checking every step against `Model` via the
same `apply`. It reaches deep, long interaction sequences the bounded traversal never visits, at
~one RPC per step, trading exhaustive coverage for depth: no completeness guarantee, but it wanders
far, and it is how the deep bugs get found. The walk is deterministic in its seed (logged), so any
failure replays exactly, and a divergence prints the event path that produced it.

```bash
TEMPORAL_TEST_TIMEOUT=12m TEMPORAL_SAASPEC_NO_NEGATIVE_POLL=1 TEMPORAL_SAASPEC_WALK_STEPS=2000 \
  go test -tags test_dep -timeout 14m -count=1 -v \
  -run 'TestStandaloneActivityTestSuite/TestSpecRandomWalk' ./tests/
```

Env vars:
- `TEMPORAL_SAASPEC_WALK_STEPS=N` — steps per config (default 200, sized to fit a bare 90s run).
- `TEMPORAL_SAASPEC_WALK_SEED=N` — RNG seed (default 1); set it to a failure's logged seed to reproduce.
- `TEMPORAL_SAASPEC_VERBOSE=1` — log every step as `FromStatus --Event--> ToStatus` (needs `-v`).
- `TEMPORAL_TEST_TIMEOUT` (with a matching `go test -timeout`) — the per-test context defaults to 90s;
  raise both for any run past a few hundred steps, else the walk fails on context exhaustion rather
  than a real bug.
- `TEMPORAL_SAASPEC_NO_NEGATIVE_POLL=1` — skip the ~3s Paused negative poll; recommended for long walks.

## Known gaps

Deliberately fails, listing verification work and spec decisions not yet resolved.

```bash
go test -tags test_dep -run 'TestStandaloneActivityTestSuite/TestSpecKnownGaps' -count=1 -v ./tests/
```

## All the spec tests at once, no other suites

`TestSpec` matches the graph traversal, timeout traces, dispatch-delay traces, and known-gaps. (Known-gaps
fails by design.)

```bash
go test -tags test_dep -run 'TestStandaloneActivityTestSuite/TestSpec' -count=1 -v ./tests/
```
