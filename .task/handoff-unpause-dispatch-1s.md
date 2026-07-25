# Handoff: why does Unpause take ~1s to dispatch an activity, when a retry backoff takes 4ms?

Written 2026-07-25. Branch `dan/saa-model-tests-w-transitions-conformance-explorer` in
`temporalio/temporal`, worktree
`/Users/dan/worktrees/temporal/dan--saa-model-tests-w-transitions-conformance-explorer/temporal`.

You do not need any of the branch's model/harness context to work on this. Read "The question" and
"Established facts", then go.

## The question

Unpausing a paused standalone activity whose retry backoff has already elapsed takes **~1.000 s** to
get a task to a waiting poller. The same activity dispatching because its retry backoff expired takes
**2–4 ms**. Why, and is the 1 s intended?

Two reasons it matters:

1. It is a user-visible latency on `UnpauseActivityExecution` and `ResetActivityExecution`, ~250× the
   equivalent path. Whether that is acceptable is a product question nobody has asked yet.
2. It makes a committed test marginal. `TestBackoff_Declarative/backoff/pause-after-dispatch-then-unpause`
   polls with a 3 s bound and fails roughly 1 in 4 runs. Do **not** "fix" that by widening the bound
   until the 1 s is understood — that hides this.

## Established facts

Measured on an idle M-series laptop, onebox (`tests/` functional test env).

| dispatch triggered by | latency | n |
|---|---|---|
| retry backoff expiring | 2, 3, 4, 4, 3, 4, 2, 3 ms | 8 |
| `Unpause` | 999 ms – 1.009 ms, mean ~1.002 s | 28 |

The `Unpause` figure is a tight cluster on 1.000 s across 28 runs — a constant, not jitter or load.

Cross-check: `TestBackoff_Declarative/backoff/reset` runs in 1.01 s where sibling traces run in
milliseconds. `Reset` uses the same dispatch path, so it pays the same ~1 s. Consistent.

## Ruled out

- **Machine load.** Reproduces on an idle laptop in a single non-concurrent `go test`. An earlier
  claim in this investigation that it was load-related was wrong and has been withdrawn.
- **`history.timerProcessorMaxPollInterval`** — default 5 min, so not the source of a 1 s delay.
- **A slow tail.** All 28 measurements were ~1 s. See "Still unexplained" for the separate question of
  why the 3 s-bounded test sometimes exceeds 3 s.

## Where the 1 s appears to come from

The dispatch is not an immediate side-effect task. `Unpause` (and `Reset`) add a **timer task due at
`now`**:

- `chasm/lib/activity/activity.go:1027` `(*Activity).unpause` → `ctx.AddTask(a,
  chasm.TaskAttributes{ScheduledTime: dispatchTime}, a.newActivityDispatchTask(ctx))` at
  `activity.go:1050`
- `dispatchTime` comes from `unpauseDispatchTime` (`activity.go:1057`), which for an activity that has
  already started an attempt is just `ctx.Now(a)` — i.e. **now**
- the reset path does the same thing around `chasm/lib/activity/statemachine.go:595-613`

Contrast the retry-backoff case: the dispatch task is added once, at failure time, with
`ScheduledTime` set to the *future* retry instant (`statemachine.go:109-110`). The timer queue has it
loaded well in advance and fires it on time, so the poller finds the task already in Matching → 4 ms.

So the hypothesis is: **a timer task due in the future fires on time; a timer task due "now" waits
~1 s.**

### The prime suspect — `history.timerProcessorMaxTimeShift`, default **1 s**

`common/dynamicconfig/constants.go:2175`. And there is a comment in the scheduled-queue code that
names it as exactly this kind of ~1 s backoff:

`service/history/queues/queue_scheduled.go:260-263`

```go
// NOTE: the backoff is actually TimerProcessorMaxTimeShift = ~1s
// since lookAheadMinTime ~= now + TimerProcessorMaxTimeShift when
// shard is valid.
p.timerGate.Update(lookAheadMinTime)
```

Expected mechanism (unverified): the timer queue's max read level is deliberately held ~1 s in the
future so concurrently-written timer tasks are not missed. A task whose fire time is `now` is
therefore below the read level and is not picked up until the level advances past it — up to
`maxTimeShift` later.

**The decisive experiment:** set `history.timerProcessorMaxTimeShift` to something small (say 100 ms)
in the test cluster's dynamic config override and re-measure. If the ~1 s tracks the setting, the
mechanism is confirmed.

Also worth reading in the same file: `lookAheadTask()` at `queue_scheduled.go:227`, and
`lookAheadRateLimitDelay = 3 * time.Second` at line 40. The 3 s is suspiciously close to the failing
test's 3 s bound; I have not established any connection, and it may be a coincidence.

## Still unexplained

Why the 3 s-bounded test sometimes exceeds 3 s. Every direct measurement was ~1 s, so I never captured
a slow case, and 28/28 at 1 s is statistically hard to square with a ~25 % failure rate at 3 s. Either
there is a second, rarer mechanism, or my measurement differs from the real test in some way I did not
find. Worth resolving, because it is the part that actually breaks the test.

One difference I checked and eliminated: it is the *final* poll that fails, not the first — the failure
report prints
`path: Schedule → Poll → RespondFailed[retryable=true] → BackoffElapses → Pause → Unpause → Poll`.

## Reproducing

Environment for everything: `export TEMPORAL_TEST_LOG_LEVEL=ERROR TEMPORAL_TEST_LOG_STACKTRACE_LEVEL=off`.
The `tests/` package needs `-tags test_dep`.

### The flaky test (~2 of 8)

```bash
for i in $(seq 8); do
  (go test -tags test_dep -count=1 -timeout 8m \
     -run 'TestStandaloneActivityTestSuite/TestBackoff_Declarative/backoff/pause-after-dispatch-then-unpause' \
     ./tests/ > /tmp/flake$i.log 2>&1) &
done; wait
grep -l 'no task was dispatched' /tmp/flake*.log
```

### The latency measurement

Drop this in `tests/zz_latency_test.go`, run it, then delete it. It drives the trace prefix with the
real driver and times only the final poll, with a bound generous enough that the poll always succeeds.

```go
package tests

import (
	"fmt"
	"testing"
	"time"

	"go.temporal.io/server/chasm/lib/activity/model"
)

func (s *standaloneActivityTestSuite) TestZZDispatchLatency() {
	env := s.newTestEnv()

	const pollBound = 30 * time.Second
	arms := map[string][]model.Event{
		"afterUnpause": {saaPoll, saaFailRetryably, saaBackoffDelayElapse, {Kind: model.Pause}, {Kind: model.Unpause}},
		"afterBackoff": {saaPoll, saaFailRetryably, saaBackoffDelayElapse},
	}

	for _, arm := range []string{"afterBackoff", "afterUnpause"} {
		s.T().Run(arm, func(t *testing.T) {
			for i := range 8 {
				t.Run(fmt.Sprintf("run%d", i), func(t *testing.T) {
					h := newSAAHarness(t, env, model.Config{MaxAttempts: 3})
					h.retryInterval = saaDelayWindow
					h.positivePollTimeout = pollBound
					a := h.driveTraceWithModelConformanceChecking(t, arms[arm])

					start := time.Now()
					resp := a.pollForTask(t, pollBound)
					t.Logf("LATENCY %s run%d: %v dispatched=%v", arm, i, time.Since(start).Round(time.Millisecond), resp != nil)
				})
			}
		})
	}
}
```

```bash
go test -tags test_dep -count=1 -timeout 20m -v \
  -run 'TestStandaloneActivityTestSuite/TestZZDispatchLatency' ./tests/ 2>&1 | grep LATENCY
```

Two notes on this harness, so the numbers aren't misread:

- Each arm costs ~6 s per run, almost all of it the 5 s retry backoff the trace configures. The
  interesting number is the logged `LATENCY`, not the subtest duration.
- More than ~14 runs per invocation exceeds the default 90 s per-test context budget and the suite
  will report a context timeout even though every run passed. Raise `TEMPORAL_TEST_TIMEOUT` or keep
  the count low.

## Vocabulary you may need

The trace steps above are events from `chasm/lib/activity/model`, driven by
`tests/activity_standalone_driver.go`. `saaPoll` = `PollActivityTaskQueue`, `saaFailRetryably` =
`RespondActivityTaskFailed` with a retryable application failure, `saaBackoffDelayElapse` = wait for
the retry backoff window to pass, `Pause`/`Unpause` = the operator RPCs. `saaDelayWindow` is 5 s.
`driveTraceWithModelConformanceChecking` drives the sequence and checks each step against the model;
`pollForTask` is the raw long poll.

## Branch state, so you don't trip over it

The branch is green except for three knowingly-red things, none of which you need to fix:

- `TestWFASAATimeoutPreservesUnderlyingFailureCause` — SAA does not chain the underlying application
  failure as the terminal timeout's `Cause`. A real product finding, untriaged.
- `TestWFASAAMetricsParity` — SAA/WFA metric emission and tag divergences. Real, untriaged.
- `backoff/pause-after-dispatch-then-unpause` — the flaky test this document is about.

The harness also has self-tests worth knowing about, since they encode invariants you might otherwise
break: `TestSAADriverReportsUnrealizedWallClockEvents` (a scripted wall-clock event whose effect never
arrives must be reported) and `TestSAAHarnessRejectsInconsistentConfig` (the activity started and the
model config must describe the same activity). Both use a `recordingT` recorder and are paired with
controls.

Fuller background on the branch, if you want it: https://github.com/dandavison/log/issues/272
