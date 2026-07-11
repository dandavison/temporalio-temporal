# Verifying the SAA operator commands (pause / unpause / reset / update-options)

## 1. Goal and constraints

We are adding Pause, Unpause, Reset, and UpdateActivityExecutionOptions to standalone
activities. The state machine grew from 9 to 12 statuses plus several deferred-action flags, and
the intended behavior has been in flux. We want to **augment the existing functional tests with
something much closer to exhaustive state-space exploration**, and to pin the intended behavior in
a **Go-authored spec** (no natural-language spec, no LLM authorship of the spec).

Hard constraints (decided):

- **100% real Temporal.** We run against the real `testcore` onebox: real history/matching/frontend,
  real CHASM engine, real task executors, real wall-clock timers. No fake clock, no fake task
  machinery — controlling the clock is a separate project and risks artefactual results.
- **The spec is Go code, written by you.** It is only used to check the implementation and is never
  part of the code under test.

Validated substrate (spike, `tests/activity_standalone_spike_test.go`):

- Full internal `ActivityState` is readable from a onebox test via
  `Host().ChasmContext(ctx)` + `chasm.ReadComponent` — status, `count`, `stamp`,
  `schedule_to_close_stamp`, the reset flags, `first_attempt_started_time`, `dispatch_time`. No debug
  endpoint needed; works on the shared cluster (single history host).
- Cost is **~3 ms per (Start + read) cycle**, so "replay each path from a fresh activity" is cheap
  enough for a search of thousands of paths (tens of seconds to a couple of minutes).

## 2. The core idea

Two independent questions, kept separate:

- **"What should happen?"** → your `Model()`, a pure Go function.
- **"What does the code do?"** → the real onebox server, observed via `ReadComponent`.

They are connected by comparing them step by step: after each event we drive on the real server, we
read the internal state and assert it exactly equals what `Model()` predicts. This comparison is
only well-defined if each event is processed on its own — one event, one observation, with no other
transition happening in between. That holds only when no timer fires between the event and the
observation, so we separate the transitions into two groups.

### Two groups of transitions

- **Transitions driven by an RPC** — start, poll→started, heartbeat, complete, fail, cancel,
  terminate, pause, unpause, reset (and its variants), update-options. Each is a call we make and
  observe immediately after. All the operator-command behavior we most need to check is here:
  deferred flags, stamp handling, option restore, the reset variants.
- **Transitions driven by a timer** — the four timeouts (schedule-to-start, schedule-to-close,
  start-to-close, heartbeat) and the delayed dispatch caused by a start delay or a retry backoff.
  These require real time to pass before they occur.

During the RPC-driven exploration we set the four timeout durations to hours, so none of those
timers fires while a scenario runs. Traversing a retry does require the backoff timer to fire, so we
use a short retry backoff and the explorer waits for the next attempt to be dispatched before it
polls; the start delay is set to zero during exploration. With this configuration the activity
changes state only in response to the RPCs we send (plus the short backoff we wait for), so each
observation reflects one event and the step-by-step comparison stays exact, without controlling the
clock. The timing of the four timeouts, of the start delay, and of longer backoffs is checked
separately by tests that use short durations and assert the resulting behavior — for example,
whether a paused activity still times out on schedule-to-close.

> Note: the stale-*dispatch* no-op case (pause bumps the stamp, the old dispatch task must not run)
> *is* covered in the RPC subgraph, because "did a dispatch happen?" is observable synchronously via
> a Poll that returns nothing. Only the four *timeout* tasks need the timer tests.

## 3. Architecture

```
        YOU (the spec)                         ME (the machinery)
   ┌───────────────────────┐          ┌──────────────────────────────────┐
   │ Model(cfg,state,event)│──used by─▶│ (b) RPC-subgraph replay-explorer │──drives──▶ onebox
   │   -> Outcome          │          │     - BFS over RPC event seqs     │◀─reads──── (ReadComponent)
   │ abstract(observed)    │──used by─▶│     - assert observed == Model    │
   │   -> AbstractState     │          │     - dedup frontier, coverage    │
   │ Initial(cfg)          │          └──────────────────────────────────┘
   └───────────┬───────────┘          ┌──────────────────────────────────┐
               └──────────────used by─▶│ (a) Model↔code static diff       │  (no server)
                                       │     - enumerate Model's relation  │
                                       │     - vs chasm.NewTransition sets │
                                       └──────────────────────────────────┘
                                       ┌──────────────────────────────────┐
                                       │ (c) time-driven edge tests        │──drives──▶ onebox
                                       │     - short timeouts, behavioral  │   (real timers)
                                       └──────────────────────────────────┘
```

Ownership:

| Part | Who | Needs server? | Produces |
| --- | --- | --- | --- |
| `Model()`, `abstract()`, `Initial()` | **you** | no | the spec |
| (a) Model↔code static diff | me | no | gaps: model edges w/o code path & vice-versa |
| (b) RPC-subgraph replay-explorer | me | yes | exact-equality violations + coverage ledger |
| (c) time-driven edge tests | me | yes | timer-interaction findings (incl. pause/STC) |

## 4. Your part — with a worked example to get started

You write three things in one small Go package (e.g. `chasm/lib/activity/saaspec/`). Nothing here
imports the server internals except the status enum; the spec stays readable and self-contained.

**Key design point that makes the oracle exact:** every field of `AbstractState` is *deterministic
across replay-from-fresh* (stamps/counts/flags never depend on wall-clock time or run IDs) and
*observable* via `ReadComponent`. So `Model()` predicts the exact tuple and the explorer checks exact
equality — a missed stamp bump or a leaked flag is caught precisely, not approximately. (Time-valued
fields like `dispatch_time` are projected to a boolean `DispatchTimeSet`, which is deterministic.)

```go
package saaspec

// Status mirrors activitypb.ActivityExecutionStatus but is defined locally so the
// spec reads independently of the proto. abstract() maps the observed enum onto it.
type Status int

const (
	Unspecified Status = iota
	Scheduled
	Started
	CancelRequested
	Completed
	Failed
	Canceled
	Terminated
	TimedOut
	Paused
	PauseRequested
	ResetRequested
)

func (s Status) terminal() bool {
	switch s {
	case Completed, Failed, Canceled, Terminated, TimedOut:
		return true
	}
	return false
}

// AbstractState is the EXACT projection of observable internal state the spec predicts.
type AbstractState struct {
	Status              Status
	Count               int32 // attempt.count
	Stamp               int32 // attempt.stamp
	STCStamp            int32 // schedule_to_close_stamp
	ResetKeepPaused     bool
	ResetHeartbeats     bool
	ResetRestoreOptions bool
	FirstAttemptStarted bool
	DispatchTimeSet     bool
}

// Config captures the start-time options that change transition behavior. The explorer
// runs the full search once per template (see §6).
type Config struct {
	HasScheduleToClose bool
	HasScheduleToStart bool
	HasHeartbeat       bool
	HasStartDelay      bool
	MaxAttempts        int32 // 0 = unlimited
}

type EventKind int

const (
	Poll EventKind = iota // worker poll that transitions Scheduled -> Started
	Heartbeat
	RespondCompleted
	RespondFailed
	RespondCanceled
	RequestCancel
	Terminate
	Pause
	Unpause
	Reset
	UpdateOptions
)

// Event carries the variant flags that affect the outcome. Leave irrelevant flags zero.
type Event struct {
	Kind            EventKind
	Retryable       bool // RespondFailed: is the failure retryable AND retries remain per policy
	KeepPaused      bool // Reset
	RestoreOriginal bool // Reset / UpdateOptions
	ResetHeartbeat  bool // Reset / Unpause
	ResetAttempts   bool // Unpause
	SameRequestID   bool // Pause/Terminate/Cancel: repeat of the previous op's request id
}

type ErrorKind int

const (
	NoError ErrorKind = iota
	FailedPrecondition
	NotFound
	InvalidArgument
)

// Outcome is what the spec says the API + resulting state should be. For a rejected or
// no-op call, Next == the input state (the call must not mutate).
type Outcome struct {
	Next   AbstractState
	Reject ErrorKind
}

// Initial is the state immediately after a successful StartActivityExecution.
func Initial(cfg Config) AbstractState {
	s := AbstractState{Status: Scheduled, Count: 1, Stamp: 1, DispatchTimeSet: true}
	if cfg.HasScheduleToClose {
		s.STCStamp = 1 // TransitionScheduled bumps schedule_to_close_stamp when STC is set
	}
	return s
}

// Model is TOTAL: every (status, event) must be handled. Making it total is the point —
// it forces every open corner of the spec to be resolved. Where a (status, event) truly
// cannot be reached, handle it explicitly (panic("unreachable: ...")) so the static diff
// in part (a) can confirm the code agrees it is unreachable.
func Model(cfg Config, s AbstractState, e Event) Outcome {
	noop := Outcome{Next: s, Reject: NoError}
	reject := func(k ErrorKind) Outcome { return Outcome{Next: s, Reject: k} }

	switch e.Kind {
	case Poll:
		if s.Status == Scheduled {
			n := s
			n.Status = Started
			n.FirstAttemptStarted = true
			// no stamp bump: Started keeps the attempt's dispatch stamp
			return Outcome{Next: n, Reject: NoError}
		}
		// A poll in any other status finds no dispatchable task -> no state change.
		return noop

	case Pause:
		switch s.Status {
		case Scheduled:
			n := s
			n.Status = Paused
			n.Stamp++ // must invalidate the pending dispatch task
			return Outcome{Next: n}
		case Started:
			n := s
			n.Status = PauseRequested // worker still in charge; NO stamp bump
			return Outcome{Next: n}
		case Paused, PauseRequested:
			if e.SameRequestID {
				return noop // idempotent
			}
			return reject(FailedPrecondition) // "already paused"
		case CancelRequested:
			return reject(FailedPrecondition) // cancel takes precedence
		case ResetRequested:
			// <<< DECISION POINT: is Pause during RESET_REQUESTED a reject, a no-op, or
			// does it depend on ResetKeepPaused? Resolve here; this is where writing the
			// model flushes out the ambiguity. >>>
			panic("SPEC GAP: Pause during ResetRequested — decide")
		}
		if s.Status.terminal() {
			return reject(FailedPrecondition)
		}
		panic("SPEC GAP: Pause from status " + itoa(s.Status))

	// ... RespondFailed, Reset (×keepPaused/restoreOriginal), Unpause, UpdateOptions, etc.

	default:
		panic("unhandled event")
	}
}
```

And the projection from what the explorer reads (the spike already reads all of these):

```go
// abstract maps the observed internal snapshot onto the spec's AbstractState.
func abstract(o Observed) AbstractState {
	return AbstractState{
		Status:              mapStatus(o.Status), // internal enum -> saaspec.Status
		Count:               o.Count,
		Stamp:               o.Stamp,
		STCStamp:            o.ScheduleToCloseStamp,
		ResetKeepPaused:     o.ResetKeepPaused,
		ResetHeartbeats:     o.ResetHeartbeats,
		ResetRestoreOptions: o.ResetRestoreOptions,
		FirstAttemptStarted: o.FirstAttemptStarted,
		DispatchTimeSet:     o.DispatchTimeSet,
	}
}
```

**How to get started (suggested order):**

1. Copy the block above into `saaspec/model.go` and make it compile.
2. Fill in `Model()` status-by-status for one event at a time. Start with `Pause`/`Unpause`
   (fewest variants), then `Reset` (the hard one — `keepPaused` × `restoreOriginal` ×
   running-vs-not), then `UpdateOptions`, then the worker events (`RespondFailed` depends on both
   `Retryable` and the current status — PAUSE_REQUESTED→PAUSED, RESET_REQUESTED→SCHEDULED/PAUSED,
   else STARTED→SCHEDULED).
3. Every time you hit a `panic("SPEC GAP: …")`, that is a finding: the intended behavior was never
   decided. Decide it, encode it, and note it. These are the highest-value outputs and they
   appear before any test runs.

Don't consult the implementation while writing the model — write what *should* happen. Divergences
between your model and the code are exactly what parts (a) and (b) exist to surface.

## 5. My parts

**(a) Model↔code static diff** (no server; delivered first). Enumerate `Model()` over its finite
domain (12 statuses × the bounded event alphabet × a few configs) and cross-check against the
`chasm.NewTransition(...)` source-status sets declared in `statemachine.go`:

- every declared transition source-status has a corresponding model edge, and
- every non-reject model edge corresponds to a declared code transition (or an in-handler helper).

Divergences (e.g. a status the code accepts but the model rejects, or vice-versa) are findings, with
zero infrastructure.

**(b) RPC-subgraph replay-explorer** (onebox). BFS/DFS over sequences of RPC events with long
(inert) timeouts:

- For each frontier path, replay from a fresh activity (~3 ms/step), apply the next event, read the
  internal state, and assert `abstract(observed) == Model(cfg, abstract(prev), event).Next` and the
  returned error kind matches `Outcome.Reject`.
- Dedup the frontier on a **coarse fingerprint** (statuses exact; `Count` capped at e.g. 3+; `Stamp`
  dropped from the fingerprint since behavior never branches on its absolute value — it is still
  checked exactly on every edge). This keeps the frontier finite despite retry loops while losing no
  checking precision.
- Emit a **coverage ledger**: which `(abstract-state, event)` pairs and `(abstract-state, event,
  next)` triples were exercised, and which model-reachable ones were not (→ directed cases).
- Besides the persisted state, assert the values an RPC returns that are functions of state but are
  not part of the persisted snapshot (see `saaspec/responses.go`): the heartbeat-response flags
  (against `ExpectedHeartbeatFlags`), the public status and run state from `DescribeActivityExecution`
  (against `ExpectedDescribe`), and the attempt number a poll returns to the worker (equal to
  `Count`). For some internal statuses these are the only external evidence — for `RESET_REQUESTED`
  the `ActivityReset` heartbeat flag is the sole observable, since Describe reports it as STARTED.
  Exact option values from update-options are not asserted here; see §8.

**(c) Time-driven edge tests** (onebox, short timeouts, behavioral). One focused test per timer
interaction the RPC subgraph can't reach, including the ones flagged in review:

- schedule-to-close still fires while PAUSED, and while repeated pause, unpause, and reset calls are
  made (the suspected divergence from the legacy `LoadAndSortActivityTimers` behavior);
- backoff actually re-dispatches; start-delay defers first dispatch but not start-to-close;
- stale timeout tasks no-op after a stamp bump (schedule + short STS, then pause, confirm no timeout).

## 6. Event alphabet and config templates

**Event alphabet** = event kinds × their relevant variant flags, bounded:

- worker: Poll, Heartbeat, RespondCompleted, RespondFailed{retryable, non-retryable}, RespondCanceled
- operator: RequestCancel, Terminate, Pause, Unpause{resetAttempts, resetHeartbeat},
  Reset{keepPaused, restoreOriginal, resetHeartbeat} (the 2³ combos), UpdateOptions{a few masks,
  restoreOriginal}
- idempotency: a "same request id as previous" variant for Pause / Terminate / Cancel

**Config templates** (full search runs once per template — this is what surfaces stamp/STC bugs):

| Template | STC | STS | Heartbeat | StartDelay | MaxAttempts |
| --- | --- | --- | --- | --- | --- |
| minimal | – | – | – | – | 0 |
| stc | ✓ | – | – | – | 3 |
| full | ✓ | ✓ | ✓ | ✓ | 3 |

## 7. Milestones

1. **Substrate spike** — done (observability + cost validated).
2. **(a) static-diff harness** + **you: first pass of `Model()`** (in parallel; both need no server).
   First findings: spec gaps (from writing the model) and model/code structural divergences.
3. **(b) explorer** on the `minimal` template, then `stc`, then `full`. Findings: exact-equality
   violations, with a minimal reproducing event trace each.
4. **(c) timer tests**. Findings: timer-interaction bugs.
5. **Triage + decide.** For each finding, decide whether the code or the spec is wrong; fix code or
   amend `Model()`. Promote the most interesting discovered traces into permanent regression tests.

## 8. Exactly what coverage this achieves

**What is verified, and how strongly:**

- **Exact, exhaustive checking of the RPC-driven transition graph.** Every operator/worker-command
  transition reachable from a fresh activity — up to the coarse dedup abstraction (statuses exact,
  attempt-count bucketed) — is exercised at least once against the **real server**, and at each step
  the **entire observable internal state** (status, attempt count, both stamps, all reset flags,
  first-started, dispatch-set) is checked for **exact equality** with your Go spec, including the
  returned error/no-op/idempotency behavior. The bug classes we most care about show up here (a
  missed or extra stamp bump, a deferred flag left set or cleared too early, an option restore
  overwriting a concurrent update, a reset landing in the wrong status), and each is caught by the
  exact state comparison.
- **Structural consistency of spec vs code.** The `Model()` transition relation is proven to agree
  with the code's declared `NewTransition` source-status sets — no transition the code allows is
  missing from the spec, and none the spec allows is missing from the code.
- **Directed coverage of the time-driven edges** — the four timeouts and delayed dispatch/backoff —
  against **real wall-clock timers**, including the pause/schedule-to-close interaction and
  stale-task no-op.
- **A coverage ledger** stating precisely which `(state, event)` pairs and `(state, event, next)`
  triples were and were not exercised — so we can state exactly what "near-exhaustive" covered
  rather than assume it.
- **State-derived RPC response values.** Beyond the persisted state, the checks include the
  heartbeat-response flags, the public status and run state reported by Describe, and the attempt
  number returned to a polling worker — each predicted from the abstract state (see
  `saaspec/responses.go`). For `RESET_REQUESTED` the heartbeat `ActivityReset` flag is the only
  external evidence, since Describe reports that status as STARTED.

**What is *not* covered (deliberately, and why):**

- **Exact option values from update-options.** The merged and normalized timeout, retry-policy,
  priority, task-queue, and start-delay values that `UpdateActivityExecutionOptions` produces are not
  compared by the explorer. Tracking them would grow the abstract state from a small scalar tuple
  into the full options struct and pull the merge/normalize logic into the spec. That correctness is
  checked by dedicated update-options tests comparing the response and the Describe output field by
  field.

- **Exhaustive event/timer interleavings.** With real wall-clock time we cannot inject "fire this
  specific timer now," so timer edges are covered by directed tests, not by exhaustive interleaving.
  (Exhaustive interleaving would require the clock-control project we chose to avoid.)
- **Cross-component / cross-shard / replication concurrency.** The explorer is sequential. This is
  sound for *per-execution* semantics because the CHASM execution lock serializes all mutations of a
  single activity — there are no intra-execution interleavings to miss. Multi-execution and
  standby/replication behavior are out of scope.
- **Matching / persistence internals and full frontend validation.** We exercise the frontend
  handlers via the RPCs we send, but the explorer targets the state machine, not matching dispatch,
  persistence races, or every frontend validation branch (those keep their existing tests).

**One-line summary of the guarantee:** *Every reachable operator/worker-command transition of the
SAA state machine is checked, exactly, against a Go-authored spec on the real server; the timer-driven
transitions are covered by directed real-timer tests; and the spec is proven structurally consistent
with the code's transition declarations.*
