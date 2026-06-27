# SAA Pause/Reset/Update + StartDelay — review orientation

## 1. The Activity state machine (Sean's substrate + Fred's start_delay)

States: `SCHEDULED · STARTED · CANCEL_REQUESTED · PAUSED · PAUSE_REQUESTED · RESET_REQUESTED`
Terminal: `COMPLETED · FAILED · CANCELED · TERMINATED · TIMED_OUT`

```
                         TransitionScheduled
              UNSPECIFIED ───────────────► SCHEDULED ◄───────────────┐
                                            │   ▲                    │
                          TransitionStarted │   │ TransitionReschedu-│ld (retry, Count++)
                          (worker pickup;   │   │                    │
                          sets FirstAttempt ▼   │                    │
                          StartedTime once) STARTED ─────────────────┘
                                           │
            ┌───────────────┬──────────────┼───────────────┐
            │ pause         │ reset        │ cancel        │ complete/fail/timeout
            ▼ (no stamp     ▼ (no stamp    ▼               ▼
   PAUSE_REQUESTED      RESET_REQUESTED  CANCEL_REQUESTED   COMPLETED/FAILED/TIMED_OUT
   (worker still        (worker still    (worker still
    running attempt N)   running N)       running N)
            │  ▲                │
   unpause  │  │ pause          │ worker yields (fail/timeout, retries left)
            │  │                ├─ ResetKeepPaused ─► PAUSED   (Count=1, backoff recorded, no dispatch)
            ▼  │                └─ else ───────────► SCHEDULED (Count=1, dispatch at retry time)
          STARTED                │
                                 │ worker yields in PAUSE_REQUESTED
   SCHEDULED ──pause(stamp++)──► PAUSED ──unpause(stamp++)──► SCHEDULED
            ◄──reset(stamp++)─── PAUSED   (Count=1, dispatch)
                                 PAUSED ──AttemptFailedWhilePauseRequested?── (that's PAUSE_REQUESTED→PAUSED)
```

The three `*_REQUESTED` states all mean the same thing: **a worker is still executing attempt N under a valid task token; an operator intent (cancel / pause / reset) has been recorded and will be applied when the worker next yields.** They are the crux of the design.

### Why `*_REQUESTED` exist (the key invariant)
A worker's task token authenticates only when `status ∈ {STARTED, CANCEL_REQUESTED, PAUSE_REQUESTED, RESET_REQUESTED}` **and** `token.Attempt == attempt.Count` (`validateActivityTaskToken`). So to record "pause/reset requested" without kicking the running worker off, we need a state that (a) keeps the token valid and (b) does **not** bump `attempt.Stamp` (which would invalidate the in-flight timeout tasks). That's exactly what `TransitionPauseRequested` / `TransitionResetRequested` do — no stamp bump, no token change. The intent is surfaced to the worker via the **heartbeat response flags** `ActivityPaused` / `ActivityReset`.

### Transition cheat-sheet (source → target : effect)
| Transition | src → tgt | stamp bump? | effect |
|---|---|---|---|
| Scheduled | UNSPEC → SCHEDULED | (init) | sets start_delay/S2C/dispatch tasks |
| Started | SCHEDULED → STARTED | no | sets `attempt.StartedTime`; `FirstAttemptStartedTime` **once** |
| Rescheduled | STARTED → SCHEDULED | attempt++ | normal retry: Count++, record backoff, dispatch@retry |
| Paused | SCHEDULED → PAUSED | attempt++ | invalidates pending dispatch |
| PauseRequested | STARTED → PAUSE_REQUESTED | **no** | worker keeps running; flag only |
| Unpaused | PAUSED → SCHEDULED | attempt++ | `unpause()`: clears backoff, dispatch@`respectStartDelay(now)` |
| UnpausedWhilePauseRequested | PAUSE_REQUESTED → STARTED | no | pure no-op (token still valid) |
| AttemptFailedWhilePauseRequested | PAUSE_REQUESTED → PAUSED | attempt++ | record backoff, Count++, **no dispatch** |
| Reset | SCHEDULED/PAUSED → SCHEDULED | attempt++ | `reset()`: Count=1, clears backoff, dispatch@`event.scheduleTime` |
| ResetRequested | STARTED/PAUSE_REQUESTED → RESET_REQUESTED | **no** | worker keeps running; flag only |
| ResetAttemptFailedToScheduled | RESET_REQUESTED → SCHEDULED | attempt++ | Count=1, record backoff, dispatch@retry |
| ResetAttemptFailedToPaused | RESET_REQUESTED → PAUSED | attempt++ | Count=1, record backoff, **no dispatch** |
| CancelRequested | many → CANCEL_REQUESTED | no | SCHEDULED/PAUSED cancel immediately; others wait for worker |
| Canceled/Completed/Failed/Timed/Terminated | → terminal | — | clears `ResetHeartbeats` |

## 2. Timing model (start_delay, backoff, the 4 timeout tasks, the 2 stamps)

Anchors:
- `scheduleTime` = `a.ScheduleTime` — fixed for the run.
- `firstDispatchTime()` = `scheduleTime + start_delay`.
- `respectStartDelay(t)` = `t` if `FirstAttemptStartedTime != nil` (post-pickup no-op); else `max(t, firstDispatchTime())`.
- `scheduleToCloseDeadline()` = `firstDispatchTime() + S2C` (zero if no S2C).

Two independent stamp counters (this trips people up):
- **`attempt.Stamp`** validates the *attempt-scoped* tasks: `ActivityDispatchTask`, `ScheduleToStartTimeoutTask`, `StartToCloseTimeoutTask`, `HeartbeatTimeoutTask`. Bumped on every new attempt **and on every options update** (so in-flight tasks from before the change are discarded).
- **`ScheduleToCloseStamp`** validates only `ScheduleToCloseTimeoutTask`. Bumped at creation and on options update, **never on retry/reset** — S2C spans the whole lifetime.

The four timeout tasks:
| task | fires at | validated by |
|---|---|---|
| ScheduleToClose | `firstDispatchTime + S2C` | `ScheduleToCloseStamp` |
| ScheduleToStart | `dispatchTime + S2S` | `attempt.Stamp` |
| StartToClose | `attempt.StartedTime + StartToClose` | `attempt.Stamp` |
| Heartbeat | `max(lastHb, StartedTime) + hbTimeout` | `attempt.Stamp` |

Fred's two reissue helpers, both called after the `attempt.Stamp++` in `UpdateActivityExecutionOptions`:
- `reissueScheduledDispatch` — re-emits dispatch + ScheduleToStart for a **SCHEDULED** activity (retry time, or `respectStartDelay(now)` for a first attempt still in its delay window).
- `reissueRunningAttemptTimers` — re-emits StartToClose + Heartbeat for the **running set** (STARTED/CANCEL_REQUESTED/PAUSE_REQUESTED/RESET_REQUESTED), so the worker isn't left with no server-side timeout after the stamp bump.

## 3. Scope decision — what to change now

Four departures from the design notes, bucketed by risk:

**Bucket 1 — bugs, safe, do now**
- S2C deadline not recomputed when Reset+RestoreOriginalOptions restores a changed `start_delay` → stale close deadline. Fix in `handleReset`/`reset` (PR #10752).
- `InvalidArgument` → `FailedPrecondition` for the "no longer in delay window" rejection (PR #10745).

**Bucket 2 — matches the notes, small, probably do now**
- Plain Reset (no RestoreOriginalOptions) of a not-yet-dispatched activity should honor remaining `start_delay` (notes: "respect any remaining start delay"; "consistently with Unpause"). Hoist `respectStartDelay` out of the RestoreOriginalOptions branch (PR #10752).

**Bucket 3 — behavior change, larger, DECIDE then probably defer**
- Honor remaining **backoff / `next_retry_delay`** on Unpause and Reset (notes treat start_delay and backoff as the same "scheduled time"). Today both `unpause()` and `reset()` clear `CurrentRetryInterval` and dispatch immediately. Changing this reshapes both functions **and contradicts an existing, deliberate test** (`TestActivityPauseApi_WhileRetryNoWait` asserts unpause = immediate). Recommend: tracked follow-up with a short design note, looped in with Sean — not a quiet edit in this stack.

Recommendation: land buckets 1+2 in this stack; open a design-decision issue for bucket 3.
