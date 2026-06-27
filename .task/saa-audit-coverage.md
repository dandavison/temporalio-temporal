# SAA operator-API audit — coverage ledger

Tracks which regions of the (operation × pre-state × start_delay × task) space have been audited for
task-set bugs, and what was found. Invariant under test: after any operation the live task set =
exactly the pending timeouts for the resulting state, each at the correct anchor + a passing stamp,
no stale survivor. Reference model = WFA (ScС lifetime-anchored; ScS per-attempt; pause = validate-on-fire).

Status legend: `—` unexplored · `OK` audited, clean · `B#n` bug (see Findings) · `D` reduces to open
design question · `N/A` unreachable/not applicable.

Open design questions (NOT bugs): (a) should pause stop/extend ScС? (b) should reset start a fresh full ScС?
Already fixed (do not re-report): reset stale-ScС re-emit, activity.go:979-986.

## Coverage matrix (operation × pre-state)

| operation        | SCHED-in-delay | SCHED-backoff | STARTED | PAUSED        | PAUSE_REQ | RESET_REQ | CANCEL_REQ |
|------------------|----------------|---------------|---------|---------------|-----------|-----------|------------|
| reset            | OK(B3 fixed)   | OK            | OK      | OK(B3 fixed)  | OK        | OK        | OK(reject) |
| reset+restore    | OK(B1 fixed)   | OK(B1 fixed)  | **B4**  | OK(B1 fixed)  | **B4**    | OK(B1 fixed)| OK(reject)|
| pause            | OK             | OK            | OK      | D1            | D1        | n/a       | OK(reject) |
| unpause          | OK             | OK            | n/a     | OK            | OK        | n/a       | OK(reject) |
| update-options   | OK             | OK            | OK      | OK            | OK        | OK        | OK(fixed)  |
| cancel/precedence| OK             | OK            | OK      | OK            | OK        | OK        | OK         |

B1, B2, B3 FIXED. B4 open. D1 = open design question (pause × ScС).
(B3 only when start_delay still pending + pre-pickup. B4 only when a prior update changed StartToClose/heartbeat then reset+restore on a running attempt.)

(reset = plain, no option change: clean everywhere — lifetime ScС anchor unchanged, so no re-arm needed.
reset+restore = the bug surface: only paths that flow through `reset()` got the ScС fix.)

## Rounds

### Round 1 (done) — reset, reset+restore, update-options + stamps
3 finders + self-verification of B1 by reading handleReset/landing transitions. Strong convergence:
B1 found independently by the reset-finder and the stamp-finder. Cells confirmed clean are marked OK above.
Not yet covered: pause, unpause (B1-shape suspected), cancel/precedence, multi-op chains.

## Findings

### B1 (HIGH) — FIXED — reset+RestoreOriginalOptions doesn't re-arm ScheduleToClose on the non-`reset()` paths
Fix: `reissueScheduleToClose` helper called from the RestoreOriginalOptions block (covers all branches);
reused in update-options; removed from reset(). Repro: ResetRestoreOriginal_OnStarted_RecomputesScheduleToCloseDeadline.
All reset/update/pause/unpause/start-delay suites green.
The ScС re-arm fix lives only inside `reset()` (activity.go:979-986). These reset-completion paths
bypass `reset()` and never bump `ScheduleToCloseStamp` / re-emit ScС:
- STARTED, PAUSE_REQUESTED → RESET_REQUESTED, landing via `TransitionResetAttemptFailedToScheduled`
  (statemachine.go:560-604) / `...ToPaused` (statemachine.go:537-553).
- PAUSED + keepPaused (activity.go:1059-1071).
`RestoreOriginalOptions` mutates `ScheduleToCloseTimeout`/`StartDelay` for ALL branches (activity.go:1012-1028).
Result: the live ScС task fires at the **un-restored** deadline (STALE-SURVIVOR); if restore re-enables
ScС from 0, it never fires (LOST-TASK, lower confidence). Worse: on STARTED the stale ScС is not
invalidated at reset-request time, so it can fire mid-RESET_REQUESTED before the reset lands.
Repro shape: start → UpdateOptions change ScС → reset+RestoreOriginal while STARTED → observe timeout at wrong deadline.
WFA does not have this class (timer sequence is recomputed live from FirstScheduledTime+S2C every transition).
Fix sketch: re-arm ScС in the `RestoreOriginalOptions` block (covers all branches uniformly + invalidates
the stale task immediately), and drop the now-redundant re-arm from `reset()`. Plain reset needs no re-arm.

### B2 (LOW) — start_delay window guard returns InvalidArgument, should be FailedPrecondition
activity.go:646. Every sibling state-precondition guard in this file uses FailedPrecondition (e.g. :622,
pause/reset/unpause guards); only this one uses InvalidArgument. Request is well-formed; it's resource
state (delay window elapsed / wrong status) that forbids it → FailedPrecondition (AIP, client retry semantics).
Known nit (PR #10745). No task-set impact.

### Lead for Round 2
`unpause()` (activity.go:927) shares B1's shape: it bumps attempt.Stamp + re-emits dispatch/ScS but
does NOT re-arm ScС — so if options changed while paused, unpause leaves a stale ScС. Verify in pause/unpause round.

### Round 2 (done) — pause, unpause, cancel/precedence, multi-op chains
3 finders + self-verification of the pause/ScС claim. NO NEW BUGS. Results:
- B1 fix verified complete and correct (the new STARTED test is a valid repro). B2 verified.
- unpause: CLEAN. The "unpause B1-shape" lead is REFUTED — UpdateOptions-while-PAUSED already re-arms
  ScС via the helper, and unpause never changes S2C/start_delay, so it has no ScС to re-arm. The stray
  multi-pause-cycle "drift" note is also REFUTED: firstDispatchTime = ScheduleTime+StartDelay (both
  immutable across pause/unpause), so respectStartDelay clamps to the same anchor every cycle.
- cancel / precedence / chains: CLEAN. Guards consistent (FailedPrecondition). The "mutate-then-reject"
  in handleReset restore branch is safe — chasm_engine rolls back all mutations on handler error.
- Cosmetic (not a bug): ResetKeepPaused not cleared on terminal transitions (only ResetHeartbeats is);
  harmless — read only in RESET_REQUESTED-gated paths, unreachable once terminal.

### Round 3 (done) — start_delay cross-cut, Heartbeat, Describe consistency
3 finders. TWO NEW BUGS (both reset-family incompleteness), plus Describe trap confirmed closed.

### B3 (MEDIUM) — plain reset ignores pending start_delay
activity.go: `handleReset` applies `respectStartDelay` only inside the `RestoreOriginalOptions` block
(~:1026). A plain reset (no restore) of a pre-pickup SCHEDULED/PAUSED activity whose start_delay window
is still open leaves `scheduleTime = now` (~:1007), so `reset()` dispatches immediately + emits ScS at
now+S2S, discarding the remaining delay. Inconsistent with BOTH unpause() and reset+RestoreOriginal,
which honor it. Also makes Describe lie: RequestedStartTime/ExpirationTime still report firstDispatchTime
(future) while dispatch fires now. (This is the original review's "bucket 2".)
Fix: hoist `scheduleTime = a.respectStartDelay(scheduleTime)` out of the RestoreOriginalOptions block so
it runs for all pre-pickup reset paths.
Consensus (#crew-standalone-activities): Reset honors the remaining start delay (mirrors Unpause); does
NOT skip or restart it. WFA has no analog (no per-activity start_delay), so this is an SAA-only decision.
Repro: TestStartDelay/ResetDuringDelay_HonorsWallClockTarget (tests/activity_standalone_test.go) — mirrors
the Unpause sibling; CONFIRMED failing on current code (dispatches ~ScheduleTime+1s vs the 5s target).

### B4 (MEDIUM-LOW) — reset+RestoreOriginal on a running attempt restores per-attempt options too early
Resetting a STARTED activity is a DEFERRED reset: it goes to RESET_REQUESTED, the worker keeps running
the in-flight attempt under its current terms, and the reset lands (attempt -> 1, re-dispatch) only when
the worker yields. So RestoreOriginalOptions should leave the running attempt alone and apply the restored
per-attempt options (StartToClose / Heartbeat) on the NEXT attempt.

What goes wrong: the restore block mutates the option fields immediately, before entering RESET_REQUESTED,
but the running attempt's per-attempt timers still fire at the pre-restore (updated) values. So Describe
reports the restored value while the in-flight attempt is actually governed by the old one — reported and
enforced disagree. (ScheduleToClose is exempt: it's a lifetime budget, restored immediately — B1.
start_delay is already skipped for a started attempt.)

Fix (deferred restore): for a running activity, don't restore the per-attempt option fields now — carry the
restore intent through RESET_REQUESTED (like ResetKeepPaused) and apply it when the reset lands on the next
attempt. (The alternative — re-arm the running attempt's timers to the restored values now, like
UpdateOptions does — was rejected: it disturbs the in-flight attempt, contradicting the RESET_REQUESTED model.)

Repro: TestStartDelay/ResetRestoreOriginal_OnStarted_DefersPerAttemptOptionRestore — CONFIRMED failing on
current code (Describe reports the restored 60s mid-attempt; should stay 30s until the reset lands). Note:
the timeout firing time can't distinguish buggy vs fixed (the timer fires at the updated value either way),
so the repro asserts on the reported field, not on a timeout. Fix not yet written.

### Cosmetic (LOW) — create-path ScС anchor uses TransitionScheduled's ctx.Now(), not ScheduleTime
Flagged independently by 2 finders. Constructor sets ScheduleTime=ctx.Now(); TransitionScheduled arms ScС
at its own ctx.Now()+start_delay+S2C. µs-level skew vs firstDispatchTime() used everywhere else (incl.
Describe). Invisible under fake-clock tests. Tidy by anchoring TransitionScheduled on firstDispatchTime().

### Describe consistency — CLEAN (trap closed)
ExpirationTime (=scheduleToCloseDeadline) now always equals the live ScС task's fire time, because every
anchor-moving op re-arms via reissueScheduleToClose. F4 note: RequestedStartTime reports the original
firstDispatchTime, not the actual post-unpause/plain-reset dispatch time — resolved for the reset case
once B3 is fixed (dispatch then == firstDispatchTime). Status mapping, Outcome/LastFailure TimeoutType,
Attempt/CurrentRetryInterval/NextAttemptScheduleTime, CloseTime all consistent.

## Design questions (for the meeting — NOT bugs)

### D1 — pause × ScheduleToClose (confirmed behavior)
SAA today: a paused activity IS timed out at its ScС deadline WHILE paused. Mechanism: TransitionTimedOut
source states include PAUSED + PAUSE_REQUESTED (statemachine.go:375-376); ScС Validate gates only on
TransitionTimedOut.Possible + S2C>0 + stamp (activity_tasks.go:125-145); pause never bumps ScheduleToCloseStamp.
WFA today: firing is SUPPRESSED while paused (timer not generated: timer_sequence.go:184; dropped on fire:
timer_queue_active_task_executor.go:555,590) — a paused WFA activity never times out until unpause, then
fires immediately if past deadline. Note: the WFA *docs* ("a paused activity can still time out") read
closer to SAA's behavior than WFA's. Meeting must pick one semantics for both products.

### D2 — reset × ScheduleToClose (settled toward WFA)
Plain reset does NOT restart ScС (lifetime-anchored). Matches WFA. Open Q for the meeting only if the
team wants reset to grant a fresh budget.

## Coverage status
Whole operation×state matrix swept (Rounds 1-2). Residual / lower-priority not yet given a dedicated pass:
start_delay as its own cross-cut (touched by every op's finder), Heartbeat-timer specifics (touched by
update finder), Describe-field consistency vs actual tasks. Round 3 candidate if desired.
