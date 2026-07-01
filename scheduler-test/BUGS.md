# CHASM (V2) scheduler — findings

Findings from the `scheduler-test/scheduler_sdk_test.py` harness. Each entry has a **Status**
that is kept current as claims are confirmed or invalidated.

- **CONFIRMED** — reproduced against a server built from this worktree; believed to be a real defect.
- **OPEN** — reproduced, but not yet established as a defect vs. intended/V1-matching behavior.
- **INVALIDATED** — claim was wrong, or the behavior does not reproduce on this worktree's code.

## Environment

- Worktree: `scheduler` branch, HEAD `04d52a12af`.
- Server under test built from this worktree: `temporal-server` **1.32.0**, started via a CLI built
  with a `go.mod` replace onto this worktree, run with
  `history.enableCHASMSchedulerCreation=true` and `history.chasmSchedulerCreationRolloutPercent=100`.
  Confirmed the schedules are V2/CHASM-backed (no `temporal-sys-scheduler-workflow` executions exist).
- Full suite result on this build: **27/27 pass**. No CONFIRMED bugs in the current worktree code.
- **V1 comparison:** performed by restarting the same server with
  `history.enableCHASMSchedulerCreation=false`, `history.chasmSchedulerCreationRolloutPercent=0`,
  `history.enableCHASMSchedulerRouting=false`. That does produce V1-backed schedules (confirmed: a
  `temporal-sys-scheduler:<id>` workflow of type `temporal-sys-scheduler-workflow` exists and the
  schedule fires). Findings 2 and 3 reproduce identically on V1, so they are pre-existing behavior,
  not V2 regressions.

## Summary

| # | Finding | Status |
|---|---------|--------|
| 1 | Unpause does not resume actions | INVALIDATED (fixed in this worktree; reproduced only on older build 1.31.0-154.0) |
| 2 | Two manual triggers in the same second coalesce into one run | RESOLVED — matches V1 (pre-existing limitation, not a V2 bug) |
| 3 | Backfill of a future window runs the actions immediately | RESOLVED — matches V1 (pre-existing behavior, not a V2 bug) |

---

## 1. Unpause does not resume actions — INVALIDATED

**Status:** INVALIDATED on this worktree (server 1.32.0). Reproduced only on the older bundled dev
server (server 1.31.0-154.0). Retained here because the harness's `pause.halts_and_resumes` test
guards against its regression.

**What (as observed on 1.31.0-154.0):** After `pause()` then `unpause()`, the schedule reported
`state.paused=false` and `next_action_times` kept advancing, yet no further actions were taken —
`num_actions` frozen, `num_actions_skipped_overlap=0`, `running_actions=0` — for 40s+, under both
SKIP and ALLOW_ALL. Action-taking worked normally before the pause (completions observed), so it was
specifically the unpause path that failed to re-arm action execution.

**Repro:** `make scheduler-test-sdk SCHEDULER_TEST_ARGS="--only pause.halts_and_resumes"` against the
respective server. Passes on 1.32.0, failed on 1.31.0-154.0.

**Mechanism (why it's fixed here):** In this worktree the generator task is kept perpetually
scheduled whenever the spec yields a next wakeup, advancing the high-water mark on each tick without
buffering while paused (`chasm/lib/scheduler/generator_tasks.go:121-123` and `:168-172`). So the
generator keeps ticking through a pause and resumes buffering as soon as `paused` clears. The older
build appears to have parked the generator on pause without re-arming it on unpause.

**Action:** none needed on current code; keep the regression test.

---

## 2. Two manual triggers within the same second coalesce into one run — RESOLVED (matches V1)

**Verdict:** Reproduced identically on a V1-backed schedule (two rapid `ALLOW_ALL` triggers →
`num_actions == 1`). Pre-existing behavior, not a V2 regression. Documented here as a limitation.


**What:** Issuing two immediate triggers (`ScheduleHandle.trigger`) less than one second apart
produces only a single workflow execution, even when each trigger passes an explicit
`overlap=ALLOW_ALL` override and the schedule's base overlap policy is also ALLOW_ALL. Triggers
spaced ≥1s apart correctly produce two concurrent runs.

**Repro:**

```python
h = await client.create_schedule(sid, Schedule(
    action=ScheduleActionStartWorkflow("sleeper", 30.0, id=sid+"-wf", task_queue=TQ),
    spec=ScheduleSpec(intervals=[ScheduleIntervalSpec(every=timedelta(seconds=3600))]),
    policy=SchedulePolicy(overlap=ScheduleOverlapPolicy.ALLOW_ALL),
    state=ScheduleState(paused=True)))
await h.trigger(overlap=ScheduleOverlapPolicy.ALLOW_ALL)
await h.trigger(overlap=ScheduleOverlapPolicy.ALLOW_ALL)   # same second
# => info.num_actions == 1, running_actions == 1   (expected 2)
# Insert `await asyncio.sleep(1.5)` between the triggers => num_actions == 2, running == 2.
```

**Mechanism:** Each trigger creates its own Backfiller, and `processTrigger` derives the started
workflow's ID from the current time via `GenerateWorkflowID(scheduler.WorkflowID(), now)`
(`chasm/lib/scheduler/backfiller_tasks.go:218-219`). `GenerateWorkflowID` truncates the nominal time
to the second (`common/schedules/id.go:42-45`: `nominalTime.Truncate(time.Second)`). Two triggers
that resolve to the same second therefore generate an identical workflow ID; the second start
collides with the first and is dropped, regardless of overlap policy.

**Is it a bug?** Uncertain. For spec-driven actions the 1s truncation is harmless (the minimum
allowed interval is 1s — sub-second intervals are rejected with "interval is too small", so two
scheduled actions can never share a second). It only bites *manual* triggers, where a user may
legitimately fire two triggers in the same second and, especially with an explicit ALLOW_ALL
override, expect two runs. V1 also embeds the truncated nominal time in the workflow ID, so this is
plausibly long-standing intended behavior rather than a V2 regression.

**If it is ever deemed worth fixing:** make manual-trigger workflow IDs unique within a second —
e.g. append the Backfiller ID (already unique per trigger) or a finer-grained timestamp component to
the workflow ID for trigger-immediate requests, rather than reusing the second-truncated spec
workflow ID. This would be a change to both V1 and V2 semantics.

---

## 3. Backfill of a future time window runs the actions immediately — RESOLVED (matches V1)

**Verdict:** Reproduced identically on a V1-backed schedule (future-window backfill ran ~57 actions).
Pre-existing behavior, not a V2 regression. Left as a note for product: a future backfill window is
arguably nonsensical and could be rejected, but that would be a change to both V1 and V2.


**What:** Calling `backfill()` with a window entirely in the future (e.g. `[now+60s, now+120s]`) is
accepted and immediately starts the actions for every nominal time in that window (observed ~34
actions for a 60s window at a 1s interval). A future window arguably has nothing to backfill and
should be a no-op or rejected.

**Repro:**

```python
h = await client.create_schedule(sid, Schedule(
    action=ScheduleActionStartWorkflow("noop", id=sid+"-wf", task_queue=TQ),
    spec=ScheduleSpec(intervals=[ScheduleIntervalSpec(every=timedelta(seconds=1))]),
    policy=SchedulePolicy(overlap=ScheduleOverlapPolicy.BUFFER_ALL),
    state=ScheduleState(paused=True)))
now = datetime.now(timezone.utc)
await h.backfill(ScheduleBackfill(start_at=now+timedelta(seconds=60),
                                  end_at=now+timedelta(seconds=120),
                                  overlap=ScheduleOverlapPolicy.BUFFER_ALL))
# => info.num_actions climbs to ~34+ (expected 0)
```

**Mechanism (hypothesis):** Backfill processing walks the spec's nominal times within
`[start_at, end_at]` and enqueues a buffered start for each, without checking that the range is in
the past. The started workflows carry future-dated nominal timestamps in their IDs.

**Is it a bug?** Uncertain and low severity. Likely matches V1 (backfill does not validate that the
window precedes now). Surprising, but harmless beyond producing actions with future-dated IDs.

**How to resolve:** Compare against V1. If undesired, reject a backfill whose `end_at` is in the
future (or clamp it to now) in the schedule-request validation path.

---

## Durability across crash + restart (manual probe — verified)

The core promise, checked with a probe (not in the automated harness, which deliberately does not
manage the server). Procedure: create an interval-1s schedule with overlap BUFFER_ALL; let it fire;
`kill -9` the server; wait ~10s; restart with the **same** SQLite DB and V2 flags; re-describe.

Result: the schedule survived the crash, `num_actions` jumped from 3 to 31 on restart (the missed
occurrences during downtime were caught up under BUFFER_ALL), and it resumed firing at ~1/s. The
CHASM scheduler's state and catch-up behavior are durable across an ungraceful restart. **No defect.**

## Ambitious combinations / product claims exercised (all pass on 1.32.0)

- **Timezone/DST:** a `daily 12:00 America/New_York` calendar spec yields next action times at 12:00
  New York local (16:00 UTC in EDT) — the zone offset is applied, not treated as UTC.
- **Jitter:** with jitter set, action scheduled times are perturbed off the nominal interval boundary.
- **limited_actions × overlap SKIP:** SKIP-skipped occurrences do **not** consume the action budget —
  a `remaining_actions=2` schedule with an action slower than its interval still takes exactly 2
  actual actions (with intervening skips recorded).
- **Search attributes:** workflows started by a schedule are queryable via
  `TemporalScheduledById = '<schedule id>'` (documented capability holds).
- **pause_on_failure semantics:** on → a failing action pauses the schedule (with a note); off → the
  schedule keeps firing despite repeated failures.

## Other areas exercised without finding defects (on 1.32.0)

Interval + calendar specs; SKIP / ALLOW_ALL / BUFFER_ONE / CANCEL_OTHER / TERMINATE_OTHER overlap
policies; limited_actions; `start_at` / `end_at` bounds; past-window backfill; `describe`
recent/running actions referencing real runs; update of action and of spec (re-arming an idle
schedule); trigger + trigger overlap override; list; delete + NOT_FOUND; recreate-after-delete;
duplicate-create rejection; steady-state action accuracy (~19 actions in 20s, no loss/drift);
minimum interval enforcement (1s; sub-second rejected); empty spec accepted as manual-only.
