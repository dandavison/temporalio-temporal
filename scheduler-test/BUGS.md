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
- Full suite result on this build: **22/22 pass**. No CONFIRMED bugs in the current worktree code.
- **V1 comparison could not be performed on this setup:** the `start-dev` server creates V2/CHASM
  schedules even with `history.enableCHASMSchedulerCreation=false` and
  `history.enableCHASMSchedulerRouting=false` (the dev server appears to force the CHASM scheduler on),
  so findings 2 and 3 below could not be checked against a genuinely V1-backed schedule here.

## Summary

| # | Finding | Status |
|---|---------|--------|
| 1 | Unpause does not resume actions | INVALIDATED (fixed in this worktree; reproduced only on older build 1.31.0-154.0) |
| 2 | Two manual triggers in the same second coalesce into one run | OPEN (mechanism understood; likely matches V1) |
| 3 | Backfill of a future window runs the actions immediately | OPEN (questionable semantics; likely matches V1) |

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

## 2. Two manual triggers within the same second coalesce into one run — OPEN

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

**How to resolve:** Compare against V1 (fire two rapid triggers at a V1-backed schedule and count
runs) — not yet done; see the V1-comparison note in Environment. If V1 also coalesces, this is a
documented limitation, not a bug. If V1 does not, the fix is to make manual-trigger workflow IDs
unique within a second — e.g. append the Backfiller ID (already unique per trigger) or a
finer-grained timestamp component to the workflow ID for trigger-immediate requests, rather than
reusing the second-truncated spec workflow ID.

---

## 3. Backfill of a future time window runs the actions immediately — OPEN

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

## Areas exercised without finding defects (on 1.32.0)

Interval + calendar specs; SKIP / ALLOW_ALL / BUFFER_ONE / CANCEL_OTHER / TERMINATE_OTHER overlap
policies; limited_actions; `start_at` / `end_at` bounds; `pause_on_failure`; past-window backfill;
`describe` recent/running actions referencing real runs; update of action and of spec (re-arming an
idle schedule); trigger + trigger overlap override; list; delete + NOT_FOUND; recreate-after-delete;
duplicate-create rejection; steady-state action accuracy (~19 actions in 20s, no loss/drift);
minimum interval enforcement (1s; sub-second rejected); empty spec accepted as manual-only.
