#!/usr/bin/env -S uv run --script
#
# /// script
# requires-python = ">=3.12"
# dependencies = ["temporalio"]
# ///
"""
SDK-client test harness for the CHASM (V2) scheduler.

Drives the public Schedule API (Create/Describe/Update/Patch/Delete via the Temporal Python SDK's
high-level schedule client) against a running server. The API is backend-agnostic, so to exercise
the V2 implementation the server must be started with CHASM scheduler creation enabled:

  temporal server start-dev \\
    --dynamic-config-value history.enableCHASMSchedulerCreation=true \\
    --dynamic-config-value history.chasmSchedulerCreationRolloutPercent=100

Architecture (mirrors cli/saa-test/saa_sdk_test.py):
  - server: NOT started here — must already be listening on localhost:7233
  - client: temporalio high-level schedule client, in-process async
  - worker: a SEPARATE subprocess (the `worker` subcommand) running the scheduled workflows
  - assertions: on ScheduleDescription fields / RPC responses / started-workflow results

Outputs under scheduler-test/: log.text (RESULT lines drive resume), bugs/NN-slug.md.
Run: uv run scheduler-test/scheduler_sdk_test.py [--fresh] [--only id,id] [--rerun-failed] [--list]
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import re
import signal
import subprocess
import sys
import traceback
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional
from zoneinfo import ZoneInfo

from temporalio.client import (
    Client,
    Schedule,
    ScheduleActionStartWorkflow,
    ScheduleAlreadyRunningError,
    ScheduleBackfill,
    ScheduleCalendarSpec,
    ScheduleHandle,
    ScheduleIntervalSpec,
    ScheduleOverlapPolicy,
    SchedulePolicy,
    ScheduleRange,
    ScheduleSpec,
    ScheduleState,
    ScheduleUpdate,
    ScheduleUpdateInput,
    WorkflowExecutionStatus,
)
from temporalio.common import RetryPolicy
from temporalio.service import RPCError, RPCStatusCode

SCRIPT_DIR = Path(__file__).resolve().parent
LOG_PATH = SCRIPT_DIR / "log.text"
BUGS_DIR = SCRIPT_DIR / "bugs"
WORKER_LOG = SCRIPT_DIR / "worker.log"

ADDRESS = "localhost:7233"
NAMESPACE = "default"
TASK_QUEUE = "scheduler-test-tq"


# --------------------------------------------------------------------------
# Failure / assertions
# --------------------------------------------------------------------------
class TestFailure(AssertionError):
    pass


def expect(cond: bool, msg: str) -> None:
    if not cond:
        raise TestFailure(msg)


def expect_eq(actual: Any, expected: Any, what: str) -> None:
    if actual != expected:
        raise TestFailure(f"{what}: expected {expected!r}, got {actual!r}")


def is_not_found(e: BaseException) -> bool:
    return isinstance(e, RPCError) and e.status == RPCStatusCode.NOT_FOUND


# An exception from a racing op is "dirty" (a real defect) if it's a server-internal fault rather
# than a legitimate precondition/argument/not-found/already-exists rejection.
CLEAN_CODES = {
    RPCStatusCode.FAILED_PRECONDITION,
    RPCStatusCode.INVALID_ARGUMENT,
    RPCStatusCode.NOT_FOUND,
    RPCStatusCode.ALREADY_EXISTS,
    RPCStatusCode.DEADLINE_EXCEEDED,
    RPCStatusCode.CANCELLED,
}


def is_dirty_error(e: BaseException) -> bool:
    if isinstance(e, RPCError):
        return e.status not in CLEAN_CODES
    return True  # any non-RPCError (panic surfaced as Unknown, client bug, etc.)


def assert_clean(results: list[Any], what: str) -> None:
    dirty = [r for r in results if isinstance(r, BaseException) and is_dirty_error(r)]
    if dirty:
        raise TestFailure(f"{what}: {len(dirty)} dirty error(s); first: {dirty[0]!r}")


async def poll_until(
    fn: Callable[[], Awaitable[bool]], timeout: float = 20.0, interval: float = 0.3
) -> bool:
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if await fn():
            return True
        await asyncio.sleep(interval)
    return False


# --------------------------------------------------------------------------
# SDK client wrapper (high-level schedule client)
# --------------------------------------------------------------------------
@dataclass
class SchedClient:
    client: Client
    ops: list[str] = field(default_factory=list)

    def _log(self, s: str) -> None:
        self.ops.append(s)

    def action(
        self,
        wf_id: str,
        *,
        workflow: str = "noop",
        arg: Any = None,
        max_attempts: int = 0,
    ) -> ScheduleActionStartWorkflow:
        kw: dict[str, Any] = {"id": wf_id, "task_queue": TASK_QUEUE}
        if arg is not None:
            kw["arg"] = arg
        if max_attempts:
            kw["retry_policy"] = RetryPolicy(maximum_attempts=max_attempts)
        return ScheduleActionStartWorkflow(workflow, **kw)

    async def create(
        self,
        sched_id: str,
        *,
        every: float = 1.0,
        workflow: str = "noop",
        arg: Any = None,
        max_attempts: int = 0,
        paused: bool = False,
        overlap: ScheduleOverlapPolicy = ScheduleOverlapPolicy.SKIP,
        pause_on_failure: bool = False,
        remaining_actions: int = 0,
        start_at: Optional[dt.datetime] = None,
        end_at: Optional[dt.datetime] = None,
        spec: Optional[ScheduleSpec] = None,
        trigger_immediately: bool = False,
    ) -> ScheduleHandle:
        self._log(
            f"create {sched_id} every={every} wf={workflow} paused={paused} "
            f"overlap={overlap.name} pause_on_failure={pause_on_failure} "
            f"remaining={remaining_actions}"
        )
        if spec is None:
            spec = ScheduleSpec(
                intervals=[ScheduleIntervalSpec(every=dt.timedelta(seconds=every))]
            )
        if start_at is not None:
            spec.start_at = start_at
        if end_at is not None:
            spec.end_at = end_at
        return await self.client.create_schedule(
            sched_id,
            Schedule(
                action=self.action(
                    f"{sched_id}-wf",
                    workflow=workflow,
                    arg=arg,
                    max_attempts=max_attempts,
                ),
                spec=spec,
                policy=SchedulePolicy(
                    overlap=overlap, pause_on_failure=pause_on_failure
                ),
                state=ScheduleState(
                    paused=paused,
                    limited_actions=remaining_actions > 0,
                    remaining_actions=remaining_actions,
                ),
            ),
            trigger_immediately=trigger_immediately,
        )

    async def num_actions(self, handle: ScheduleHandle) -> int:
        return (await handle.describe()).info.num_actions

    async def num_running(self, handle: ScheduleHandle) -> int:
        return len((await handle.describe()).info.running_actions)

    async def wf_status(self, wf_id: str, run_id: str) -> WorkflowExecutionStatus:
        return (
            await self.client.get_workflow_handle(wf_id, run_id=run_id).describe()
        ).status

    async def recent(self, handle: ScheduleHandle):
        return (await handle.describe()).info.recent_actions

    async def cleanup(self, handle: ScheduleHandle) -> None:
        try:
            await handle.delete()
        except RPCError:
            pass


# --------------------------------------------------------------------------
# Test registry
# --------------------------------------------------------------------------
@dataclass
class Test:
    id: str
    fn: Callable[[SchedClient], Awaitable[None]]
    doc: str


REGISTRY: list[Test] = []


def test(test_id: str):
    def deco(fn):
        REGISTRY.append(Test(test_id, fn, (fn.__doc__ or "").strip()))
        return fn

    return deco


_COUNTER = 0


def sid(prefix: str) -> str:
    global _COUNTER
    _COUNTER += 1
    return f"{prefix}-{dt.datetime.now().strftime('%H%M%S')}-{_COUNTER}"


# ==========================================================================
# Worker (subcommand): runs the scheduled workflows
# ==========================================================================
from temporalio import workflow  # noqa: E402
from temporalio.exceptions import ApplicationError  # noqa: E402


@workflow.defn(name="noop")
class Noop:
    @workflow.run
    async def run(self) -> str:
        return "ok"


@workflow.defn(name="sleeper")
class Sleeper:
    @workflow.run
    async def run(self, seconds: float = 5.0) -> str:
        await asyncio.sleep(seconds)
        return "slept"


@workflow.defn(name="boom")
class Boom:
    @workflow.run
    async def run(self) -> str:
        raise ApplicationError("intentional scheduled-action failure")


def run_worker() -> None:
    from temporalio.worker import UnsandboxedWorkflowRunner, Worker

    async def main() -> None:
        client = await Client.connect(ADDRESS, namespace=NAMESPACE)
        async with Worker(
            client,
            task_queue=TASK_QUEUE,
            workflows=[Noop, Sleeper, Boom],
            workflow_runner=UnsandboxedWorkflowRunner(),
        ):
            stop = asyncio.Event()
            loop = asyncio.get_running_loop()
            for s in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(s, stop.set)
            await stop.wait()

    asyncio.run(main())


class Worker:
    def __init__(self) -> None:
        self.proc: Optional[subprocess.Popen] = None

    def start(self) -> None:
        args = ["uv", "run", str(SCRIPT_DIR / "scheduler_sdk_test.py"), "worker"]
        self.proc = subprocess.Popen(
            args, stdout=open(WORKER_LOG, "w"), stderr=subprocess.STDOUT
        )

    def stop(self) -> None:
        if self.proc and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGINT)
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()

    async def wait_ready(self, client: Client) -> None:
        from temporalio.client import WorkflowFailureError

        deadline = asyncio.get_event_loop().time() + 60
        last: Optional[Exception] = None
        while asyncio.get_event_loop().time() < deadline:
            try:
                await client.execute_workflow(
                    "noop",
                    id=sid("worker-probe"),
                    task_queue=TASK_QUEUE,
                    execution_timeout=dt.timedelta(seconds=20),
                )
                return
            except (RPCError, WorkflowFailureError) as e:
                last = e
                await asyncio.sleep(1)
        raise RuntimeError(f"worker did not execute probe; see {WORKER_LOG}: {last}")


# ==========================================================================
# TESTS
# ==========================================================================
@test("basic.interval_triggers")
async def t_interval_triggers(c: SchedClient) -> None:
    """An interval schedule starts actions on its cadence, and the started workflows run to
    completion on the worker."""
    h = await c.create(sid("interval"), every=1.0)
    try:
        fired = await poll_until(lambda: _at_least_actions(c, h, 2), timeout=20)
        expect(fired, "expected >= 2 actions within 20s")
        desc = await h.describe()
        wf_id = desc.info.recent_actions[0].action.workflow_id
        run_id = desc.info.recent_actions[0].action.first_execution_run_id
        result = await c.client.get_workflow_handle(wf_id, run_id=run_id).result()
        expect_eq(result, "ok", "scheduled workflow result")
    finally:
        await c.cleanup(h)


async def _at_least_actions(c: SchedClient, h: ScheduleHandle, n: int) -> bool:
    return await c.num_actions(h) >= n


@test("trigger.immediate")
async def t_trigger_immediate(c: SchedClient) -> None:
    """A paused schedule takes no actions on its own, but an explicit trigger fires one immediately."""
    h = await c.create(sid("trigger"), every=3600, paused=True)
    try:
        await asyncio.sleep(2)
        expect_eq(await c.num_actions(h), 0, "paused schedule should not act")
        await h.trigger()
        fired = await poll_until(lambda: _at_least_actions(c, h, 1), timeout=15)
        expect(fired, "trigger should produce one action")
    finally:
        await c.cleanup(h)


@test("pause.halts_and_resumes")
async def t_pause_resume(c: SchedClient) -> None:
    """Pausing stops new actions; unpausing resumes them."""
    h = await c.create(sid("pause"), every=1.0)
    try:
        expect(
            await poll_until(lambda: _at_least_actions(c, h, 1), timeout=15),
            "expected initial action",
        )
        await h.pause(note="halt")
        await asyncio.sleep(0.5)
        n = await c.num_actions(h)
        await asyncio.sleep(3)
        expect_eq(
            await c.num_actions(h), n, "paused schedule should not accrue actions"
        )
        expect(
            (await h.describe()).schedule.state.paused, "describe should report paused"
        )
        await h.unpause(note="resume")
        expect(
            await poll_until(lambda: _at_least_actions(c, h, n + 1), timeout=15),
            "unpause should resume actions",
        )
    finally:
        await c.cleanup(h)


@test("update.changes_action")
async def t_update(c: SchedClient) -> None:
    """Update replaces the schedule's action; the change is reflected in Describe."""
    h = await c.create(sid("update"), every=3600, paused=True)
    try:
        new_wf_id = sid("updated-target")

        async def mutate(inp: ScheduleUpdateInput) -> ScheduleUpdate:
            sched = inp.description.schedule
            sched.action = c.action(new_wf_id, workflow="noop")
            return ScheduleUpdate(schedule=sched)

        await h.update(mutate)
        desc = await h.describe()
        expect_eq(desc.schedule.action.id, new_wf_id, "updated action workflow id")
    finally:
        await c.cleanup(h)


@test("delete.then_describe_not_found")
async def t_delete(c: SchedClient) -> None:
    """After Delete, Describe fails with NOT_FOUND."""
    h = await c.create(sid("delete"), every=3600, paused=True)
    await h.delete()
    try:
        await h.describe()
        raise TestFailure("describe after delete should have failed")
    except RPCError as e:
        expect(is_not_found(e), f"expected NOT_FOUND, got {e.status}")


@test("overlap.skip_while_running")
async def t_overlap_skip(c: SchedClient) -> None:
    """With overlap=SKIP and an action slower than the interval, overlapping actions are skipped
    rather than run concurrently."""
    h = await c.create(
        sid("overlap"),
        every=1.0,
        workflow="sleeper",
        arg=10.0,
        overlap=ScheduleOverlapPolicy.SKIP,
    )
    try:
        skipped = await poll_until(lambda: _skipped_at_least(c, h, 1), timeout=20)
        expect(skipped, "expected at least one skipped-overlap action")
        expect_eq(
            (await h.describe()).schedule.policy.overlap,
            ScheduleOverlapPolicy.SKIP,
            "overlap policy round-trip",
        )
    finally:
        await c.cleanup(h)


async def _skipped_at_least(c: SchedClient, h: ScheduleHandle, n: int) -> bool:
    return (await h.describe()).info.num_actions_skipped_overlap >= n


async def _running_at_least(c: SchedClient, h: ScheduleHandle, n: int) -> bool:
    return await c.num_running(h) >= n


async def _wf_status_is(
    c: SchedClient, wf_id: str, run_id: str, status: WorkflowExecutionStatus
) -> bool:
    return await c.wf_status(wf_id, run_id) == status


# ---- overlap policies ----------------------------------------------------
@test("overlap.allow_all_runs_concurrently")
async def t_overlap_allow_all(c: SchedClient) -> None:
    """overlap=ALLOW_ALL lets slower-than-interval actions run concurrently."""
    h = await c.create(
        sid("allowall"),
        every=1.0,
        workflow="sleeper",
        arg=6.0,
        overlap=ScheduleOverlapPolicy.ALLOW_ALL,
    )
    try:
        expect(
            await poll_until(lambda: _running_at_least(c, h, 2), timeout=15),
            "expected >= 2 concurrently running actions under ALLOW_ALL",
        )
    finally:
        await c.cleanup(h)


@test("overlap.buffer_one_serializes")
async def t_overlap_buffer_one(c: SchedClient) -> None:
    """overlap=BUFFER_ONE never runs two actions at once; buffered actions run serially, so the
    action count still advances past the first while running stays capped at 1."""
    h = await c.create(
        sid("bufferone"),
        every=1.0,
        workflow="sleeper",
        arg=3.0,
        overlap=ScheduleOverlapPolicy.BUFFER_ONE,
    )
    try:
        max_running = 0
        for _ in range(20):
            await asyncio.sleep(0.5)
            max_running = max(max_running, await c.num_running(h))
        expect(
            max_running <= 1,
            f"BUFFER_ONE must not run concurrently (saw {max_running})",
        )
        expect(
            await c.num_actions(h) >= 2, "buffered actions should still run serially"
        )
    finally:
        await c.cleanup(h)


@test("overlap.cancel_other_cancels_running")
async def t_overlap_cancel_other(c: SchedClient) -> None:
    """overlap=CANCEL_OTHER cancels the in-flight action before starting the next."""
    h = await c.create(
        sid("cancelother"),
        every=2.0,
        workflow="sleeper",
        arg=60.0,
        overlap=ScheduleOverlapPolicy.CANCEL_OTHER,
    )
    try:
        expect(
            await poll_until(lambda: _running_at_least(c, h, 1), timeout=15),
            "no first run",
        )
        first = (await h.describe()).info.running_actions[0]
        expect(
            await poll_until(lambda: _at_least_actions(c, h, 2), timeout=15),
            "second action never started",
        )
        expect(
            await poll_until(
                lambda: _wf_status_is(
                    c,
                    first.workflow_id,
                    first.first_execution_run_id,
                    WorkflowExecutionStatus.CANCELED,
                ),
                timeout=15,
            ),
            "first run should be CANCELED when the next action starts",
        )
    finally:
        await c.cleanup(h)


@test("overlap.terminate_other_terminates_running")
async def t_overlap_terminate_other(c: SchedClient) -> None:
    """overlap=TERMINATE_OTHER terminates the in-flight action before starting the next."""
    h = await c.create(
        sid("termother"),
        every=2.0,
        workflow="sleeper",
        arg=60.0,
        overlap=ScheduleOverlapPolicy.TERMINATE_OTHER,
    )
    try:
        expect(
            await poll_until(lambda: _running_at_least(c, h, 1), timeout=15),
            "no first run",
        )
        first = (await h.describe()).info.running_actions[0]
        expect(
            await poll_until(lambda: _at_least_actions(c, h, 2), timeout=15),
            "second action never started",
        )
        expect(
            await poll_until(
                lambda: _wf_status_is(
                    c,
                    first.workflow_id,
                    first.first_execution_run_id,
                    WorkflowExecutionStatus.TERMINATED,
                ),
                timeout=15,
            ),
            "first run should be TERMINATED when the next action starts",
        )
    finally:
        await c.cleanup(h)


# ---- limited actions / bounds --------------------------------------------
@test("limited.stops_after_remaining")
async def t_limited(c: SchedClient) -> None:
    """A limited schedule takes exactly remaining_actions actions and then stops."""
    h = await c.create(sid("limited"), every=1.0, remaining_actions=2)
    try:
        expect(
            await poll_until(lambda: _at_least_actions(c, h, 2), timeout=15),
            "expected the 2 permitted actions",
        )
        await asyncio.sleep(4)
        expect_eq(
            await c.num_actions(h),
            2,
            "limited schedule must not exceed remaining_actions",
        )
    finally:
        await c.cleanup(h)


@test("spec.start_at_defers_actions")
async def t_start_at(c: SchedClient) -> None:
    """No actions are taken before spec.start_at; actions begin only afterwards."""
    now = _utcnow()
    h = await c.create(
        sid("startat"), every=1.0, start_at=now + dt.timedelta(seconds=6)
    )
    try:
        await asyncio.sleep(3)
        expect_eq(await c.num_actions(h), 0, "no actions should occur before start_at")
        expect(
            await poll_until(lambda: _at_least_actions(c, h, 1), timeout=15),
            "actions should start after start_at",
        )
    finally:
        await c.cleanup(h)


@test("spec.end_at_stops_actions")
async def t_end_at(c: SchedClient) -> None:
    """Actions stop once spec.end_at has passed."""
    now = _utcnow()
    h = await c.create(sid("endat"), every=1.0, end_at=now + dt.timedelta(seconds=4))
    try:
        expect(
            await poll_until(lambda: _at_least_actions(c, h, 1), timeout=10),
            "expected some actions before end_at",
        )
        await asyncio.sleep(5)
        n = await c.num_actions(h)
        await asyncio.sleep(4)
        expect_eq(await c.num_actions(h), n, "no actions should occur after end_at")
    finally:
        await c.cleanup(h)


# ---- pause on failure ----------------------------------------------------
@test("pause_on_failure.pauses_schedule")
async def t_pause_on_failure(c: SchedClient) -> None:
    """With pause_on_failure, a failing scheduled workflow pauses the schedule (with a note)."""
    h = await c.create(
        sid("pof"), every=1.0, workflow="boom", max_attempts=1, pause_on_failure=True
    )
    try:
        expect(
            await poll_until(lambda: _is_paused(c, h), timeout=25),
            "schedule should pause after a failing action",
        )
        desc = await h.describe()
        expect(desc.schedule.state.paused, "state.paused should be true")
        expect(bool(desc.schedule.state.note), "a pause note should be set")
    finally:
        await c.cleanup(h)


async def _is_paused(c: SchedClient, h: ScheduleHandle) -> bool:
    return (await h.describe()).schedule.state.paused


# ---- backfill ------------------------------------------------------------
@test("backfill.runs_past_window")
async def t_backfill(c: SchedClient) -> None:
    """Backfilling a past time window enqueues the actions that would have run in it."""
    now = _utcnow()
    h = await c.create(
        sid("backfill"),
        every=1.0,
        paused=True,
        overlap=ScheduleOverlapPolicy.BUFFER_ALL,
    )
    try:
        await h.backfill(
            ScheduleBackfill(
                start_at=now - dt.timedelta(seconds=6),
                end_at=now - dt.timedelta(seconds=1),
                overlap=ScheduleOverlapPolicy.BUFFER_ALL,
            )
        )
        expect(
            await poll_until(lambda: _at_least_actions(c, h, 3), timeout=20),
            "backfill should produce multiple past actions",
        )
    finally:
        await c.cleanup(h)


# ---- describe / listing / action identity --------------------------------
@test("describe.recent_actions_reference_real_runs")
async def t_recent_actions(c: SchedClient) -> None:
    """recent_actions entries reference started workflow runs that actually exist, and carry a
    workflow id derived from (but distinct from) the configured base id."""
    base = sid("recent")
    h = await c.create(base, every=1.0)
    try:
        expect(
            await poll_until(lambda: _at_least_actions(c, h, 1), timeout=15),
            "no action",
        )
        r = (await c.recent(h))[0]
        expect(
            r.action.workflow_id.startswith(f"{base}-wf"),
            "workflow id should derive from base",
        )
        expect(
            r.action.workflow_id != f"{base}-wf",
            "scheduler should append the nominal time to keep ids unique",
        )
        status = await c.wf_status(
            r.action.workflow_id, r.action.first_execution_run_id
        )
        expect(
            status
            in (WorkflowExecutionStatus.RUNNING, WorkflowExecutionStatus.COMPLETED),
            f"referenced run should exist (status={status})",
        )
    finally:
        await c.cleanup(h)


@test("list.includes_created_schedule")
async def t_list(c: SchedClient) -> None:
    """A created schedule appears in ListSchedules."""
    s = sid("listed")
    h = await c.create(s, every=3600, paused=True)
    try:
        found = await poll_until(lambda: _in_list(c, s), timeout=20)
        expect(found, f"{s} should appear in ListSchedules (eventually consistent)")
    finally:
        await c.cleanup(h)


async def _in_list(c: SchedClient, sched_id: str) -> bool:
    async for d in await c.client.list_schedules():
        if d.id == sched_id:
            return True
    return False


# ---- update re-arms the generator ----------------------------------------
@test("update.spec_change_takes_effect")
async def t_update_cadence(c: SchedClient) -> None:
    """Updating the spec to a live cadence causes an effectively-idle schedule to start firing."""
    h = await c.create(sid("cadence"), every=3600)
    try:
        await asyncio.sleep(2)
        n0 = await c.num_actions(h)

        async def mutate(inp: ScheduleUpdateInput) -> ScheduleUpdate:
            sched = inp.description.schedule
            sched.spec = ScheduleSpec(
                intervals=[ScheduleIntervalSpec(every=dt.timedelta(seconds=1))]
            )
            return ScheduleUpdate(schedule=sched)

        await h.update(mutate)
        expect(
            await poll_until(lambda: _at_least_actions(c, h, n0 + 1), timeout=15),
            "spec update to a 1s cadence should produce new actions",
        )
    finally:
        await c.cleanup(h)


@test("trigger.overlap_override_allows_concurrency")
async def t_trigger_override(c: SchedClient) -> None:
    """A manual trigger's overlap override forces concurrency even on a default-SKIP schedule.

    The triggers are spaced >1s apart on purpose: manual-trigger workflow IDs embed the nominal
    time truncated to the second (common/schedules/id.go GenerateWorkflowID), so two triggers in
    the same second collide on workflow ID and coalesce to one run — see BUGS.md 'trigger
    coalescing'."""
    h = await c.create(
        sid("trigoverride"), every=3600, paused=True, workflow="sleeper", arg=30.0
    )
    try:
        await h.trigger(overlap=ScheduleOverlapPolicy.ALLOW_ALL)
        await asyncio.sleep(1.5)
        await h.trigger(overlap=ScheduleOverlapPolicy.ALLOW_ALL)
        expect(
            await poll_until(lambda: _running_at_least(c, h, 2), timeout=15),
            "two spaced ALLOW_ALL triggers should run concurrently",
        )
    finally:
        await c.cleanup(h)


@test("spec.calendar_every_second")
async def t_calendar(c: SchedClient) -> None:
    """A calendar spec matching every second fires and round-trips as a structured calendar."""
    every_sec = ScheduleCalendarSpec(
        second=[ScheduleRange(0, 59)],
        minute=[ScheduleRange(0, 59)],
        hour=[ScheduleRange(0, 23)],
    )
    h = await c.create(sid("calendar"), spec=ScheduleSpec(calendars=[every_sec]))
    try:
        expect(
            await poll_until(lambda: _at_least_actions(c, h, 2), timeout=20),
            "calendar spec matching every second should fire repeatedly",
        )
        expect(
            len((await h.describe()).schedule.spec.calendars) >= 1,
            "calendar should round-trip",
        )
    finally:
        await c.cleanup(h)


@test("lifecycle.recreate_after_delete")
async def t_recreate(c: SchedClient) -> None:
    """A schedule ID can be reused once the prior schedule is deleted."""
    s = sid("recreate")
    h = await c.create(s, every=3600, paused=True)
    await h.delete()
    h2 = await c.create(s, every=3600, paused=True)
    try:
        expect(
            bool((await h2.describe()).id), "recreated schedule should be describable"
        )
    finally:
        await c.cleanup(h2)


@test("lifecycle.duplicate_create_rejected")
async def t_duplicate(c: SchedClient) -> None:
    """Creating a schedule whose ID is already running is rejected."""
    s = sid("dup")
    h = await c.create(s, every=3600, paused=True)
    try:
        try:
            await c.create(s, every=3600, paused=True)
            raise TestFailure("duplicate create should have been rejected")
        except ScheduleAlreadyRunningError:
            pass
    finally:
        await c.cleanup(h)


# ---- ambitious combinations / product claims -----------------------------
@test("spec.timezone_calendar_fires_at_local_time")
async def t_timezone(c: SchedClient) -> None:
    """A calendar spec with a time_zone_name resolves to the correct absolute (UTC) instants: a
    daily 12:00 America/New_York schedule's next action times are 12:00 *New York* local, i.e. the
    server applies the zone's UTC offset (and would track DST) rather than treating the wall clock
    as UTC."""
    every_noon_ny = ScheduleSpec(
        calendars=[
            ScheduleCalendarSpec(
                hour=[ScheduleRange(12)],
                minute=[ScheduleRange(0)],
                second=[ScheduleRange(0)],
            )
        ],
        time_zone_name="America/New_York",
    )
    h = await c.create(sid("tz"), paused=True, spec=every_noon_ny)
    try:
        nts = (await h.describe()).info.next_action_times
        expect(len(nts) >= 1, "expected upcoming action times")
        ny = ZoneInfo("America/New_York")
        for nt in nts[:3]:
            local = nt.astimezone(ny)
            expect_eq(
                (local.hour, local.minute),
                (12, 0),
                f"{nt.isoformat()} should be 12:00 NY",
            )
    finally:
        await c.cleanup(h)


@test("spec.jitter_perturbs_scheduled_times")
async def t_jitter(c: SchedClient) -> None:
    """With jitter set, action scheduled times are offset off their nominal interval boundary
    (nominal times for a 2s interval land on even seconds at .000)."""
    spec = ScheduleSpec(
        intervals=[ScheduleIntervalSpec(every=dt.timedelta(seconds=2))],
        jitter=dt.timedelta(seconds=1),
    )
    h = await c.create(
        sid("jitter"), overlap=ScheduleOverlapPolicy.ALLOW_ALL, spec=spec
    )
    try:
        expect(
            await poll_until(lambda: _at_least_actions(c, h, 3), timeout=20),
            "no actions",
        )
        recent = await c.recent(h)
        perturbed = [
            r
            for r in recent
            if r.scheduled_at.second % 2 != 0 or r.scheduled_at.microsecond != 0
        ]
        expect(
            len(perturbed) >= 1,
            "jitter should perturb some scheduled times off the boundary",
        )
    finally:
        await c.cleanup(h)


@test("combo.limited_actions_not_consumed_by_skip")
async def t_limited_x_skip(c: SchedClient) -> None:
    """An overlap-SKIP-skipped occurrence must not consume the limited_actions budget: a
    remaining_actions=2 schedule whose action outlives its interval still takes exactly 2 *actual*
    actions, with the intervening occurrences recorded as skips."""
    h = await c.create(
        sid("limskip"),
        every=1.0,
        workflow="sleeper",
        arg=3.0,
        overlap=ScheduleOverlapPolicy.SKIP,
        remaining_actions=2,
    )
    try:
        expect(
            await poll_until(lambda: _at_least_actions(c, h, 2), timeout=15),
            "expected 2 actions",
        )
        await asyncio.sleep(4)
        desc = await h.describe()
        expect_eq(
            desc.info.num_actions,
            2,
            "skips must not consume the limited-actions budget",
        )
        expect(
            desc.info.num_actions_skipped_overlap >= 1,
            "expected some skipped occurrences during the run",
        )
    finally:
        await c.cleanup(h)


@test("searchattr.scheduled_workflow_queryable_by_schedule_id")
async def t_search_attr(c: SchedClient) -> None:
    """Workflows started by a schedule carry the TemporalScheduledById search attribute, so they can
    be found via a visibility query for that schedule id (a documented product capability)."""
    s = sid("saq")
    h = await c.create(s, every=1.0)
    try:
        expect(
            await poll_until(lambda: _at_least_actions(c, h, 1), timeout=15),
            "no actions",
        )
        found = await poll_until(lambda: _sa_query_nonempty(c, s), timeout=15)
        expect(found, f"expected workflows returned by TemporalScheduledById = '{s}'")
    finally:
        await c.cleanup(h)


async def _sa_query_nonempty(c: SchedClient, schedule_id: str) -> bool:
    async for _ in c.client.list_workflows(
        query=f"TemporalScheduledById = '{schedule_id}'"
    ):
        return True
    return False


@test("pause_on_failure.disabled_keeps_running")
async def t_pof_disabled(c: SchedClient) -> None:
    """Without pause_on_failure, a repeatedly-failing action does not pause the schedule; it keeps
    taking (failing) actions."""
    h = await c.create(
        sid("pofoff"),
        every=1.0,
        workflow="boom",
        max_attempts=1,
        pause_on_failure=False,
    )
    try:
        expect(
            await poll_until(lambda: _at_least_actions(c, h, 2), timeout=20),
            "schedule should keep firing despite failures",
        )
        expect(
            not (await h.describe()).schedule.state.paused,
            "schedule must not auto-pause",
        )
    finally:
        await c.cleanup(h)


# ---- chaos: concurrent knobs used together -------------------------------
@test("chaos.rpc_storm_keeps_schedule_consistent")
async def t_rpc_storm(c: SchedClient) -> None:
    """Hammer one live schedule with concurrent pause/unpause/trigger/update/backfill/describe. No
    op should return a server-internal (dirty) error, and afterwards the schedule must still be
    describable and resume taking actions once unpaused."""
    now = _utcnow()
    h = await c.create(sid("storm"), every=1.0, overlap=ScheduleOverlapPolicy.ALLOW_ALL)

    async def note_update() -> None:
        async def m(inp: ScheduleUpdateInput) -> ScheduleUpdate:
            s = inp.description.schedule
            s.state.note = "storm"
            return ScheduleUpdate(schedule=s)

        await h.update(m)

    def a_backfill():
        return h.backfill(
            ScheduleBackfill(
                start_at=now - dt.timedelta(seconds=5),
                end_at=now - dt.timedelta(seconds=1),
                overlap=ScheduleOverlapPolicy.BUFFER_ALL,
            )
        )

    try:
        ops: list[Awaitable[Any]] = []
        for _ in range(6):
            ops += [
                h.pause(),
                h.unpause(),
                h.trigger(overlap=ScheduleOverlapPolicy.ALLOW_ALL),
                h.describe(),
                note_update(),
                a_backfill(),
            ]
        results = await asyncio.gather(*ops, return_exceptions=True)
        assert_clean(results, "rpc storm")
        await h.unpause()
        n1 = await c.num_actions(h)
        expect(
            await poll_until(lambda: _at_least_actions(c, h, n1 + 1), timeout=15),
            "schedule should still take actions after the storm",
        )
    finally:
        await c.cleanup(h)


@test("chaos.concurrent_backfills_dedup_no_errors")
async def t_concurrent_backfills(c: SchedClient) -> None:
    """Several concurrent backfills of the same past window produce no dirty errors and dedup by
    nominal time (one run per second-boundary), leaving the schedule describable."""
    now = _utcnow()
    h = await c.create(
        sid("cbf"), every=1.0, paused=True, overlap=ScheduleOverlapPolicy.BUFFER_ALL
    )
    try:
        bf = [
            h.backfill(
                ScheduleBackfill(
                    start_at=now - dt.timedelta(seconds=10),
                    end_at=now - dt.timedelta(seconds=1),
                    overlap=ScheduleOverlapPolicy.BUFFER_ALL,
                )
            )
            for _ in range(4)
        ]
        assert_clean(
            await asyncio.gather(*bf, return_exceptions=True), "concurrent backfills"
        )
        expect(
            await poll_until(lambda: _at_least_actions(c, h, 3), timeout=20),
            "no backfill actions",
        )
        n = await c.num_actions(h)
        expect(
            n <= 12,
            f"4x backfill of a ~10s window should dedup, not multiply (got {n})",
        )
    finally:
        await c.cleanup(h)


@test("combo.backfill_runs_while_paused")
async def t_backfill_while_paused(c: SchedClient) -> None:
    """An explicit backfill executes even while the schedule is paused, and does not clear the
    pause: automatic actions stay suppressed but the requested past actions run."""
    now = _utcnow()
    h = await c.create(
        sid("bfpaused"),
        every=1.0,
        paused=True,
        overlap=ScheduleOverlapPolicy.BUFFER_ALL,
    )
    try:
        await h.backfill(
            ScheduleBackfill(
                start_at=now - dt.timedelta(seconds=6),
                end_at=now - dt.timedelta(seconds=1),
                overlap=ScheduleOverlapPolicy.BUFFER_ALL,
            )
        )
        expect(
            await poll_until(lambda: _at_least_actions(c, h, 3), timeout=20),
            "backfill should run even while paused",
        )
        expect(
            (await h.describe()).schedule.state.paused, "schedule must remain paused"
        )
        # Wait for the backfill to finish draining, then confirm the count no longer grows —
        # a paused schedule must take no *automatic* actions.
        n = await _stabilized_count(c, h)
        await asyncio.sleep(3)
        expect_eq(
            await c.num_actions(h), n, "no automatic actions should occur while paused"
        )
    finally:
        await c.cleanup(h)


async def _stabilized_count(
    c: SchedClient, h: ScheduleHandle, timeout: float = 25
) -> int:
    """Return num_actions once it stops increasing across a 2s window (backfill fully drained)."""
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout
    prev = await c.num_actions(h)
    while loop.time() < deadline:
        await asyncio.sleep(2)
        cur = await c.num_actions(h)
        if cur == prev:
            return cur
        prev = cur
    return prev


@test("chaos.many_schedules_fire_independently")
async def t_many_schedules(c: SchedClient) -> None:
    """A dozen schedules created concurrently each fire on their own cadence without cross-talk."""
    handles = await asyncio.gather(
        *[c.create(sid("multi"), every=1.0) for _ in range(12)]
    )
    try:
        await asyncio.sleep(6)
        counts = await asyncio.gather(*[c.num_actions(h) for h in handles])
        expect(
            all(n >= 1 for n in counts), f"every schedule should fire (counts={counts})"
        )
    finally:
        await asyncio.gather(*[c.cleanup(h) for h in handles])


@test("combo.buffer_overrun_survives")
async def t_buffer_overrun(c: SchedClient) -> None:
    """A large backfill of actions slower than the interval (BUFFER_ALL) overruns the action buffer;
    the schedule must survive it — remain describable and keep an action running — rather than wedge
    or error."""
    now = _utcnow()
    h = await c.create(
        sid("overrun"),
        every=1.0,
        workflow="sleeper",
        arg=30.0,
        paused=True,
        overlap=ScheduleOverlapPolicy.BUFFER_ALL,
    )
    try:
        await h.backfill(
            ScheduleBackfill(
                start_at=now - dt.timedelta(seconds=300),
                end_at=now - dt.timedelta(seconds=1),
                overlap=ScheduleOverlapPolicy.BUFFER_ALL,
            )
        )
        expect(
            await poll_until(lambda: _running_at_least(c, h, 1), timeout=20),
            "overrun backfill should still start draining actions",
        )
        desc = await h.describe()  # must not error
        expect(
            desc.info is not None, "schedule should remain describable after overrun"
        )
    finally:
        await c.cleanup(h)


def _utcnow() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


# ==========================================================================
# Runner
# ==========================================================================
def completed_results(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    if path.exists():
        for ln in path.read_text().splitlines():
            m = re.search(r"RESULT (\S+) (PASS|FAIL|ERROR)\b", ln)
            if m:
                out[m.group(1)] = m.group(2)
    return out


def next_bug_number() -> int:
    BUGS_DIR.mkdir(parents=True, exist_ok=True)
    n = 0
    for f in BUGS_DIR.glob("*.md"):
        m = re.match(r"(\d+)-", f.name)
        if m:
            n = max(n, int(m.group(1)))
    return n + 1


def write_bug(t: Test, status: str, err: str, c: SchedClient) -> Path:
    num = next_bug_number()
    path = BUGS_DIR / f"{num:02d}-{t.id.replace('.', '-')}.md"
    path.write_text(
        "\n".join(
            [
                f"# {t.id} — {status}",
                "",
                f"- when: {dt.datetime.now().isoformat(timespec='seconds')}",
                f"- doc: {t.doc}",
                "",
                "## Failure",
                "```",
                err.strip(),
                "```",
                "",
                "## Ops",
                "```",
                "\n".join(c.ops) or "(none)",
                "```",
                "",
            ]
        )
    )
    return path


async def _connect_with_retry(timeout: float = 30) -> Client:
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout
    last: Optional[Exception] = None
    while loop.time() < deadline:
        try:
            return await Client.connect(ADDRESS, namespace=NAMESPACE)
        except Exception as e:  # noqa: BLE001
            last = e
            await asyncio.sleep(0.5)
    raise RuntimeError(
        f"could not connect to {ADDRESS} (is the server running?): {last}"
    )


async def run_suite(args: argparse.Namespace) -> int:
    if args.fresh and LOG_PATH.exists():
        LOG_PATH.unlink()
    prior = completed_results(LOG_PATH)
    only = set(args.only.split(",")) if args.only else None
    selected = [
        t
        for t in REGISTRY
        if (not only or t.id in only)
        and not (args.rerun_failed and prior.get(t.id) == "PASS")
        and not (not args.rerun_failed and not only and t.id in prior)
    ]

    log = open(LOG_PATH, "a", buffering=1)

    def line(s: str) -> None:
        ts = dt.datetime.now().strftime("%H:%M:%S")
        log.write(f"[{ts}] {s}\n")
        print(f"[{ts}] {s}", flush=True)

    line(f"=== scheduler SDK run: {len(selected)} selected ({len(REGISTRY)} total) ===")
    worker = Worker()
    passed = failed = errored = 0
    try:
        client = await _connect_with_retry()
        line(f"connected to {ADDRESS}")
        worker.start()
        await worker.wait_ready(client)
        line("worker ready")

        for t in selected:
            c = SchedClient(client)
            line(f"RUN  {t.id}")
            try:
                await t.fn(c)
            except TestFailure as e:
                failed += 1
                line(f"RESULT {t.id} FAIL -> {write_bug(t, 'FAIL', str(e), c).name}")
            except Exception:
                errored += 1
                line(
                    f"RESULT {t.id} ERROR -> {write_bug(t, 'ERROR', traceback.format_exc(), c).name}"
                )
            else:
                passed += 1
                line(f"RESULT {t.id} PASS")
    finally:
        worker.stop()
        line(f"=== done: {passed} passed, {failed} failed, {errored} errored ===")
        log.close()
    return 1 if (failed or errored) else 0


def main() -> int:
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd")
    sub.add_parser("worker")
    p.add_argument("--only")
    p.add_argument("--fresh", action="store_true")
    p.add_argument("--rerun-failed", action="store_true")
    p.add_argument("--list", action="store_true")
    args = p.parse_args()

    if args.cmd == "worker":
        run_worker()
        return 0
    if args.list:
        for t in REGISTRY:
            print(t.id)
        return 0
    return asyncio.run(run_suite(args))


if __name__ == "__main__":
    sys.exit(main())
