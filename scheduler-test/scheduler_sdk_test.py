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

from temporalio.client import (
    Client,
    Schedule,
    ScheduleActionStartWorkflow,
    ScheduleHandle,
    ScheduleIntervalSpec,
    ScheduleOverlapPolicy,
    SchedulePolicy,
    ScheduleSpec,
    ScheduleState,
    ScheduleUpdate,
    ScheduleUpdateInput,
)
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
        self, wf_id: str, *, workflow: str = "noop", arg: Any = None
    ) -> ScheduleActionStartWorkflow:
        kw: dict[str, Any] = {"id": wf_id, "task_queue": TASK_QUEUE}
        if arg is not None:
            kw["arg"] = arg
        return ScheduleActionStartWorkflow(workflow, **kw)

    async def create(
        self,
        sched_id: str,
        *,
        every: float = 1.0,
        workflow: str = "noop",
        arg: Any = None,
        paused: bool = False,
        overlap: ScheduleOverlapPolicy = ScheduleOverlapPolicy.SKIP,
        trigger_immediately: bool = False,
    ) -> ScheduleHandle:
        self._log(
            f"create {sched_id} every={every} wf={workflow} paused={paused} overlap={overlap.name}"
        )
        return await self.client.create_schedule(
            sched_id,
            Schedule(
                action=self.action(f"{sched_id}-wf", workflow=workflow, arg=arg),
                spec=ScheduleSpec(
                    intervals=[ScheduleIntervalSpec(every=dt.timedelta(seconds=every))]
                ),
                policy=SchedulePolicy(overlap=overlap),
                state=ScheduleState(paused=paused),
            ),
            trigger_immediately=trigger_immediately,
        )

    async def num_actions(self, handle: ScheduleHandle) -> int:
        return (await handle.describe()).info.num_actions

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


def run_worker() -> None:
    from temporalio.worker import UnsandboxedWorkflowRunner, Worker

    async def main() -> None:
        client = await Client.connect(ADDRESS, namespace=NAMESPACE)
        async with Worker(
            client,
            task_queue=TASK_QUEUE,
            workflows=[Noop, Sleeper],
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
