import asyncio
from datetime import timedelta

from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.envconfig import ClientConfig
from temporalio.exceptions import ActivityError, CancelledError
from temporalio.worker import Worker


@activity.defn
async def slow_activity() -> str:
    # Heartbeat so the worker can detect cancellation
    for i in range(100):
        activity.heartbeat(i)
        await asyncio.sleep(0.1)
    return "done"


@workflow.defn
class CancelActivityWorkflow:
    @workflow.run
    async def run(self) -> str:
        # Start the activity as a task so we can cancel it
        task = asyncio.ensure_future(
            workflow.execute_activity(
                slow_activity,
                start_to_close_timeout=timedelta(seconds=30),
                heartbeat_timeout=timedelta(seconds=5),
            )
        )
        # Let it run briefly, then cancel from workflow code
        await asyncio.sleep(1)
        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, CancelledError, ActivityError):
            return "activity was cancelled by workflow"
        return "activity completed unexpectedly"


async def main():
    import logging
    logging.basicConfig(level=logging.INFO)

    config = ClientConfig.load_client_connect_config()
    config.setdefault("target_host", "localhost:7233")
    client = await Client.connect(**config)

    async with Worker(
        client,
        task_queue="cancel-demo-tq",
        workflows=[CancelActivityWorkflow],
        activities=[slow_activity],
    ):
        result = await client.execute_workflow(
            CancelActivityWorkflow.run,
            id="cancel-activity-demo",
            task_queue="cancel-demo-tq",
        )
        print(f"Result: {result}")


if __name__ == "__main__":
    asyncio.run(main())
