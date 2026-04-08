"""Can a workflow-attached activity cancel itself by raising CancelledError
without a prior cancel request from the workflow?"""

import asyncio
import traceback
from datetime import timedelta

from temporalio import activity, workflow
from temporalio.client import Client, WorkflowFailureError
from temporalio.envconfig import ClientConfig
from temporalio.exceptions import CancelledError
from temporalio.worker import Worker


@activity.defn
async def self_cancelling_activity() -> str:
    # No cancel was requested — just raise CancelledError unilaterally
    raise CancelledError("I decided to cancel myself")


@workflow.defn
class RunSelfCancellingActivity:
    @workflow.run
    async def run(self) -> str:
        return await workflow.execute_activity(
            self_cancelling_activity,
            start_to_close_timeout=timedelta(seconds=10),
        )


async def main():
    import logging
    logging.basicConfig(level=logging.INFO)

    config = ClientConfig.load_client_connect_config()
    config.setdefault("target_host", "localhost:7233")
    client = await Client.connect(**config)

    async with Worker(
        client,
        task_queue="activity-self-cancel-tq",
        workflows=[RunSelfCancellingActivity],
        activities=[self_cancelling_activity],
    ):
        try:
            result = await client.execute_workflow(
                RunSelfCancellingActivity.run,
                id="activity-self-cancel-demo",
                task_queue="activity-self-cancel-tq",
            )
            print(f"Result: {result}")
        except WorkflowFailureError:
            print("Workflow failed:")
            print(traceback.format_exc())

    # Show what happened
    handle = client.get_workflow_handle("activity-self-cancel-demo")
    resp = await handle.describe()
    print(f"\nWorkflow status: {resp.status}")

    async for event in handle.fetch_history_events():
        print(f"  {event.event_id:>3}  {event.event_type.name}")


if __name__ == "__main__":
    asyncio.run(main())
