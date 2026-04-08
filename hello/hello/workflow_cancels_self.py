import asyncio
import traceback

from temporalio import workflow
from temporalio.client import Client, WorkflowFailureError
from temporalio.envconfig import ClientConfig
from temporalio.exceptions import CancelledError
from temporalio.worker import Worker


@workflow.defn
class SelfCancellingWorkflow:
    @workflow.run
    async def run(self) -> str:
        # Do some work, then decide to cancel ourselves
        await asyncio.sleep(1)
        raise CancelledError("I changed my mind")


async def main():
    import logging
    logging.basicConfig(level=logging.INFO)

    config = ClientConfig.load_client_connect_config()
    config.setdefault("target_host", "localhost:7233")
    client = await Client.connect(**config)

    async with Worker(
        client,
        task_queue="self-cancel-demo-tq",
        workflows=[SelfCancellingWorkflow],
    ):
        try:
            await client.execute_workflow(
                SelfCancellingWorkflow.run,
                id="workflow-cancels-self-demo",
                task_queue="self-cancel-demo-tq",
            )
        except WorkflowFailureError:
            print("Workflow self-cancelled:")
            print(traceback.format_exc())


if __name__ == "__main__":
    asyncio.run(main())
