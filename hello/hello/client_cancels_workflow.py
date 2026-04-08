import asyncio
import traceback

from temporalio import workflow
from temporalio.client import Client, WorkflowFailureError
from temporalio.envconfig import ClientConfig
from temporalio.worker import Worker


@workflow.defn
class LongRunningWorkflow:
    @workflow.run
    async def run(self) -> str:
        # Simulate long-running work with a timer
        await asyncio.sleep(3600)
        return "done"


async def main():
    import logging
    logging.basicConfig(level=logging.INFO)

    config = ClientConfig.load_client_connect_config()
    config.setdefault("target_host", "localhost:7233")
    client = await Client.connect(**config)

    async with Worker(
        client,
        task_queue="cancel-workflow-demo-tq",
        workflows=[LongRunningWorkflow],
    ):
        handle = await client.start_workflow(
            LongRunningWorkflow.run,
            id="client-cancels-workflow-demo",
            task_queue="cancel-workflow-demo-tq",
        )
        # Let it start, then cancel from the client
        await asyncio.sleep(1)
        await handle.cancel()

        try:
            await handle.result()
        except WorkflowFailureError:
            print("Workflow cancelled by client:")
            print(traceback.format_exc())


if __name__ == "__main__":
    asyncio.run(main())
