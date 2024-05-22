import asyncio
import concurrent.futures
import logging
from temporalio.client import Client
from temporalio.worker import Worker

from activities import prompt_bedrock
from workflows import SignalQueryBedrockWorkflow


async def main():
    # Create client connected to server at the given address
    client = await Client.connect("localhost:7233")

    # Run the worker
    with concurrent.futures.ThreadPoolExecutor(max_workers=100) as activity_executor:
        worker = Worker(
            client,
            task_queue="bedrock-task-queue",
            workflows=[SignalQueryBedrockWorkflow],
            activities=[prompt_bedrock],
            activity_executor=activity_executor,
            max_concurrent_activities=100,
        )
        await worker.run()

if __name__ == "__main__":
    print("Starting worker")
    print("Then run 'python send_message.py \"<prompt>\"'")

    logging.basicConfig(level=logging.INFO)

    asyncio.run(main())
