import asyncio
import logging
from dataclasses import dataclass
from datetime import timedelta

from enum import Enum
from typing import Dict

from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.worker import Worker

from typing import List, Tuple

# logging.basicConfig(level=logging.INFO)

# While we could use multiple parameters in the activity, Temporal strongly
# encourages using a single dataclass instead which can have fields added to it
# in a backwards-compatible way.
@dataclass
class ComposeGreetingInput:
    greeting: str
    name: str

class TaskStatus(Enum):
    NOT_STARTED = "NOT_STARTED"
    PENDING = "PENDING"
    COMPLETED = "COMPLETED"

@dataclass
class TaskInfo:
    id: int
    status: TaskStatus
    result: str = ""

# Basic activity that logs and does string concatenation
@activity.defn
async def compose_greeting(input: ComposeGreetingInput) -> str:
    print("Sleeping for 0.2 seconds")
    await asyncio.sleep(0.2)
    activity.logger.info("Running activity with parameter %s" % input)
    return f"{input.greeting}, {input.name}!"


# Basic workflow that logs and invokes an activity
@workflow.defn
class GreetingWorkflow:
    @workflow.run
    async def run(self, name: str) -> str:
        workflow.logger.info("Running workflow with parameter %s" % name)
        
        # Initialize task tracking
        tasks: Dict[int, TaskInfo] = {
            i: TaskInfo(id=i, status=TaskStatus.NOT_STARTED)
            for i in range(10)
        }
        
        # Create timer task
        max_duration_seconds = 1
        timer_task = asyncio.create_task(asyncio.sleep(max_duration_seconds))
        
        # Process tasks sequentially
        for i in range(10):
            # Update task status to pending
            tasks[i].status = TaskStatus.PENDING
            workflow.logger.info(f"Starting activity {i}")
            
            # Create and start one task
            work_task = asyncio.create_task(workflow.execute_activity(
                "compose_greeting",
                ComposeGreetingInput("Hello", name),
                start_to_close_timeout=timedelta(seconds=10),
            ))
            
            # Wait for either the current task or timer
            done, pending = await asyncio.wait(
                {timer_task, work_task},
                return_when=asyncio.FIRST_COMPLETED
            )
            
            # Check if timer completed
            if timer_task in done:
                workflow.logger.info("Timer task completed")
                break
            
            # If we get here, the work task completed
            tasks[i].status = TaskStatus.COMPLETED
            try:
                tasks[i].result = work_task.result()
            except Exception as e:
                tasks[i].result = f"Error: {str(e)}"
            workflow.logger.info(f"Completed activity {i}")
        
        # Print final status report
        not_started = [t.id for t in tasks.values() if t.status == TaskStatus.NOT_STARTED]
        pending_tasks = [t.id for t in tasks.values() if t.status == TaskStatus.PENDING]
        completed = [t.id for t in tasks.values() if t.status == TaskStatus.COMPLETED]
        
        workflow.logger.error("Final Status Report:")
        workflow.logger.error(f"Not Started Activities: {not_started}")
        workflow.logger.error(f"Pending Activities: {pending_tasks}")
        workflow.logger.error(f"Completed Activities: {completed}")
        
        if timer_task in pending_tasks:
            workflow.logger.error("Timer task is still pending")

        return "Workflow complete"
        


async def main():
    # Uncomment the line below to see logging
    # logging.basicConfig(level=logging.INFO)

    # Start client
    client = await Client.connect("localhost:7233")

    # Run a worker for the workflow
    async with Worker(
        client,
        task_queue="hello-activity-task-queue",
        workflows=[GreetingWorkflow],
        activities=[compose_greeting],
    ):

        # While the worker is running, use the client to run the workflow and
        # print out its result. Note, in many production setups, the client
        # would be in a completely separate process from the worker.
        result = await client.execute_workflow(
            GreetingWorkflow.run,
            "World",
            id="hello-activity-workflow-id",
            task_queue="hello-activity-task-queue",
        )
        print(f"Result: {result}")


if __name__ == "__main__":
    asyncio.run(main())
