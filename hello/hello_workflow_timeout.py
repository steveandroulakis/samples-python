# UNOFFICIAL SAMPLE, CONTACT STEVE ANDROULAKIS FOR QUESTIONS
import asyncio
from dataclasses import dataclass
from datetime import timedelta
from typing import Optional

from temporalio import workflow, activity
from temporalio.common import RetryPolicy
from temporalio.client import Client
from temporalio.worker import Worker
from temporalio.exceptions import CancelledError, ApplicationError

# Activity definitions
@dataclass
class WorkInput:
    task_name: str
    steps: int

@activity.defn
async def perform_work(input: WorkInput) -> str:
    """Activity that simulates doing work with progress updates."""
    try:
        print(f"Starting work on {input.task_name}, attempt {activity.info().attempt}")
        
        for i in range(input.steps):
            await asyncio.sleep(2)  # Simulate work
            print(f"Progress: {(i+1)/input.steps*100}%")
            
        return f"Completed {input.task_name} successfully"
    except asyncio.CancelledError:
        print(f"\nActivity cancelled at step {i+1}")
        raise

# Workflow definitions
@workflow.defn
class TimerConstrainedWorkflow:
    def __init__(self) -> None:
        self._cancelled = False
        
    @workflow.run
    async def run(self, max_duration_seconds: int, task_name: str) -> str:
        """
        Runs a workflow with a maximum time constraint.
        """
        if max_duration_seconds <= 0:
            raise ApplicationError("max_duration_seconds must be positive")
            
        try:
            timer_task = asyncio.create_task(asyncio.sleep(max_duration_seconds))
            
            # Execute the activity with retry policy
            work_task = asyncio.create_task(
                workflow.execute_activity(
                    perform_work,
                    WorkInput(task_name, steps=5),
                    start_to_close_timeout=timedelta(seconds=max_duration_seconds),
                    retry_policy=RetryPolicy(
                        initial_interval=timedelta(seconds=1),
                        maximum_interval=timedelta(seconds=5),
                        maximum_attempts=3
                    )
                )
            )
            
            done, pending = await asyncio.wait(
                [timer_task, work_task],
                return_when=asyncio.FIRST_COMPLETED
            )
            
            # Cancel pending tasks
            for task in pending:
                task.cancel()
                try:
                    await task
                except (asyncio.CancelledError, CancelledError):
                    pass
            
            if timer_task in done:
                return f"Workflow timed out after {max_duration_seconds} seconds"
            else:
                return await work_task
                
        except CancelledError:
            self._cancelled = True
            raise

    @workflow.query
    def is_cancelled(self) -> bool:
        return self._cancelled

@workflow.defn
class MainWorkflow:
    @workflow.run
    async def run(
        self,
        max_duration_seconds: int,
        task_name: str,
        child_workflow_id: Optional[str] = None
    ) -> str:
        workflow_id = child_workflow_id or "constrained-workflow"
        
        result = await workflow.execute_child_workflow(
            TimerConstrainedWorkflow.run,
            args=(max_duration_seconds, task_name),
            id=workflow_id
        )
        
        return f"Main workflow completed. Child result: {result}"

async def main():
    # Create client
    client = await Client.connect("localhost:7233")

    # Set up and run worker
    async with Worker(
        client,
        task_queue="timer-constraint-task-queue",
        workflows=[MainWorkflow, TimerConstrainedWorkflow],
        activities=[perform_work],
    ):
        print("Worker started, executing workflow...")
        
        try:
            # Execute workflow with different timeout scenarios
            
            # Scenario 1: Enough time to complete
            print("\nExecuting Scenario 1: Enough time to complete (15s)...")
            result1 = await client.execute_workflow(
                MainWorkflow.run,
                args=(15, "Long-running task"),
                id="workflow-with-time-1",
                task_queue="timer-constraint-task-queue",
            )
            print(f"Scenario 1 (15s timeout) Result: {result1}")
            
            # Wait a moment before starting next scenario
            await asyncio.sleep(2)
            
            # Scenario 2: Not enough time to complete
            print("\nExecuting Scenario 2: Not enough time to complete (5s)...")
            result2 = await client.execute_workflow(
                MainWorkflow.run,
                args=(5, "Time-constrained task"),
                id="workflow-with-time-2",
                task_queue="timer-constraint-task-queue",
            )
            print(f"Scenario 2 (5s timeout) Result: {result2}")
            
        except Exception as e:
            print(f"Client returned: {e}")

if __name__ == "__main__":
    asyncio.run(main())