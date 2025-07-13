#!/usr/bin/env python3
"""Test script to run 2 workers simultaneously and test research workflow."""

import asyncio
import subprocess
import sys
import time
from datetime import timedelta
from pathlib import Path

from temporalio.client import Client
from temporalio.contrib.pydantic import pydantic_data_converter

from openai_agents.workflows.research_bot_workflow import ResearchWorkflow


async def run_research_workflow():
    """Run the research workflow that originally caused the MockValSer error."""
    print("🔬 Starting research workflow...")
    
    client = await Client.connect(
        "localhost:7233",
        data_converter=pydantic_data_converter,
    )
    
    try:
        result = await client.execute_workflow(
            ResearchWorkflow.run,
            "What are the latest developments in AI agents and how do they compare to traditional chatbots?",
            id=f"research-workflow-test-{int(time.time())}",
            task_queue="openai-agents-task-queue",
            execution_timeout=timedelta(minutes=5),
        )
        print(f"✅ Research workflow completed successfully!")
        print(f"📝 Result: {result[:200]}..." if len(str(result)) > 200 else f"📝 Result: {result}")
        return True
    except Exception as e:
        print(f"❌ Research workflow failed with error: {e}")
        if "MockValSer" in str(e):
            print("🔍 MockValSer error detected - this is the issue we're trying to fix!")
        return False


def start_worker(worker_id: int):
    """Start a single worker process."""
    print(f"🚀 Starting worker {worker_id}...")
    
    # Use python -m to run the worker module
    cmd = [
        sys.executable, "-m", "openai_agents.run_worker"
    ]
    
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
        universal_newlines=True
    )
    
    return process


async def test_dual_workers():
    """Test running 2 workers simultaneously with research workflow."""
    print("🧪 Testing dual workers with research workflow...")
    print("=" * 60)
    
    # Start 2 worker processes
    worker1 = start_worker(1)
    worker2 = start_worker(2)
    
    workers = [worker1, worker2]
    
    try:
        # Give workers time to start up and register
        print("⏳ Waiting for workers to start up...")
        await asyncio.sleep(5)
        
        # Check if workers are still running
        for i, worker in enumerate(workers, 1):
            if worker.poll() is not None:
                stdout, stderr = worker.communicate()
                print(f"❌ Worker {i} died during startup!")
                print(f"stdout: {stdout}")
                print(f"stderr: {stderr}")
                return False
        
        print("✅ Both workers appear to be running")
        
        # Run the research workflow
        success = await run_research_workflow()
        
        if success:
            print("🎉 Test passed! Dual workers successfully handled research workflow")
        else:
            print("💥 Test failed! Research workflow failed with dual workers")
            
        return success
        
    finally:
        # Clean up worker processes
        print("🧹 Cleaning up worker processes...")
        for i, worker in enumerate(workers, 1):
            if worker.poll() is None:  # Still running
                print(f"🔪 Terminating worker {i}...")
                worker.terminate()
                try:
                    worker.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    print(f"⚡ Force killing worker {i}...")
                    worker.kill()
                    worker.wait()


async def main():
    """Main test function."""
    print("🔧 Testing ModelActivity fix for dual workers")
    print("=" * 60)
    print(f"📁 Working directory: {Path.cwd()}")
    print(f"🐍 Python executable: {sys.executable}")
    print()
    
    try:
        success = await test_dual_workers()
        
        if success:
            print("\n✅ ALL TESTS PASSED! The ModelActivity fix resolved the issue.")
            sys.exit(0)
        else:
            print("\n❌ TESTS FAILED! The issue may still exist.")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n⛔ Test interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n💥 Test failed with unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())