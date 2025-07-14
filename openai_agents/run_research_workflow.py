import asyncio
import argparse
import sys
from typing import Dict, List

from temporalio.client import Client, WorkflowHandle
from temporalio.contrib.pydantic import pydantic_data_converter

from openai_agents.workflows.research_bot_workflow import ResearchWorkflow
from openai_agents.workflows.research_agents.research_models import (
    ClarificationInput,
    ResearchInteraction,
)


async def run_basic_research(client: Client, query: str, workflow_id: str):
    """Run basic research without clarifications"""
    print(f"🔍 Starting basic research: {query}")
    
    result = await client.execute_workflow(
        ResearchWorkflow.run,
        args=[query, False],  # query, use_clarifications=False
        id=workflow_id,
        task_queue="openai-agents-task-queue",
    )
    
    print(f"\n📄 Research Result:")
    print("=" * 60)
    print(result)
    return result


async def run_interactive_research(client: Client, query: str, workflow_id: str):
    """Run interactive research with clarifying questions"""
    print(f"🤖 Starting interactive research: {query}")
    
    # Start the workflow
    handle = await client.start_workflow(
        ResearchWorkflow.run,
        args=[query, True],  # query, use_clarifications=True
        id=workflow_id,
        task_queue="openai-agents-task-queue",
    )
    
    print(f"✅ Workflow started with ID: {workflow_id}")
    
    # Monitor for clarifications
    while True:
        try:
            status = await handle.query(ResearchWorkflow.get_status)
            
            if not status:
                await asyncio.sleep(1)
                continue
                
            print(f"📊 Status: {status.status}")
            
            if status.status == "awaiting_clarifications":
                print(f"\n❓ Clarifying questions needed:")
                print("-" * 40)
                
                # Display questions and collect responses
                responses = {}
                questions = status.clarification_questions or []
                for i, question in enumerate(questions):
                    print(f"{i+1}. {question}")
                    answer = input(f"   Answer: ").strip()
                    responses[f"question_{i}"] = answer if answer else "No specific preference"
                
                # Send clarification responses
                print(f"\n📤 Sending clarification responses...")
                await handle.execute_update(
                    ResearchWorkflow.provide_clarifications,
                    ClarificationInput(responses=responses)
                )
                print(f"✅ Clarifications sent, continuing research...")
                
            elif status.status == "completed":
                print(f"\n🎉 Research completed!")
                result = await handle.result()
                print(f"\n📄 Research Result:")
                print("=" * 60)
                print(result)
                return result
                
            elif status.status in ["pending", "clarifications_received"]:
                print(f"⏳ Research in progress...")
                
            await asyncio.sleep(2)
            
        except Exception as e:
            print(f"❌ Error monitoring workflow: {e}")
            await asyncio.sleep(2)


async def get_workflow_status(client: Client, workflow_id: str):
    """Get the status of an existing workflow"""
    try:
        handle = client.get_workflow_handle(workflow_id)
        status = await handle.query(ResearchWorkflow.get_status)
        
        if status:
            print(f"📊 Workflow {workflow_id} status: {status.status}")
            if status.clarification_questions:
                print(f"❓ Pending questions: {len(status.clarification_questions)}")
            if status.final_result:
                print(f"✅ Has final result")
        else:
            print(f"❌ No status available for workflow {workflow_id}")
            
    except Exception as e:
        print(f"❌ Error getting workflow status: {e}")


async def send_clarifications(client: Client, workflow_id: str, responses: Dict[str, str]):
    """Send clarification responses to an existing workflow"""
    try:
        handle = client.get_workflow_handle(workflow_id)
        result = await handle.execute_update(
            ResearchWorkflow.provide_clarifications,
            ClarificationInput(responses=responses)
        )
        print(f"✅ Clarifications sent to workflow {workflow_id}")
        print(f"📊 Updated status: {result.status}")
        
    except Exception as e:
        print(f"❌ Error sending clarifications: {e}")


def parse_clarifications(clarification_args: List[str]) -> Dict[str, str]:
    """Parse clarification responses from command line arguments"""
    responses = {}
    for arg in clarification_args:
        if "=" in arg:
            key, value = arg.split("=", 1)
            responses[key] = value
    return responses


async def main():
    parser = argparse.ArgumentParser(description="OpenAI Research Workflow CLI")
    parser.add_argument("query", nargs="?", help="Research query")
    parser.add_argument("--interactive", "-i", action="store_true", 
                       help="Use interactive mode with clarifying questions")
    parser.add_argument("--workflow-id", default="research-workflow", 
                       help="Workflow ID (default: research-workflow)")
    parser.add_argument("--status", action="store_true",
                       help="Get status of existing workflow")
    parser.add_argument("--clarify", nargs="+", metavar="KEY=VALUE",
                       help="Send clarification responses (e.g., --clarify question_0='travel budget' question_1='March')")
    
    args = parser.parse_args()
    
    # Create client
    try:
        client = await Client.connect(
            "localhost:7233",
            data_converter=pydantic_data_converter,
        )
        print(f"🔗 Connected to Temporal server")
    except Exception as e:
        print(f"❌ Failed to connect to Temporal server: {e}")
        print(f"   Make sure Temporal server is running on localhost:7233")
        return
    
    # Handle different modes
    if args.status:
        await get_workflow_status(client, args.workflow_id)
        
    elif args.clarify:
        responses = parse_clarifications(args.clarify)
        await send_clarifications(client, args.workflow_id, responses)
        
    elif args.query:
        if args.interactive:
            await run_interactive_research(client, args.query, args.workflow_id)
        else:
            await run_basic_research(client, args.query, args.workflow_id)
            
    else:
        # Interactive query input
        print("🔍 OpenAI Research Workflow")
        print("=" * 40)
        query = input("Enter your research query: ").strip()
        
        if not query:
            print("❌ Query cannot be empty")
            return
            
        mode = input("Use clarifying questions? (y/n, default=n): ").strip().lower()
        use_interactive = mode == 'y'
        
        if use_interactive:
            await run_interactive_research(client, query, args.workflow_id)
        else:
            await run_basic_research(client, query, args.workflow_id)


if __name__ == "__main__":
    asyncio.run(main())
