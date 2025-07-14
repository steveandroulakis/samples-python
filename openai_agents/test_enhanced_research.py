#!/usr/bin/env python3
"""
Test script for the enhanced research workflow with clarifying questions
"""

import asyncio
import sys
from temporalio import workflow

# Test imports
try:
    from openai_agents.workflows.research_agents.clarifying_agent import (
        new_clarifying_agent,
        Clarifications,
    )
    from openai_agents.workflows.research_agents.triage_agent import new_triage_agent
    from openai_agents.workflows.research_agents.instruction_agent import new_instruction_agent
    from openai_agents.workflows.research_agents.research_manager import (
        ResearchManager,
        ClarificationResult,
    )
    from openai_agents.workflows.research_agents.research_models import (
        ClarificationInput,
        ResearchInteraction,
    )
    from openai_agents.workflows.research_bot_workflow import ResearchWorkflow
    print("✅ All imports successful")
except ImportError as e:
    print(f"❌ Import error: {e}")
    sys.exit(1)


def test_agent_creation():
    """Test that all agents can be created successfully"""
    print("🧪 Testing agent creation...")
    
    try:
        clarifying_agent = new_clarifying_agent()
        assert clarifying_agent.name == "Clarifying Questions Agent"
        assert clarifying_agent.output_type == Clarifications
        print("  ✅ Clarifying agent created")
        
        triage_agent = new_triage_agent()
        assert triage_agent.name == "Triage Agent"
        print("  ✅ Triage agent created")
        
        instruction_agent = new_instruction_agent()
        assert instruction_agent.name == "Research Instruction Agent"
        print("  ✅ Instruction agent created")
        
        print("✅ All agents created successfully")
        return True
        
    except Exception as e:
        print(f"❌ Agent creation failed: {e}")
        return False


def test_research_manager():
    """Test that ResearchManager can be instantiated"""
    print("🧪 Testing ResearchManager...")
    
    try:
        manager = ResearchManager()
        assert hasattr(manager, 'triage_agent')
        assert hasattr(manager, 'run')
        assert hasattr(manager, 'run_with_clarifications_start')
        assert hasattr(manager, 'run_with_clarifications_complete')
        print("✅ ResearchManager created successfully")
        return True
        
    except Exception as e:
        print(f"❌ ResearchManager creation failed: {e}")
        return False


def test_models():
    """Test that Pydantic models work correctly"""
    print("🧪 Testing Pydantic models...")
    
    try:
        # Test Clarifications model
        clarifications = Clarifications(questions=["What's your budget?", "When do you want to travel?"])
        assert len(clarifications.questions) == 2
        print("  ✅ Clarifications model works")
        
        # Test ClarificationInput model
        clarif_input = ClarificationInput(responses={"question_0": "Under $1000", "question_1": "March"})
        assert len(clarif_input.responses) == 2
        print("  ✅ ClarificationInput model works")
        
        # Test ResearchInteraction model
        interaction = ResearchInteraction(
            original_query="Best vacation spots",
            status="pending"
        )
        assert interaction.original_query == "Best vacation spots"
        assert interaction.status == "pending"
        print("  ✅ ResearchInteraction model works")
        
        # Test ClarificationResult model
        result = ClarificationResult(
            needs_clarifications=True,
            questions=["Budget?", "Timeline?"]
        )
        assert result.needs_clarifications is True
        assert result.questions and len(result.questions) == 2
        print("  ✅ ClarificationResult model works")
        
        print("✅ All models work correctly")
        return True
        
    except Exception as e:
        print(f"❌ Model testing failed: {e}")
        return False


def test_workflow_definition():
    """Test that the workflow definition is valid"""
    print("🧪 Testing workflow definition...")
    
    try:
        # Test workflow class exists and has required methods
        assert hasattr(ResearchWorkflow, 'run')
        assert hasattr(ResearchWorkflow, 'get_status')
        assert hasattr(ResearchWorkflow, 'provide_clarifications')
        
        # Check that it's properly decorated as a workflow
        # Note: The actual attribute name may be different in this version
        workflow_attrs = [attr for attr in dir(ResearchWorkflow) if 'workflow' in attr.lower()]
        assert len(workflow_attrs) > 0, f"No workflow attributes found. Available: {workflow_attrs}"
        
        print("✅ Workflow definition is valid")
        return True
        
    except Exception as e:
        print(f"❌ Workflow definition test failed: {e}")
        return False


def test_query_enrichment():
    """Test the query enrichment functionality"""
    print("🧪 Testing query enrichment...")
    
    try:
        manager = ResearchManager()
        
        original_query = "Best vacation spots"
        questions = ["What's your budget?", "When do you want to travel?"]
        responses = {"question_0": "Under $1000", "question_1": "March"}
        
        enriched = manager._enrich_query(original_query, questions, responses)
        
        assert "Best vacation spots" in enriched
        assert "Under $1000" in enriched
        assert "March" in enriched
        assert "budget" in enriched.lower()
        
        print("✅ Query enrichment works correctly")
        return True
        
    except Exception as e:
        print(f"❌ Query enrichment test failed: {e}")
        return False


async def run_all_tests():
    """Run all tests"""
    print("🚀 Starting Enhanced Research Workflow Tests")
    print("=" * 50)
    
    tests = [
        test_agent_creation,
        test_research_manager,
        test_models,
        test_workflow_definition,
        test_query_enrichment,
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        try:
            if test():
                passed += 1
            else:
                print(f"❌ Test {test.__name__} failed")
        except Exception as e:
            print(f"❌ Test {test.__name__} failed with exception: {e}")
    
    print("\n" + "=" * 50)
    print(f"📊 Test Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! The enhanced research workflow is ready to use.")
        return True
    else:
        print("❌ Some tests failed. Please check the implementation.")
        return False


def print_usage():
    """Print usage instructions"""
    print("\n📋 Usage Instructions:")
    print("-" * 30)
    print("1. Basic Research:")
    print("   python openai_agents/run_research_workflow.py 'Your research query'")
    print()
    print("2. Interactive Research (with clarifying questions):")
    print("   python openai_agents/run_research_workflow.py 'Your research query' --interactive")
    print()
    print("3. Check workflow status:")
    print("   python openai_agents/run_research_workflow.py --status")
    print()
    print("4. Send clarifications to running workflow:")
    print("   python openai_agents/run_research_workflow.py --clarify question_0='answer1' question_1='answer2'")
    print()
    print("5. Interactive mode (prompts for input):")
    print("   python openai_agents/run_research_workflow.py")
    print()
    print("📝 Note: Make sure Temporal server is running on localhost:7233")
    print("📝 Note: Set OPENAI_API_KEY environment variable")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Test the enhanced research workflow")
    parser.add_argument("--usage", action="store_true", help="Show usage instructions")
    args = parser.parse_args()
    
    if args.usage:
        print_usage()
    else:
        success = asyncio.run(run_all_tests())
        if success:
            print_usage()
        sys.exit(0 if success else 1)