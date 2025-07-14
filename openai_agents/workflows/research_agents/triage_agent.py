from __future__ import annotations

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from agents import Agent

    from openai_agents.workflows.research_agents.clarifying_agent import new_clarifying_agent
    from openai_agents.workflows.research_agents.instruction_agent import new_instruction_agent


TRIAGE_AGENT_PROMPT = """
Decide whether clarifications are required.
• If yes → call transfer_to_clarifying_questions_agent
• If no  → call transfer_to_research_instruction_agent
Return exactly ONE function-call.
"""


def new_triage_agent() -> Agent:
    """Create a new triage agent for routing research requests"""
    clarifying_agent = new_clarifying_agent()
    instruction_agent = new_instruction_agent()
    
    return Agent(
        name="Triage Agent",
        model="gpt-4o-mini",
        instructions=TRIAGE_AGENT_PROMPT,
        handoffs=[clarifying_agent, instruction_agent],
    )