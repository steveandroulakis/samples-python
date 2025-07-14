from temporalio import workflow

from openai_agents.workflows.research_agents.research_manager import ResearchManager
from openai_agents.workflows.research_agents.research_models import (
    ClarificationInput,
    ResearchStatusInput,
    ResearchInteraction,
)


@workflow.defn
class ResearchWorkflow:
    def __init__(self):
        self.research_manager = ResearchManager()
        self.current_interaction: ResearchInteraction | None = None
        self._end_workflow = False

    @workflow.run
    async def run(self, query: str, use_clarifications: bool = False) -> str:
        """
        Run research workflow
        
        Args:
            query: The research query
            use_clarifications: If True, enables interactive clarifying questions
        """
        if use_clarifications:
            return await self._run_interactive(query)
        else:
            return await self.research_manager.run(query, use_clarifications=False)

    async def _run_interactive(self, query: str) -> str:
        """Run interactive research with clarifying questions"""
        self.current_interaction = ResearchInteraction(
            original_query=query,
            status="pending"
        )
        
        # Start the clarification flow
        result = await self.research_manager.run_with_clarifications_start(query)
        
        if result.needs_clarifications:
            self.current_interaction.clarification_questions = result.questions
            self.current_interaction.status = "awaiting_clarifications"
            
            # Wait for clarification responses
            await workflow.wait_condition(
                lambda: bool(self.current_interaction and self.current_interaction.status != "awaiting_clarifications")
            )
            
            if self.current_interaction and self.current_interaction.status == "clarifications_received":
                questions = self.current_interaction.clarification_questions or []
                responses = self.current_interaction.clarification_responses or {}
                # Continue with research using clarifications
                final_result = await self.research_manager.run_with_clarifications_complete(
                    query, 
                    questions,
                    responses
                )
                self.current_interaction.final_result = final_result
                self.current_interaction.status = "completed"
                return final_result
        
        # No clarifications needed, return direct result
        self.current_interaction.final_result = result.research_output
        self.current_interaction.status = "completed"
        return result.research_output or ""

    @workflow.query
    def get_status(self) -> ResearchInteraction | None:
        """Get current research status"""
        return self.current_interaction

    @workflow.update
    async def provide_clarifications(self, input: ClarificationInput) -> ResearchInteraction:
        """Provide clarification responses"""
        if not self.current_interaction:
            raise ValueError("No active research interaction")
        
        if self.current_interaction.status != "awaiting_clarifications":
            raise ValueError(f"Not awaiting clarifications. Current status: {self.current_interaction.status}")
        
        self.current_interaction.clarification_responses = input.responses
        self.current_interaction.status = "clarifications_received"
        
        return self.current_interaction

    @provide_clarifications.validator
    def validate_provide_clarifications(self, input: ClarificationInput) -> None:
        if not input.responses:
            raise ValueError("Clarification responses cannot be empty")
        
        if self.current_interaction and self.current_interaction.clarification_questions:
            expected_count = len(self.current_interaction.clarification_questions)
            provided_count = len(input.responses)
            if provided_count != expected_count:
                raise ValueError(f"Expected {expected_count} responses, got {provided_count}")

    @workflow.signal
    async def end_workflow_signal(self):
        """Signal to end the workflow"""
        self._end_workflow = True
