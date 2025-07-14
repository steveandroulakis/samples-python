from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional

from pydantic import BaseModel


class ClarificationInput(BaseModel):
    """Input for providing clarification responses"""
    responses: Dict[str, str]  # question -> answer mapping


class ResearchStatusInput(BaseModel):
    """Input for getting research status"""
    pass


@dataclass
class ResearchInteraction:
    """Represents a research interaction with clarifications"""
    original_query: str
    clarification_questions: Optional[List[str]] = None
    clarification_responses: Optional[Dict[str, str]] = None
    enriched_query: Optional[str] = None
    final_result: Optional[str] = None
    status: str = "pending"  # pending, awaiting_clarifications, researching, completed

    def __str__(self):
        return f"Query: {self.original_query}, Status: {self.status}, Questions: {len(self.clarification_questions or [])}"