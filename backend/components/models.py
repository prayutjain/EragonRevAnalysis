from typing import Dict, List, Any, Optional, TypedDict, Annotated, Union, Set, Tuple
from pydantic import BaseModel, Field

# Pydantic models for structured outputs
class ToolCall(BaseModel):
    tool: str = Field(..., description="Tool name: duckdb_sql, cypher_query, or vector_search")
    query: str = Field(..., description="The actual query to execute")
    purpose: str = Field(..., description="What this query will find")

class PlannerOutput(BaseModel):
    plan: str = Field(..., description="Step-by-step explanation of how to answer the question")
    tool_calls: List[ToolCall] = Field(..., description="List of tool calls to execute")
    reasoning: str = Field(..., description="Why these tools and queries are appropriate")

class ReasonerOutput(BaseModel):
    answer: str = Field(..., description="Complete answer to the question")
    confidence: float = Field(..., ge=0.0, le=1.0, description="Confidence level")
    needs_more_data: bool = Field(False, description="Whether more data is needed")
    missing_data: Optional[str] = Field(None, description="Description of missing data")
    reasoning: str = Field(..., description="Step-by-step reasoning process")

class ReflectorOutput(BaseModel):
    evaluation: str = Field(..., description="Critical assessment of the current answer")
    gaps: List[str] = Field(default_factory=list, description="List of specific gaps")
    suggestions: List[str] = Field(default_factory=list, description="Specific suggestions")
    continue_search: bool = Field(..., description="Whether to continue searching", alias="continue")

# State definition for LangGraph
class QueryState(TypedDict):
    """State for the query processing workflow"""
    question: str
    plan: Optional[str]
    tool_calls: List[Dict[str, Any]]
    evidence: List[Dict[str, Any]]
    raw_results: List[Dict[str, Any]]
    reasoning_steps: List[str]
    answer: Optional[str]
    execution_history: List[Dict[str, Any]]
    errors: List[str]
    confidence_score: Optional[float]
    total_execution_time: Optional[float]
    needs_more_data: bool
    iteration_count: int
    max_iterations: int
    seen_queries: Set[Tuple[str, str]]  # Track seen (tool, query) pairs
    session_id: str  # Track session for memory
    conversation_history: List[Dict[str, Any]]  # Recent conversation context