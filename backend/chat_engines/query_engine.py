# query_engine.py - Optimized Query Engine with all enhancements
import json
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
from pathlib import Path

import duckdb
from neo4j import GraphDatabase
import chromadb
from chromadb.config import Settings
from langchain_openai import OpenAIEmbeddings, ChatOpenAI
from langgraph.graph import StateGraph, END

from config import *
from exceptions import NoResultError, BadQuestionError, PlanningError
from components.models import QueryState, PlannerOutput, ReasonerOutput, ReflectorOutput, ToolCall
from components.tools import ToolExecutor
from components.nodes import NodeHandlers

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class QueryEngine:
    """Main query engine that orchestrates LangGraph workflow with optimizations"""
    
    def __init__(self):
        # Initialize connections (lazy loading)
        self._duckdb_con = None
        self._neo4j_driver = None
        self._chroma_client = None
        self._embeddings = None
        self._llm = None
        self._tool_executor = None
        self._node_handlers = None
        
        # Load schemas
        self.schema_config = self._load_schema_config()
        self.llm_schema = self._load_llm_schema()

        # Add conversation memory
        self.memory = {}  # Dict[session_id, List[Dict[question, answer, timestamp]]]
        self.max_memory_turns = 5  # Keep last 5 turns per session
        
        # Initialize the workflow
        self.workflow = self._build_workflow()
        
    @property
    def duckdb_con(self):
        """Lazy load DuckDB connection"""
        if self._duckdb_con is None:
            self._duckdb_con = duckdb.connect(str(DUCKDB_PATH))
            logger.info(f"Connected to DuckDB at {DUCKDB_PATH}")
        return self._duckdb_con
    
    @property
    def neo4j_driver(self):
        """Lazy load Neo4j driver"""
        if self._neo4j_driver is None:
            self._neo4j_driver = GraphDatabase.driver(
                NEO4J_URI, 
                auth=(NEO4J_USER, NEO4J_PASSWORD)
            )
            logger.info(f"Connected to Neo4j at {NEO4J_URI}")
        return self._neo4j_driver
    
    @property
    def chroma_client(self):
        """Lazy load ChromaDB client"""
        if self._chroma_client is None:
            self._chroma_client = chromadb.PersistentClient(
                path=str(CHROMA_PERSIST_DIR),
                settings=Settings(anonymized_telemetry=False)
            )
            logger.info(f"Connected to ChromaDB at {CHROMA_PERSIST_DIR}")
        return self._chroma_client
    
    @property
    def embeddings(self):
        """Lazy load embeddings model"""
        if self._embeddings is None:
            self._embeddings = OpenAIEmbeddings(
                model=EMBEDDING_MODEL,
                openai_api_key=OPENAI_API_KEY
            )
        return self._embeddings
    
    @property
    def llm(self):
        """Lazy load LLM"""
        if self._llm is None:
            self._llm = ChatOpenAI(
                model="gpt-4o",
                temperature=0,
                openai_api_key=OPENAI_API_KEY,
                stream=True
            )
        return self._llm
    
    @property
    def tool_executor(self):
        """Lazy load tool executor"""
        if self._tool_executor is None:
            self._tool_executor = ToolExecutor(
                duckdb_con=self.duckdb_con,
                neo4j_driver=self.neo4j_driver,
                chroma_client=self.chroma_client,
                llm=self.llm
            )
        return self._tool_executor
    
    @property
    def node_handlers(self):
        """Lazy load node handlers"""
        if self._node_handlers is None:
            self._node_handlers = NodeHandlers(
                llm=self.llm,
                tool_executor=self.tool_executor,
                schema_config=self.schema_config,
                llm_schema=self.llm_schema
            )
        return self._node_handlers
    
    def _load_schema_config(self) -> Dict[str, Any]:
        """Load the full schema configuration"""
        schema_path = CONFIG_DIR / "schema_config.json"
        if not schema_path.exists():
            raise FileNotFoundError(f"Schema config not found at {schema_path}")
        
        with open(schema_path, 'r') as f:
            return json.load(f)
    
    def _load_llm_schema(self) -> Dict[str, Any]:
        """Load the simplified LLM schema"""
        llm_schema_path = CONFIG_DIR / "llm_schema.json"
        if not llm_schema_path.exists():
            raise FileNotFoundError(f"LLM schema not found at {llm_schema_path}")
        
        with open(llm_schema_path, 'r') as f:
            return json.load(f)
    
    def _build_workflow(self) -> StateGraph:
        """Build the LangGraph workflow"""
        workflow = StateGraph(QueryState)
        
        # Add nodes
        workflow.add_node("planner", self._planner_node)
        workflow.add_node("retriever", self._retriever_node)
        workflow.add_node("reasoner", self._reasoner_node)
        workflow.add_node("reflector", self._reflector_node)
        
        # Add edges
        workflow.set_entry_point("planner")
        workflow.add_edge("planner", "retriever")
        workflow.add_edge("retriever", "reasoner")
        
        # Conditional edge from reasoner
        workflow.add_conditional_edges(
            "reasoner",
            self._should_continue,
            {
                "reflector": "reflector",
                "end": END
            }
        )
        
        # From reflector back to planner
        workflow.add_edge("reflector", "planner")
        
        return workflow.compile()
    
    # Delegate node methods to node_handlers
    def _planner_node(self, state: QueryState) -> QueryState:
        """Plan which tools to use and generate queries"""
        return self.node_handlers.planner_node(state)
    
    def _retriever_node(self, state: QueryState) -> QueryState:
        """Execute the planned tool calls and retrieve data"""
        return self.node_handlers.retriever_node(state)
    
    def _reasoner_node(self, state: QueryState) -> QueryState:
        """Reason over retrieved data to form an answer"""
        return self.node_handlers.reasoner_node(state)
    
    def _reflector_node(self, state: QueryState) -> QueryState:
        """Reflect on the current state and determine next steps"""
        return self.node_handlers.reflector_node(state)
    
    def _should_continue(self, state: QueryState) -> str:
        """Determine whether to continue with reflection or end"""
        if state.get("needs_more_data", False) and state.get("iteration_count", 0) < state.get("max_iterations", 3):
            return "reflector"
        return "end"
    
    # Keep these methods for backward compatibility
    def _build_planning_prompt(self) -> str:
        """Build the system prompt for planning with full schema context"""
        return self.node_handlers._build_planning_prompt()
    
    def _extract_search_terms_from_sql(self, sql_query: str) -> str:
        """Extract search terms from SQL for vector fallback"""
        return self.tool_executor.extract_search_terms_from_sql(sql_query)

    def _hydrate_vector_hits(self, vector_results: List[Dict]) -> List[Dict]:
        """Hydrate vector search results with full records from DuckDB"""
        return self.tool_executor.hydrate_vector_hits(vector_results)
    
    def _auto_fix_sql(self, query: str, error: str) -> str:
        """Use LLM to fix SQL query based on error message"""
        return self.tool_executor.auto_fix_sql(query, error)
    
    def _execute_sql(self, query: str) -> List[Dict[str, Any]]:
        """Execute SQL query on DuckDB with auto-repair"""
        return self.tool_executor.execute_sql(query)
    
    def _auto_fix_cypher(self, query: str, error: str) -> str:
        """Use LLM to fix Cypher query based on error message"""
        return self.tool_executor.auto_fix_cypher(query, error)
    
    def _execute_cypher(self, query: str) -> List[Dict[str, Any]]:
        """Execute Cypher query on Neo4j with auto-repair"""
        return self.tool_executor.execute_cypher(query)
    
    def _execute_vector_search(self, query_str: str) -> List[Dict[str, Any]]:
        """Execute vector search on ChromaDB"""
        return self.tool_executor.execute_vector_search(query_str)
    
    def _summarize_evidence(self, raw_results: List[Dict[str, Any]]) -> str:
        """Summarize evidence for reasoning"""
        return self.node_handlers._summarize_evidence(raw_results)
    
    def query(self, question: str, max_iterations: int = 3, session_id: str = "default") -> Dict[str, Any]:
        """Main entry point for querying - optimized with session support"""
        start_time = datetime.now()
        
        # Get conversation history for this session
        session_memory = self.memory.get(session_id, [])
        
        # Initialize state with all required fields
        initial_state = {
            "question": question,
            "max_iterations": max_iterations,
            "iteration_count": 0,
            "execution_history": [],
            "reasoning_steps": [],
            "errors": [],
            "plan": None,
            "tool_calls": [],
            "evidence": [],
            "raw_results": [],
            "answer": None,
            "confidence_score": 0.0,
            "needs_more_data": False,
            "total_execution_time": None,
            "conversation_history": session_memory[-self.max_memory_turns:],
            "seen_queries": set(),  # Initialize empty set for tracking duplicates
            "session_id": session_id
        }
        
        # Run the workflow
        try:
            final_state = self.workflow.invoke(initial_state)
            
            # Calculate total execution time
            total_time = (datetime.now() - start_time).total_seconds()
            
            # Update memory with this turn
            if session_id not in self.memory:
                self.memory[session_id] = []
            
            self.memory[session_id].append({
                "question": question,
                "answer": final_state.get("answer", ""),
                "timestamp": datetime.now().isoformat()
            })
            
            # Trim memory to max turns
            if len(self.memory[session_id]) > self.max_memory_turns:
                self.memory[session_id] = self.memory[session_id][-self.max_memory_turns:]
            
            # Prepare response with all tracing information
            response = {
                "answer": final_state.get("answer", "Unable to generate answer"),
                "evidence": [str(e["id"]) for e in final_state.get("evidence", [])],
                "confidence_score": final_state.get("confidence_score", 0.0),
                "reasoning_steps": final_state.get("reasoning_steps", []),
                "execution_history": final_state.get("execution_history", []),
                "errors": final_state.get("errors", []),
                "total_execution_time": total_time,
                "iterations": final_state.get("iteration_count", 0),
                "raw_results": final_state.get("raw_results", []),
                "conversation_history": session_memory
            }
            
            return response
            
        except NoResultError as e:
            # Raise domain exception for proper handling upstream
            raise e
        except Exception as e:
            logger.error(f"Error in query workflow: {e}")
            return {
                "answer": f"Error processing query: {str(e)}",
                "evidence": [],
                "confidence_score": 0.0,
                "errors": [str(e)],
                "total_execution_time": (datetime.now() - start_time).total_seconds(),
                "raw_results": [],
                "reasoning_steps": [],
                "execution_history": [],
                "iterations": 0,
                "conversation_history": session_memory
            }
    
    def close(self):
        """Close all connections"""
        if self._duckdb_con:
            self._duckdb_con.close()
            logger.info("Closed DuckDB connection")
        if self._neo4j_driver:
            self._neo4j_driver.close()
            logger.info("Closed Neo4j connection")


# Example usage
if __name__ == "__main__":
    engine = QueryEngine()
    
    # Test queries
    test_questions = [
        "What are the top 5 accounts by total opportunity value?",
        "Which contacts are involved in multiple opportunities?",
        "How many opportunities are closing this month?",
        "Show me at-risk opportunities",
        "What's the average deal size by stage?"
    ]
    
    # Test with session support
    session_id = "test_session_123"
    
    for i, question in enumerate(test_questions[:2]):
        print(f"\n{'='*80}")
        print(f"Question {i+1}: {question}")
        print(f"{'='*80}")
        
        try:
            result = engine.query(question, max_iterations=2, session_id=session_id)
            
            print(f"\nAnswer: {result['answer']}")
            print(f"Confidence: {result['confidence_score']:.2f}")
            print(f"Iterations: {result['iterations']}")
            print(f"Total execution time: {result['total_execution_time']:.2f}s")
            
            if result['errors']:
                print(f"Errors: {result['errors']}")
            
            print(f"\nEvidence IDs: {result['evidence'][:3]}...")
            print(f"Raw results count: {len(result['raw_results'])}")
            
        except NoResultError as e:
            print(f"No results: {e}")
        except Exception as e:
            print(f"Error: {e}")
    
    engine.close()