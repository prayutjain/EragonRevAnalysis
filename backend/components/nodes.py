# nodes.py - All LangGraph node methods
import json
import logging
from datetime import datetime
from langchain_core.messages import HumanMessage, SystemMessage
from typing import List, Dict, Any

from .models import QueryState, PlannerOutput, ReasonerOutput, ReflectorOutput
from exceptions import NoResultError

logger = logging.getLogger(__name__)


class NodeHandlers:
    """Handles all LangGraph node operations"""
    
    def __init__(self, llm, tool_executor, schema_config, llm_schema):
        self.llm = llm
        self.tool_executor = tool_executor
        self.schema_config = schema_config
        self.llm_schema = llm_schema
    
    def planner_node(self, state: QueryState) -> QueryState:
        """Plan which tools to use and generate queries"""
        start_time = datetime.now()
        
        # Build the planning prompt with conversation context
        system_prompt = self._build_planning_prompt()
        
        # Include conversation history if available
        context = ""
        if state.get("conversation_history"):
            context = "\n\nRecent conversation:\n"
            for turn in state["conversation_history"][-3:]:  # Last 3 turns
                context += f"Q: {turn['question']}\nA: {turn['answer'][:100]}...\n\n"
        
        # Add previous attempt context if this is a retry
        if state.get("execution_history"):
            retrieval_logs = [
                hist for hist in state["execution_history"]
                if hist.get("phase") == "retrieval" and "tool" in hist
            ]
            if retrieval_logs:
                context += "\n\nPrevious retrieval attempts:\n"
                for hist in retrieval_logs:
                    context += (
                        f"- Tool: {hist.get('tool', 'N/A')}, "
                        f"Query: {hist.get('query', 'N/A')}, "
                        f"Result count: {hist.get('result_count', 0)}\n"
                    )
                    if hist.get('error'):
                        context += f"  Error: {hist['error']}\n"
            
            if state.get("errors"):
                context += "\nPrevious errors:\n" + "\n".join(f"- {err}" for err in state["errors"])
        
        user_prompt = f"""Question: {state['question']}
        
{context}

Based on the schema and available tools, generate a plan to answer this question. Pay close attention to history and previous attempts.
Include specific tool calls with exact queries.

IMPORTANT QUERY PLANNING STRATEGIES:

1. **Semantic-First Approach for Entity Resolution**:
   - When searching for specific entities (companies, accounts, contacts), ALWAYS start with vector_search first
   - This helps find entities even with spelling variations, abbreviations, or different formats
   - Example: "AT&T" might be stored as "AT and T", "ATT", "A.T.&T.", etc.

2. **Progressive Search Strategy**:
   - Phase 1: Use vector_search to find potential entity matches
   - Phase 2: Extract found entity IDs/names from vector results
   - Phase 3: Use exact matches from vector results in SQL/Cypher queries
   
3. **Entity Name Handling**:
   - For company names: Consider variations (e.g., "AT&T" → search for "AT&T", "ATT", "AT and T")
   - For partial matches: Use CONTAINS in Cypher, LIKE '%term%' in SQL
   - Always handle case-insensitive searches

4. **Graph Search Optimization**:
   - After finding entities via vector search, use their exact IDs in graph queries
   - This avoids the "no results" problem when names don't match exactly
   
5. **Fallback Strategies**:
   - If specific entity search fails, broaden to category search
   - Example: If "AT&T opportunities" fails, try "telecommunications opportunities"

6. User may refer to **historical chat** context, so consider previous questions and answers to build a more informed plan.


Example Planning Approach for "Find opportunities connected to AT&T":
```
1. vector_search: "AT&T ATT AT and T telecommunications accounts"
2. Extract account names/IDs from vector results
3. cypher_query: Use exact matches found in step 2
4. If no results, broaden: vector_search: "telecom telecommunications opportunities"
```

Remember:
- Always prefer ID-based lookups over name-based when possible
- Use vector search to resolve entity names before graph/SQL queries
- Include multiple spelling variations in searches
- Plan for progressive refinement if initial queries return no results

Generate your plan following this semantic-first approach."""

        messages = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=user_prompt)
        ]
        
        # Use structured output with Pydantic
        structured_llm = self.llm.with_structured_output(PlannerOutput)
        
        try:
            # Get structured response
            plan_data = structured_llm.invoke(messages)
            
            # Update state
            state["plan"] = plan_data.plan
            state["tool_calls"] = [tc.dict() for tc in plan_data.tool_calls]
            
            # Add to reasoning steps
            if "reasoning_steps" not in state:
                state["reasoning_steps"] = []
            state["reasoning_steps"].append(f"Planning: {plan_data.reasoning}")
            
            # Log execution for tracing
            execution_entry = {
                "phase": "planning",
                "timestamp": datetime.now().isoformat(),
                "duration": (datetime.now() - start_time).total_seconds(),
                "plan": plan_data.dict()
            }
            if "execution_history" not in state:
                state["execution_history"] = []
            state["execution_history"].append(execution_entry)
            
        except Exception as e:
            logger.error(f"Planning error: {e}")
            state["errors"] = state.get("errors", []) + [f"Planning error: {str(e)}"]
            state["tool_calls"] = []
        
        return state
    
    def retriever_node(self, state: QueryState) -> QueryState:
        """Execute the planned tool calls and retrieve data"""
        if "raw_results" not in state:
            state["raw_results"] = []
        if "evidence" not in state:
            state["evidence"] = []
        if "seen_queries" not in state:
            state["seen_queries"] = set()
        
        for tool_call in state.get("tool_calls", []):
            start_time = datetime.now()
            tool_name = tool_call.get("tool")
            query = tool_call.get("query")
            purpose = tool_call.get("purpose", "")
            
            # Skip duplicate queries
            query_key = (tool_name, query)
            if query_key in state["seen_queries"]:
                logger.info(f"Skipping duplicate query: {tool_name} - {query[:50]}...")
                continue
            state["seen_queries"].add(query_key)
            
            try:
                results = []
                
                # Execute based on tool type
                if tool_name == "duckdb_sql":
                    results = self.tool_executor.execute_sql(query)
                    
                    # Hybrid retriever fallback
                    if len(results) == 0:
                        logger.info(f"SQL returned 0 results, falling back to vector search")
                        search_text = self.tool_executor.extract_search_terms_from_sql(query)
                        if search_text:
                            vector_results = self.tool_executor.execute_vector_search(search_text)
                            if vector_results:
                                hydrated_results = self.tool_executor.hydrate_vector_hits(vector_results)
                                results.extend(hydrated_results)
                                logger.info(f"Vector fallback added {len(hydrated_results)} results")
                        
                elif tool_name == "cypher_query":
                    results = self.tool_executor.execute_cypher(query)
                    
                elif tool_name == "vector_search":
                    results = self.tool_executor.execute_vector_search(query)
                    # Log detailed info about vector results
                    logger.info(f"Vector search completed: {len(results)} results")
                    if results:
                        # Log sample of what was found
                        sample = results[0]
                        logger.info(f"Sample result - ID: {sample.get('id')}, Has metadata: {'metadata' in sample}")
                        if 'metadata' in sample:
                            logger.info(f"Metadata keys: {list(sample['metadata'].keys())}")
                else:
                    logger.warning(f"Unknown tool: {tool_name}")
                    continue
                
                # Ensure results is always a list
                if results is None:
                    results = []
                    logger.warning(f"{tool_name} returned None, converting to empty list")
                
                # Store raw results with metadata
                result_entry = {
                    "tool": tool_name,
                    "query": query,
                    "purpose": purpose,
                    "results": results,
                    "result_count": len(results),
                    "timestamp": datetime.now().isoformat(),
                    "duration": (datetime.now() - start_time).total_seconds()
                }
                state["raw_results"].append(result_entry)
                
                # Extract evidence for tracing
                for idx, result in enumerate(results):
                    if isinstance(result, dict):
                        # Generate evidence ID
                        evidence_id = (
                            result.get("id") or 
                            result.get("row_id") or 
                            result.get("n.id") or
                            f"{tool_name}_{len(state['evidence'])}"
                        )
                        evidence_id = str(evidence_id)
                        
                        state["evidence"].append({
                            "id": evidence_id,
                            "source": tool_name,
                            "data": result,
                            "index": idx
                        })
                
                # Log successful execution
                if "execution_history" not in state:
                    state["execution_history"] = []
                state["execution_history"].append({
                    "phase": "retrieval",
                    "tool": tool_name,
                    "query": query,
                    "result_count": len(results),
                    "duration": (datetime.now() - start_time).total_seconds(),
                    "has_results": len(results) > 0
                })
                
            except Exception as e:
                logger.error(f"Error executing {tool_name}: {e}", exc_info=True)
                error_msg = f"{tool_name} error: {str(e)}"
                state["errors"] = state.get("errors", []) + [error_msg]
                
                # Log failed attempt
                if "execution_history" not in state:
                    state["execution_history"] = []
                state["execution_history"].append({
                    "phase": "retrieval",
                    "tool": tool_name,
                    "query": query,
                    "error": str(e),
                    "duration": (datetime.now() - start_time).total_seconds(),
                    "has_results": False
                })
    
    def reasoner_node(self, state: QueryState) -> QueryState:
        """Reason over retrieved data to form an answer"""
        start_time = datetime.now()

        # FIXED: Check ALL results, not just counting
        all_results = state.get("raw_results", [])
        total_results = sum(r.get("result_count", 0) for r in all_results)
        
        # Also check if we have any actual data, even if counts are wrong
        has_any_data = any(
            r.get("results") and len(r.get("results", [])) > 0 
            for r in all_results
        )
        
        # Log what we found for debugging
        logger.info(f"Reasoner check - Total result count: {total_results}, Has any data: {has_any_data}")
        for r in all_results:
            logger.info(f"  - {r.get('tool')}: {r.get('result_count', 0)} results, actual data: {len(r.get('results', []))}")
        
        if total_results == 0 and not has_any_data and not state.get("errors"):
            raise NoResultError(f"No results found for query: {state['question']}")
        
        # Build reasoning prompt
        system_prompt = """You are a data analyst reasoning over retrieved evidence to answer questions.
        Analyze the data carefully and provide a comprehensive answer based on the evidence.
        
        IMPORTANT: Consider results from ALL tools used:
        - Vector search results show semantic matches and may contain the entities you're looking for
        - SQL/Cypher results show exact database matches
        - Even if SQL/Cypher returned no results, vector search results are still valid data
        
        If vector search found matches but database queries didn't, this likely means:
        - The entity exists but with a different name format
        - You should report what was found in vector search
        - Suggest using the exact names from vector search for future queries"""

        
        # Prepare evidence summary
        evidence_summary = self._summarize_evidence(state.get("raw_results", []))
        
        user_prompt = f"""Original Question: {state['question']}

Plan: {state.get('plan', 'No plan available')}

Retrieved Evidence:
{evidence_summary}

Based on this evidence, provide a comprehensive answer to the question."""

        messages = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=user_prompt)
        ]
        
        # Use structured output with Pydantic
        structured_llm = self.llm.with_structured_output(ReasonerOutput)
        
        try:
            reasoning_data = structured_llm.invoke(messages)
            
            # Update state
            state["answer"] = reasoning_data.answer
            state["confidence_score"] = reasoning_data.confidence
            state["needs_more_data"] = reasoning_data.needs_more_data
            
            # Add reasoning steps
            if "reasoning_steps" not in state:
                state["reasoning_steps"] = []
            state["reasoning_steps"].append(f"Reasoning: {reasoning_data.reasoning}")
            
            if reasoning_data.missing_data:
                state["reasoning_steps"].append(f"Missing data: {reasoning_data.missing_data}")
            
            # Log execution
            execution_entry = {
                "phase": "reasoning",
                "timestamp": datetime.now().isoformat(),
                "duration": (datetime.now() - start_time).total_seconds(),
                "reasoning": reasoning_data.dict()
            }
            if "execution_history" not in state:
                state["execution_history"] = []
            state["execution_history"].append(execution_entry)
            
        except Exception as e:
            logger.error(f"Reasoning error: {e}")
            state["answer"] = "Failed to generate answer due to processing error"
            state["confidence_score"] = 0.0
            state["needs_more_data"] = False
            state["errors"] = state.get("errors", []) + [f"Reasoning error: {str(e)}"]
        
        # Increment iteration count
        state["iteration_count"] = state.get("iteration_count", 0) + 1
        
        # Ensure answer exists and is a string
        if "answer" not in state or not isinstance(state["answer"], str):
            if "raw_results" in state and state["raw_results"]:
                first_result = state["raw_results"][0]
                if first_result["result_count"] > 0:
                    state["answer"] = f"Found {first_result['result_count']} results. See evidence for details."
                else:
                    state["answer"] = "No results found for your query."
            else:
                state["answer"] = "Unable to process query."
        
        return state
    
    def reflector_node(self, state: QueryState) -> QueryState:
        """Reflect on the current state and determine next steps"""
        start_time = datetime.now()
        
        # Skip reflection if we don't have tool_calls yet
        if not state.get("tool_calls"):
            state["needs_more_data"] = False
            return state
        
        # Build reflection prompt
        system_prompt = """You are a critical analyst reviewing the query process.
        Evaluate whether the current answer adequately addresses the original question.
        Identify gaps and suggest specific improvements."""
        
        user_prompt = f"""Original Question: {state['question']}

Current Answer: {state.get('answer', 'No answer yet')}
Confidence: {state.get('confidence_score', 0)}

Execution History:
{json.dumps(state.get('execution_history', []), indent=2)}

Errors encountered: {state.get('errors', [])}

Critically evaluate:
1. Does the answer fully address the question?
2. What specific data is missing?
3. What additional queries should be run?"""

        messages = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=user_prompt)
        ]
        
        # Use structured output
        structured_llm = self.llm.with_structured_output(ReflectorOutput)
        
        try:
            reflection_data = structured_llm.invoke(messages)
            
            # Add reflection to reasoning steps
            if "reasoning_steps" not in state:
                state["reasoning_steps"] = []
            state["reasoning_steps"].append(f"Reflection: {reflection_data.evaluation}")
            
            # Update needs_more_data based on reflection
            if reflection_data.continue_search and state["iteration_count"] < state.get("max_iterations", 3):
                state["needs_more_data"] = True
            else:
                state["needs_more_data"] = False
            
            # Log reflection
            execution_entry = {
                "phase": "reflection",
                "timestamp": datetime.now().isoformat(),
                "duration": (datetime.now() - start_time).total_seconds(),
                "reflection": reflection_data.dict()
            }
            if "execution_history" not in state:
                state["execution_history"] = []
            state["execution_history"].append(execution_entry)
            
        except Exception as e:
            logger.error(f"Reflection error: {e}")
            state["needs_more_data"] = False
            state["errors"] = state.get("errors", []) + [f"Reflection error: {str(e)}"]
        
        return state
    
    def _build_planning_prompt(self) -> str:
        """Build the system prompt for planning with full schema context"""
        # Get original column names from schema_config
        column_mapping = {}
        for table_name, table_info in self.schema_config['tables'].items():
            column_mapping[table_name] = {}
            for col_name, col_info in table_info['columns'].items():
                column_mapping[table_name][col_info['clean_name']] = col_name
        
        return f"""You are an intelligent query planner with access to three data stores:

1. **DuckDB (SQL)**: For analytical queries, aggregations, and filtering
   
   IMPORTANT: 
   - NEVER remove the quotes shown in the examples
   - You must use the ORIGINAL column names (with spaces, capitals, special characters) in quotes
   - Date columns ("Close Date", "Created Date") are DATE type - use proper date functions
   
   Tables and original column names:
{json.dumps(column_mapping, indent=2)}
   
   Example SQL queries:
   - SELECT "Account Name", SUM("Amount") FROM opportunities GROUP BY "Account Name"
   - SELECT "Stage", AVG("Amount") FROM opportunities GROUP BY "Stage"
   - SELECT * FROM opportunities WHERE "Probability (%)" > 50
   - SELECT COUNT(*) FROM opportunities WHERE strftime("Close Date", '%Y-%m') = strftime(CURRENT_DATE, '%Y-%m')
   - SELECT * FROM opportunities WHERE "Close Date" >= CURRENT_DATE - INTERVAL 30 DAY

2. **Neo4j (Cypher)**: For relationship queries and graph traversal
   
   IMPORTANT: WHERE clause must come BEFORE RETURN
   
   Node labels (use exact capitalization):
   - AccountAndContact (from account_and_contact table)
   - Opportunities (from opportunities table)
   - Sample (from sample table)
   
   Relationships:
{json.dumps(self.llm_schema['relationships'], indent=2)}
   
   Example Cypher queries (note the clause order!):
   - MATCH (o:Opportunities) WHERE o.amount > 100000 RETURN o.id, o.account_name
   - MATCH (o:Opportunities) WITH o.primary_contact AS contact, count(*) AS opps WHERE opps > 1 RETURN contact, opps
   - MATCH (o1:Opportunities)-[:SHARES_CONTACT]->(o2:Opportunities) RETURN o1.id, o2.id, o1.account_name, o2.account_name
   - MATCH (a:AccountAndContact)-[:ACCOUNT_NAME]->(o:Opportunities) WHERE a.account_name CONTAINS 'Disney' RETURN a.id, o.id

3. **ChromaDB (Vector Search)**: For semantic similarity and fuzzy matching
   
   IMPORTANT: Vector search FAILS when collection doesn't exist. Use SQL for exact matches instead.
   
   Collections: account_and_contact_vectors, opportunities_vectors, sample_vectors
   
   Example vector search queries (as JSON):
   {{"text": "cloud infrastructure", "collection": "opportunities_vectors", "n_results": 10}}
   {{"text": "executive meeting", "where": {{"Stage": "Negotiate"}}, "collection": "opportunities_vectors"}}

Guidelines:
- For SQL: ALWAYS use double quotes around column names. For dates, use strftime() or date comparisons
- For Cypher: WHERE comes before RETURN. Use WITH for aggregations
- For Vector search: Only use when you need semantic/fuzzy matching, not exact matches
- Return max 3 tool_calls unless the question explicitly needs more"""
    
    def _summarize_evidence(self, raw_results: List[Dict[str, Any]]) -> str:
        """Summarize evidence for reasoning"""
        summary = ""
        for i, result in enumerate(raw_results):
            summary += f"\n--- Result {i+1} from {result['tool']} ---\n"
            summary += f"Query: {result['query']}\n"
            summary += f"Purpose: {result.get('purpose', 'N/A')}\n"
            summary += f"Result count: {result['result_count']}\n"
            
            if result['results']:
                summary += "Data:\n"
                # Show first few results
                for j, data in enumerate(result['results'][:5]):
                    try:
                        # Handle different data types
                        if isinstance(data, dict):
                            # Convert any non-serializable values
                            clean_data = {}
                            for k, v in data.items():
                                if hasattr(v, 'isoformat'):  # datetime
                                    clean_data[k] = v.isoformat()
                                elif hasattr(v, '__dict__'):  # objects
                                    clean_data[k] = str(v)
                                else:
                                    clean_data[k] = v
                            summary += f"  {j+1}. {json.dumps(clean_data, indent=2)}\n"
                        else:
                            summary += f"  {j+1}. {str(data)}\n"
                    except Exception as e:
                        summary += f"  {j+1}. [Error displaying data: {str(e)}]\n"
                if len(result['results']) > 5:
                    summary += f"  ... and {len(result['results']) - 5} more results\n"
            else:
                summary += "No results found\n"
        
        return summary