# cro_query_engine.py - Fixed version with more generous visualization logic
import json
import re
import time
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
import pandas as pd
from .query_engine import QueryEngine
import logging

from components.cro_models import ChartOptions, UIBlock, Visualization, ConfidenceIndicator, AnswerData
from components.cro_visualizers import Visualizer
from components.cro_utils import CROUtils

logger = logging.getLogger(__name__)


class CROQueryEngine:
    """Query engine wrapper that formats answers for CRO consumption with UI blocks and visuals"""
    
    def __init__(self):
        self.base_engine = QueryEngine()
        self.kpi_cache = {}
        self.visualizer = Visualizer()
        self.utils = CROUtils()
        
        # Enhanced system prompt for CRO formatting with smarter visualization guidance
        self.cro_formatting_prompt = """
You are formatting data insights for a Chief Revenue Officer (CRO). Include sections like summary, key insights, and actionable recommendations.

Guidelines:
- Use executive-friendly language (no technical jargon)
- Focus on business impact and revenue implications
- Provide actionable insights and next steps
- Format using proper Markdown
- Add appropriate sections and headers like ## Summary, ## Key Insights, ## Recommendations

When analyzing data:
1. Identify the key business insight
2. Explain why it matters for revenue/growth
3. Recommend specific actions with timelines

For visualizations:
- Suggest visualizations that enhance understanding of the data. 
- Generally with numerical data, it is good to have visualizations - like bar chart or atleast tables summarising the result.
- Consider the data structure and choose appropriate visualization types:
  - Funnel chart: for stage progression data with clear stages
  - Bar chart: for comparing metrics across categories (3+ categories)
  - Doughnut chart: for showing distribution of items that sum to a meaningful whole
  - Table: for detailed data with multiple dimensions or when showing many rows
- Be generous with visualizations - if data can be visualized meaningfully, suggest it
- Include maximum 2 visualizations (1 chart + 1 table if needed)

[SUGGESTED_VISUALIZATIONS]
Analyze the data and suggest visualizations that would add value.
Specify: chart_type:reason_why_useful
Example: bar:comparing revenue across 5 product lines
Example: table:showing detailed metrics for 15 accounts
If no visualization adds value, write: none:data not suitable for visualization
[/SUGGESTED_VISUALIZATIONS]
"""

    def query(self, question: str, max_iterations: int = 3, session_id: str = "default") -> Dict[str, Any]:
        """Execute query and format for CRO with UI blocks and visuals"""
        # Get base results
        base_result = self.base_engine.query(question, max_iterations, session_id)
        
        # Debug logging
        logger.info(f"Base result keys: {list(base_result.keys())}")
        logger.info(f"Raw results count: {len(base_result.get('raw_results', []))}")
        
        # Reconstruct raw_results if missing
        if not base_result.get("raw_results") and base_result.get("execution_history"):
            logger.warning("Raw results missing, attempting to reconstruct from execution history")
            base_result["raw_results"] = self.utils.reconstruct_raw_results(base_result["execution_history"])
        
        # Store original answer before any modifications
        original_answer = base_result.get("answer", "")
        
        # Check if we should run follow-up queries
        follow_up_queries = self._analyze_for_follow_ups(question, base_result)
        
        if follow_up_queries:
            logger.info(f"Running {len(follow_up_queries)} follow-up queries to enhance answer")
            additional_results = self._execute_follow_up_queries(follow_up_queries, question, session_id)
            
            # Merge additional results into base_result
            if additional_results:
                # Add to raw_results
                if not base_result.get("raw_results"):
                    base_result["raw_results"] = []
                base_result["raw_results"].extend(additional_results)
                
                # Regenerate answer with enhanced data
                enhanced_answer = self._regenerate_answer_with_enhanced_data(
                    question, 
                    original_answer,
                    base_result["raw_results"]
                )
                base_result["answer"] = enhanced_answer
        
        # Ensure we have an answer
        if not base_result.get("answer"):
            base_result["answer"] = original_answer or "Unable to generate a complete answer."
        
        # Continue with normal CRO formatting
        # Sanitize the result but keep traces for UI
        sanitized = self.utils.strip_private_keys(base_result)
        enhanced_result = {**sanitized, "_traces": base_result}
        
        # Let LLM format the answer with visualization suggestions
        llm_response = self._get_llm_formatted_response(
            question,
            base_result["answer"],
            base_result.get("raw_results", [])
        )
        
        # Parse LLM response to extract answer and visualization suggestions
        formatted_answer, viz_suggestions = self._parse_llm_response(llm_response)
        
        # Filter visualizations with MORE GENEROUS reflection
        filtered_suggestions = self._filter_visualizations_with_llm(
            question, formatted_answer, viz_suggestions, base_result.get("raw_results", [])
        )
        
        # Generate UI blocks and visualizations based on filtered suggestions
        blocks, visualizations = self._generate_ui_components(
            question,
            base_result.get("raw_results", []),
            formatted_answer,
            filtered_suggestions
        )
        
        # Calculate confidence with visualization penalty
        base_confidence = base_result.get("confidence_score", 0.5)
        
        # Penalize if we suggested visualizations but didn't use them
        suggested_count = len([s for s in viz_suggestions if s.get("type") != "none"])
        actual_count = len([b for b in blocks if b.get("type") in ["chart", "table"]])
        
        if suggested_count > actual_count:
            # 5% penalty per rejected visualization
            penalty = (suggested_count - actual_count) * 0.05
            base_confidence = max(0.1, base_confidence - penalty)
        
        # Ensure all required fields are present for downstream compatibility
        enhanced_result["answer"] = formatted_answer
        enhanced_result["blocks"] = blocks if blocks else [{"type": "markdown", "content": formatted_answer}]
        enhanced_result["visualizations"] = visualizations  # Keep for compatibility but often empty
        
        # Add executive summary
        enhanced_result["executive_summary"] = self.utils.generate_executive_summary(
            question,
            base_result.get("raw_results", [])
        ) or "Analysis complete."
        
        # Add confidence indicator with updated score
        enhanced_result["confidence_indicator"] = self.utils.get_confidence_indicator(
            base_confidence
        ) or {"emoji": "🟡", "text": "Medium confidence", "color": "yellow"}
        enhanced_result["confidence_score"] = base_confidence
        
        # Add KPIs if relevant
        enhanced_result["kpis"] = self.utils.extract_kpis(
            question,
            base_result.get("raw_results", [])
        ) or {}

        # Add sources
        sources = []
        if base_result.get("raw_results"):
            for result_set in base_result["raw_results"]:
                tool = result_set.get("tool", "Unknown")
                query = result_set.get("query", "")
                count = result_set.get("result_count", 0)
                is_follow_up = result_set.get("is_follow_up", False)
                
                if count > 0:
                    sources.append({
                        "tool": tool,
                        "query": query[:100] + "..." if len(query) > 100 else query,
                        "result_count": count,
                        "type": "database" if tool in ["duckdb_sql", "cypher_query"] else "semantic_search",
                        "is_follow_up": is_follow_up
                    })

        enhanced_result["sources"] = sources
        
        # Ensure all fields required by downstream are present
        enhanced_result["evidence"] = base_result.get("evidence", [])
        enhanced_result["errors"] = base_result.get("errors", [])
        enhanced_result["total_execution_time"] = base_result.get("total_execution_time", 0.0)
        enhanced_result["iterations"] = base_result.get("iterations", 0)
        
        # Add reflection metadata
        enhanced_result["reflection_metadata"] = {
            "follow_up_queries_executed": len(follow_up_queries) if follow_up_queries else 0,
            "data_completeness": self._assess_data_completeness(question, base_result.get("raw_results", [])),
            "visualizations_rejected": suggested_count - actual_count if suggested_count > actual_count else 0
        }
        
        return enhanced_result
    
    def _filter_visualizations_with_llm(self, question: str, answer: str, 
                                       viz_suggestions: List[Dict], raw_results: List[Dict]) -> List[Dict]:
        """Use intelligent reflection to decide which visualizations actually add value - MORE GENEROUS"""
        filtered = []
        
        # Check if the question implies visualization need
        viz_keywords = ["show", "chart", "graph", "visualize", "plot", "diagram", "compare", "trend", 
                       "distribution", "breakdown", "analysis", "top", "best", "worst", "by"]
        wants_viz = any(keyword in question.lower() for keyword in viz_keywords)
        
        # Check answer complexity
        answer_lines = answer.split('\n')
        has_complex_data = len(answer_lines) > 5 or any('|' in line for line in answer_lines)
        
        # Check if it's a simple query - BE MORE RESTRICTIVE HERE
        simple_keywords = ["what is the exact", "how much exactly is", "single specific"]
        is_simple_query = any(keyword in question.lower() for keyword in simple_keywords)
        
        # Get result count
        total_results = sum(r.get("result_count", 0) for r in raw_results) if raw_results else 0
        
        # DEFAULT TO INCLUDING VISUALIZATIONS when we have data
        has_meaningful_data = total_results > 0
        
        for suggestion in viz_suggestions:
            viz_type = suggestion.get("type")
            reason = suggestion.get("reason", "")
            
            # Skip if marked as not suitable
            if viz_type == "none" or "not suitable" in reason:
                continue
            
            # Apply MORE GENEROUS filtering based on context
            should_include = False
            
            if viz_type == "bar":
                # Include bar charts more generously
                category_count = self._extract_category_count_from_reason(reason)
                if has_meaningful_data and (category_count >= 3 or wants_viz or has_complex_data):
                    should_include = True
            
            elif viz_type == "table":
                # Include tables for any multi-row results
                if total_results > 5:  # Lowered from 10
                    should_include = True
            
            elif viz_type == "funnel":
                # Funnels are usually intentional - include if data supports it
                if "progression" in reason or "stages" in reason:
                    should_include = True
            
            elif viz_type == "doughnut":
                # Include doughnut charts more generously
                if has_meaningful_data and ("distribution" in reason or category_count >= 3):
                    should_include = True
            
            if should_include:
                filtered.append(suggestion)
        
        # If we have data but no visualizations yet, try to add at least one
        if has_meaningful_data and total_results > 2 and not filtered:
            # Default to a bar chart or table
            if total_results > 10:
                filtered.append({"type": "table", "reason": "showing detailed results"})
            else:
                filtered.append({"type": "bar", "reason": "visualizing data comparison"})
        
        # Maximum 2 visualizations, prioritize charts over tables
        if len(filtered) > 2:
            # Sort to prioritize: funnel > bar > doughnut > table
            priority = {"funnel": 0, "bar": 1, "doughnut": 2, "table": 3}
            filtered.sort(key=lambda x: priority.get(x.get("type"), 4))
            filtered = filtered[:2]
        
        return filtered
    
    def _extract_category_count_from_reason(self, reason: str) -> int:
        """Extract category count from visualization reason"""
        import re
        match = re.search(r'(\d+)\s*(?:categories|items|segments|products)', reason)
        if match:
            return int(match.group(1))
        # Default to 5 if not found (be generous)
        return 5
    
    def _analyze_for_follow_ups(self, question: str, initial_result: Dict[str, Any]) -> List[Dict[str, str]]:
        """Analyze if follow-up queries would enhance the answer"""
        follow_ups = []
        
        # Only analyze if we have initial results
        if not initial_result.get("raw_results") or not initial_result.get("answer"):
            return follow_ups
        
        raw_results = initial_result["raw_results"]
        question_lower = question.lower()
        
        # Pattern 1: Time-based questions might need historical context
        if any(term in question_lower for term in ["trend", "growth", "change", "compare", "vs", "versus"]):
            if not self._has_time_series_data(raw_results):
                follow_ups.append({
                    "query": "Show monthly trends for the past 12 months",
                    "reason": "Adding historical context to show trends over time"
                })
        
        # Pattern 2: Top/bottom questions might need more detail
        if any(term in question_lower for term in ["top", "best", "worst", "bottom", "highest", "lowest"]):
            if self._has_limited_results(raw_results):
                follow_ups.append({
                    "query": "Provide detailed breakdown including all relevant metrics",
                    "reason": "Getting more detailed breakdown of top performers"
                })
        
        # Pattern 3: Revenue/sales questions might need product/segment breakdown
        if any(term in question_lower for term in ["revenue", "sales", "profit", "margin"]):
            if not self._has_dimensional_breakdown(raw_results):
                follow_ups.append({
                    "query": "Break down by product line and customer segment",
                    "reason": "Breaking down by product lines or customer segments"
                })
        
        # Limit to 3 follow-ups maximum
        return follow_ups[:3]
    
    def _execute_follow_up_queries(self, follow_up_queries: List[Dict[str, str]], 
                                   original_question: str, session_id: str) -> List[Dict[str, Any]]:
        """Execute follow-up queries and return results"""
        additional_results = []
        
        for follow_up in follow_up_queries:
            # Contextualize the follow-up query with the original question
            enhanced_query = f"{original_question}. Additionally, {follow_up['query']}"
            
            # Execute the follow-up query
            try:
                follow_up_result = self.base_engine.query(enhanced_query, max_iterations=2, session_id=session_id)
                
                # Mark results as follow-up queries
                if follow_up_result.get("raw_results"):
                    for result_set in follow_up_result["raw_results"]:
                        result_set["is_follow_up"] = True
                        result_set["follow_up_reason"] = follow_up["reason"]
                    
                    additional_results.extend(follow_up_result["raw_results"])
            except Exception as e:
                logger.warning(f"Follow-up query failed: {str(e)}")
        
        return additional_results
    
    def _regenerate_answer_with_enhanced_data(self, question: str, original_answer: str,
                                              all_results: List[Dict[str, Any]]) -> str:
        """Regenerate answer incorporating all data including follow-ups"""
        # Start with the original answer
        answer_parts = [original_answer]
        
        # Separate follow-up results
        follow_up_results = [r for r in all_results if r.get("is_follow_up", False)]
        
        # Add enhanced insights from follow-ups if available
        if follow_up_results:
            answer_parts.append("\n\n**Additional Insights:**")
            
            for result in follow_up_results:
                if result.get("results") and result.get("result_count", 0) > 0:
                    reason = result.get("follow_up_reason", "Additional analysis")
                    answer_parts.append(f"\n- {reason}:")
                    
                    # Add a brief summary of the follow-up findings
                    answer_parts.append(f"  - Found {result['result_count']} additional data points")
                    
                    # Add key insights from the data
                    if result["results"]:
                        sample_data = result["results"][:3]  # First 3 results as example
                        for data_point in sample_data:
                            if isinstance(data_point, dict):
                                key_info = list(data_point.values())[:2]  # First 2 values
                                answer_parts.append(f"  - {', '.join(str(v) for v in key_info)}")
        
        return "\n".join(answer_parts)
    
    def _has_time_series_data(self, raw_results: List[Dict]) -> bool:
        """Check if results contain time-series data"""
        for result_set in raw_results:
            if result_set.get("results") and isinstance(result_set["results"], list):
                if result_set["results"] and isinstance(result_set["results"][0], dict):
                    # Check for date/time columns
                    time_columns = ["date", "month", "quarter", "year", "period", "timestamp"]
                    sample_row = result_set["results"][0]
                    if any(col in sample_row.keys() for col in time_columns):
                        return True
        return False
    
    def _has_limited_results(self, raw_results: List[Dict]) -> bool:
        """Check if results are limited and might benefit from more detail"""
        total_results = sum(r.get("result_count", 0) for r in raw_results)
        return total_results < 10
    
    def _has_dimensional_breakdown(self, raw_results: List[Dict]) -> bool:
        """Check if results have dimensional breakdown"""
        for result_set in raw_results:
            if result_set.get("results") and isinstance(result_set["results"], list):
                if result_set["results"] and isinstance(result_set["results"][0], dict):
                    # Check for dimension columns
                    dimension_columns = ["product", "segment", "category", "region", "channel", "type"]
                    sample_row = result_set["results"][0]
                    if any(col in sample_row.keys() for col in dimension_columns):
                        return True
        return False
    
    def _assess_data_completeness(self, question: str, raw_results: List[Dict]) -> float:
        """Assess how complete the data is for answering the question"""
        if not raw_results:
            return 0.0
        
        completeness_score = 0.5  # Base score
        
        # Check if we have results
        total_results = sum(r.get("result_count", 0) for r in raw_results)
        if total_results > 0:
            completeness_score += 0.2
        
        # Check if we have follow-up data
        has_follow_ups = any(r.get("is_follow_up", False) for r in raw_results)
        if has_follow_ups:
            completeness_score += 0.2
        
        # Check data variety
        unique_tools = set(r.get("tool", "") for r in raw_results)
        if len(unique_tools) > 1:
            completeness_score += 0.1
        
        return min(completeness_score, 1.0)
    
    def _get_llm_formatted_response(self, question: str, raw_answer: str, raw_results: List[Dict]) -> str:
        """Simulate LLM response with MORE GENEROUS visualization analysis"""
        # Analyze data for visualization potential
        viz_analysis = self._analyze_data_for_visualization(raw_results)
        
        # Build visualization suggestions based on data analysis - BE MORE GENEROUS
        viz_suggestions = []
        
        # Only skip viz for truly trivial cases
        if viz_analysis["row_count"] == 0:
            viz_suggestions.append("none:no data available for visualization")
        elif viz_analysis["row_count"] == 1 and "total" in question.lower():
            viz_suggestions.append("none:single value doesn't require visualization")
        else:
            # Be generous with visualizations
            if viz_analysis["suitable_for_funnel"] and viz_analysis["stage_count"] >= 3:
                viz_suggestions.append(f"funnel:showing progression through {viz_analysis['stage_count']} stages")
            
            if viz_analysis["suitable_for_bar"] and viz_analysis["category_count"] >= 2:
                viz_suggestions.append(f"bar:comparing {viz_analysis['category_count']} categories")
            
            if viz_analysis["suitable_for_doughnut"] and viz_analysis["category_count"] >= 2:
                viz_suggestions.append(f"doughnut:showing distribution of {viz_analysis['category_count']} items")
            
            # Be generous with tables
            if viz_analysis["row_count"] >= 5:
                viz_suggestions.append(f"table:detailed view of {min(viz_analysis['row_count'], 20)} records")
        
        if not viz_suggestions:
            viz_suggestions.append("bar:visualizing data results")  # Default fallback
        
        # Format the response
        llm_response = f"""
{raw_answer}

[SUGGESTED_VISUALIZATIONS]
{chr(10).join(viz_suggestions)}
[/SUGGESTED_VISUALIZATIONS]
"""
        return llm_response
    
    def _analyze_data_for_visualization(self, raw_results: List[Dict]) -> Dict[str, Any]:
        """Analyze raw results to determine visualization suitability with MORE GENEROUS heuristics"""
        analysis = {
            "suitable_for_funnel": False,
            "suitable_for_bar": False,
            "suitable_for_doughnut": False,
            "suitable_for_table": False,
            "row_count": 0,
            "category_count": 0,
            "stage_count": 0,
            "has_numeric": False,
            "numeric_columns": [],
            "categorical_columns": [],
            "meaningful_numeric_columns": []
        }
        
        if not raw_results:
            return analysis
        
        # Find the first result set with actual data
        results = None
        for result_set in raw_results:
            if result_set.get("results") and isinstance(result_set["results"], list):
                if result_set["results"] and isinstance(result_set["results"][0], dict):
                    results = result_set["results"]
                    break
        
        if not results:
            return analysis
        
        analysis["row_count"] = len(results)
        
        # Analyze columns
        sample_row = results[0]
        sample_values = {}
        
        for key, value in sample_row.items():
            # Collect sample values for analysis
            sample_values[key] = [row.get(key) for row in results[:50]]
            
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                analysis["numeric_columns"].append(key)
                # Check if it's a meaningful numeric column
                if self._is_meaningful_numeric_column(key, sample_values[key]):
                    analysis["meaningful_numeric_columns"].append(key)
                    analysis["has_numeric"] = True
            elif isinstance(value, str):
                analysis["categorical_columns"].append(key)
                
                # Check for stage-like columns
                if key.lower() in ["stage", "status", "phase", "step", "state"]:
                    # Count unique stages
                    unique_stages = set()
                    for row in results:
                        stage = row.get(key)
                        if stage:
                            unique_stages.add(stage)
                    
                    if 2 <= len(unique_stages) <= 8:
                        analysis["suitable_for_funnel"] = True
                        analysis["stage_count"] = len(unique_stages)
        
        # Determine suitability - BE MORE GENEROUS
        if analysis["meaningful_numeric_columns"] and analysis["categorical_columns"]:
            # Count unique categories in first categorical column
            if analysis["categorical_columns"]:
                cat_col = analysis["categorical_columns"][0]
                unique_cats = set()
                for row in results[:50]:  # Sample first 50 rows
                    cat = row.get(cat_col)
                    if cat:
                        unique_cats.add(cat)
                
                analysis["category_count"] = len(unique_cats)
                
                # More generous bar chart detection
                if analysis["category_count"] >= 2 and analysis["row_count"] >= 2:
                    analysis["suitable_for_bar"] = True
                
                # More generous doughnut detection
                if 2 <= analysis["category_count"] <= 10:
                    analysis["suitable_for_doughnut"] = True
        
        # Table: be generous for any multi-column data
        if len(sample_row.keys()) >= 2 and analysis["row_count"] >= 5:
            analysis["suitable_for_table"] = True
        
        return analysis
    
    def _is_meaningful_numeric_column(self, col_name: str, values: List[Any]) -> bool:
        """Check if a numeric column is meaningful for visualization"""
        col_lower = col_name.lower()
        
        # Skip ID-like columns
        id_indicators = ["_id", "id", "number", "code", "key", "_num", "_no", "_code"]
        if any(indicator in col_lower for indicator in id_indicators):
            return False
        
        # Skip columns with too many unique values (likely IDs)
        if values and len(values) > 0:
            numeric_values = [v for v in values if isinstance(v, (int, float)) and v is not None]
            if numeric_values:
                unique_ratio = len(set(numeric_values)) / len(numeric_values)
                if unique_ratio > 0.9:  # More than 90% unique values
                    return False
        
        return True
    
    def _parse_llm_response(self, llm_response: str) -> Tuple[str, List[Dict]]:
        """Parse LLM response to extract answer and visualization suggestions"""
        viz_suggestions = []
        
        viz_match = re.search(r'\[SUGGESTED_VISUALIZATIONS\](.*?)\[/SUGGESTED_VISUALIZATIONS\]', 
                             llm_response, re.DOTALL)
        
        if viz_match:
            viz_text = viz_match.group(1).strip()
            
            # Parse structured suggestions
            for line in viz_text.split('\n'):
                line = line.strip()
                if ':' in line:
                    parts = line.split(':', 1)
                    if len(parts) == 2:
                        viz_type = parts[0].strip().lower()
                        reason = parts[1].strip()
                        
                        # Include all valid visualization types
                        if viz_type in ["funnel", "bar", "doughnut", "table", "none"]:
                            viz_suggestions.append({
                                "type": viz_type,
                                "reason": reason
                            })
            
            # Remove visualization section from answer
            formatted_answer = llm_response.replace(viz_match.group(0), "").strip()
        else:
            formatted_answer = llm_response.strip()
        
        return formatted_answer, viz_suggestions
    
    def _generate_ui_components(self, question: str, raw_results: List[Dict], 
                            formatted_answer: str, viz_suggestions: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
        """Generate UI blocks and visualizations based on LLM suggestions and data"""
        blocks = []
        visualizations = []
        
        # Add the formatted answer as a markdown block
        blocks.append({"type": "markdown", "content": formatted_answer})
        
        # Only proceed with visualizations if we have data and suggestions
        if not raw_results or not viz_suggestions:
            return blocks, visualizations
        
        # Find the first result set with actual data
        results = None
        for result_set in raw_results:
            if result_set.get("results") and isinstance(result_set["results"], list):
                if result_set["results"] and isinstance(result_set["results"][0], dict):
                    results = result_set["results"]
                    break
        
        if not results:
            return blocks, visualizations
        
        # Process visualization suggestions (max 2)
        charts_added = 0
        tables_added = 0
        
        for suggestion in viz_suggestions:
            if (charts_added + tables_added) >= 2:
                break
            
            viz_type = suggestion["type"]
            
            # Skip "none" type
            if viz_type == "none":
                continue
            
            # Skip if we already have a chart and this is another chart
            if viz_type in ["funnel", "bar", "doughnut"] and charts_added >= 1:
                continue
            
            # Skip if we already have a table
            if viz_type == "table" and tables_added >= 1:
                continue
            
            # Create visualization based on type
            viz = None
            block = None
            
            if viz_type == "funnel":
                viz, block = self._create_funnel_components(results)
                if viz and block:
                    charts_added += 1
            
            elif viz_type == "bar":
                viz, block = self._create_bar_components(results)
                if viz and block:
                    charts_added += 1
            
            elif viz_type == "doughnut":
                viz, block = self._create_doughnut_components(results)
                if viz and block:
                    charts_added += 1
            
            elif viz_type == "table":
                viz, block = self._create_table_components(results)
                if viz and block:
                    tables_added += 1
            
            # Add visualization to visualizations array, block stays separate for metadata
            if viz and block:
                visualizations.append(viz)  # RESTORED: Add to visualizations array
                blocks.append(block)        # Keep block for metadata/tracking
        
        return blocks, visualizations  # Return empty visualizations list
    
    def _create_funnel_components(self, results: List[Dict]) -> Tuple[Optional[Dict], Optional[Dict]]:
        """Create funnel visualization and block"""
        funnel_data = self._extract_funnel_data(results)
        
        if not funnel_data or len(funnel_data) < 2:  # Need at least 2 stages
            return None, None
        
        viz_id = "pipeline_funnel"
        html = self.visualizer.generate_funnel_chart_html(funnel_data, viz_id)
        
        viz = {
            "type": "html",
            "id": viz_id,
            "title": "Pipeline Distribution",
            "content": html
        }
        
        block = {
            "type": "chart",
            "chartType": "funnel",
            "id": viz_id,
            "data": funnel_data
        }
        
        return viz, block
    
    def _create_bar_components(self, results: List[Dict]) -> Tuple[Optional[Dict], Optional[Dict]]:
        """Create bar chart visualization and block"""
        chart_data = self._extract_chart_data(results, "bar")
        
        if not chart_data or len(chart_data) < 2:  # Need at least 2 data points
            return None, None
        
        viz_id = "data_bar_chart"
        html = self.visualizer.generate_bar_chart_html(
            chart_data,
            viz_id,
            {"xAxis": "Category", "yAxis": "Value"}
        )
        
        viz = {
            "type": "html",
            "id": viz_id,
            "title": "Comparative Analysis",
            "content": html
        }
        
        block = {
            "type": "chart",
            "chartType": "bar",
            "id": viz_id,
            "data": chart_data
        }
        
        return viz, block
    
    def _create_doughnut_components(self, results: List[Dict]) -> Tuple[Optional[Dict], Optional[Dict]]:
        """Create doughnut chart visualization and block"""
        # Extract and aggregate data for doughnut
        chart_data = self._extract_doughnut_data(results)
        
        if not chart_data or len(chart_data) < 2:  # Need at least 2 segments
            return None, None
        
        viz_id = "data_doughnut"
        
        # Convert to format expected by visualizer
        doughnut_dict = {item["label"]: item["value"] for item in chart_data}
        
        html = self.visualizer.generate_doughnut_chart_html(doughnut_dict, viz_id)
        
        viz = {
            "type": "html",
            "id": viz_id,
            "title": "Distribution Analysis",
            "content": html
        }
        
        block = {
            "type": "chart",
            "chartType": "doughnut",
            "id": viz_id,
            "data": chart_data
        }
        
        return viz, block
    
    def _create_table_components(self, results: List[Dict]) -> Tuple[Optional[Dict], Optional[Dict]]:
        """Create table visualization and block"""
        if not results or not isinstance(results[0], dict):
            return None, None
        
        # Extract columns and rows
        columns = list(results[0].keys())
        rows = []
        
        # Limit to 20 rows for display
        for row in results[:20]:
            row_data = []
            for col in columns:
                value = row.get(col, "")
                # Format numbers nicely
                if isinstance(value, float):
                    row_data.append(f"{value:,.2f}")
                elif isinstance(value, int):
                    row_data.append(f"{value:,}")
                else:
                    row_data.append(str(value))
            rows.append(row_data)
        
        if not rows:
            return None, None
        
        viz_id = "data_table"
        html = self.visualizer.generate_data_table_html(
            columns,
            rows,
            viz_id,
            f"Query Results ({len(results)} total rows)"
        )
        
        viz = {
            "type": "html",
            "id": viz_id,
            "title": "Detailed Results",
            "content": html
        }
        
        block = {
            "type": "table",
            "id": viz_id,
            "columns": columns,
            "rows": rows
        }
        
        return viz, block
    
    def _extract_funnel_data(self, results: List[Dict]) -> List[Dict]:
        """Extract data specifically for funnel charts"""
        # Find stage column
        stage_column = None
        for key in results[0].keys():
            if key.lower() in ["stage", "status", "phase", "step", "state"]:
                stage_column = key
                break
        
        if not stage_column:
            return []
        
        # Count by stage
        stage_counts = {}
        for row in results:
            stage = row.get(stage_column)
            if stage:  # Only count non-empty stages
                stage_counts[stage] = stage_counts.get(stage, 0) + 1
        
        if len(stage_counts) < 2:  # Need at least 2 stages
            return []
        
        # Sort stages if possible (common patterns)
        stage_order = ["lead", "qualified", "proposal", "negotiation", "closed", "won"]
        sorted_stages = []
        
        # First add stages in known order
        for stage in stage_order:
            for actual_stage in stage_counts.keys():
                if stage in actual_stage.lower():
                    if actual_stage not in sorted_stages:
                        sorted_stages.append(actual_stage)
        
        # Then add any remaining stages
        for stage in sorted(stage_counts.keys()):
            if stage not in sorted_stages:
                sorted_stages.append(stage)
        
        # Create funnel data
        total = sum(stage_counts.values())
        funnel_data = []
        
        for stage in sorted_stages:
            count = stage_counts[stage]
            percentage = (count / total * 100) if total > 0 else 0
            funnel_data.append({
                "stage": stage,
                "count": count,
                "percentage": round(percentage, 1)
            })
        
        return funnel_data
    
    def _extract_chart_data(self, results: List[Dict], chart_type: str) -> List[Dict]:
        """Extract data in format suitable for bar charts with improved filtering"""
        if not results:
            return []
        
        # Find best columns for visualization
        numeric_cols = []
        text_cols = []
        
        sample_row = results[0]
        sample_values = {}
        
        # Collect sample values for all columns
        for key in sample_row.keys():
            sample_values[key] = [row.get(key) for row in results[:50]]
        
        # Categorize columns with improved detection
        for key, value in sample_row.items():
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                # Only add if it's a meaningful numeric column
                if self._is_meaningful_numeric_column(key, sample_values[key]):
                    numeric_cols.append(key)
            elif isinstance(value, str):
                text_cols.append(key)
        
        if not numeric_cols or not text_cols:
            return []
        
        # Use first text column as label and first meaningful numeric as value
        label_col = text_cols[0]
        value_col = numeric_cols[0]
        
        # Aggregate data if needed
        aggregated = {}
        for row in results:
            label = str(row.get(label_col, "Unknown"))
            value = row.get(value_col, 0)
            
            if label and value is not None:
                if label in aggregated:
                    aggregated[label] += value
                else:
                    aggregated[label] = value
        
        # Sort by value and limit
        sorted_items = sorted(aggregated.items(), key=lambda x: x[1], reverse=True)
        limit = 15 if chart_type == "bar" else 20  # Be more generous
        
        chart_data = []
        for label, value in sorted_items[:limit]:
            if value != 0:  # Skip zero values
                chart_data.append({
                    "label": label[:30] + "..." if len(label) > 30 else label,
                    "value": round(value, 2) if isinstance(value, float) else value
                })
        
        return chart_data
    
    def _extract_doughnut_data(self, results: List[Dict]) -> List[Dict]:
        """Extract data specifically for doughnut charts"""
        chart_data = self._extract_chart_data(results, "doughnut")
        
        # For doughnut, limit to top 5-8 items and add "Others" if needed
        if len(chart_data) > 8:
            top_items = chart_data[:7]
            others_value = sum(item["value"] for item in chart_data[7:])
            
            if others_value > 0:
                top_items.append({
                    "label": "Others",
                    "value": round(others_value, 2)
                })
            
            return top_items
        
        return chart_data[:8]
    
    def query_stream(self, question: str, max_iterations: int = 3):
        """Stream query results as they're generated"""
        yield f"Starting analysis for: {question}\n"
        yield "Planning query strategy...\n"
        
        try:
            # Execute the full query
            full_result = self.query(question, max_iterations)
            
            # Report on follow-up queries if executed
            reflection_meta = full_result.get("reflection_metadata", {})
            if reflection_meta.get("follow_up_queries_executed", 0) > 0:
                yield f"Executing {reflection_meta['follow_up_queries_executed']} follow-up queries for deeper insights...\n"
            
            # Stream the answer in chunks
            answer = full_result.get("answer", "")
            chunk_size = 50
            
            for i in range(0, len(answer), chunk_size):
                chunk = answer[i:i + chunk_size]
                yield chunk
                time.sleep(0.05)
            
            # Send trace heartbeat with reflection info
            yield f"\n\n[TRACE_HEARTBEAT]{{'iterations': {full_result.get('_traces', {}).get('iterations', 0)}, 'confidence': {full_result.get('confidence_score', 0.0)}, 'follow_ups': {reflection_meta.get('follow_up_queries_executed', 0)}, 'visualizations_rejected': {reflection_meta.get('visualizations_rejected', 0)}}}\n"
            
        except Exception as e:
            yield f"\n\nError during analysis: {str(e)}\n"
    
    def close(self):
        """Close connections"""
        self.base_engine.close()
