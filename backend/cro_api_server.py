# cro_api_server.py - FastAPI server with CRO-optimized responses and visualizations (OPTIMIZED)
from fastapi import FastAPI, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
import logging
from datetime import datetime
import uvicorn
import asyncio
import json

from chat_engines.cro_query_engine import CROQueryEngine
# from chat_engines.cond_query_engine import CROQueryEngine
from config import API_HOST, API_PORT
from exceptions import NoResultError, BadQuestionError, DataSourceError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="CRO Analytics API",
    description="Executive-optimized sales analytics powered by AI with rich visualizations and data tables",
    version="2.1.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize query engine (singleton)
query_engine = None

# Request/Response models
class QueryRequest(BaseModel):
    question: str = Field(..., description="Natural language question to answer")
    max_iterations: int = Field(3, description="Maximum iterations for multi-step reasoning")
    session_id: str = Field("default", description="Session ID for conversation memory")

class StreamQueryRequest(BaseModel):
    question: str = Field(..., description="Natural language question to answer")
    session_id: str = Field("default", description="Session ID for conversation memory")
    max_iterations: int = Field(3, description="Maximum iterations for multi-step reasoning")

class ConfidenceIndicator(BaseModel):
    emoji: str
    text: str
    color: str

class TableBlock(BaseModel):
    id: str
    columns: List[str]
    rows: List[List[Any]]

class Block(BaseModel):
    type: str  # "headline", "table", "markdown", "kpis", "chart"
    content: Optional[Any] = None

class Visualization(BaseModel):
    type: str = Field(..., description="Type of visualization (html, chart, table, etc.)")
    id: str = Field(..., description="Unique identifier for the visualization")
    title: str = Field(..., description="Title of the visualization")
    content: str = Field(..., description="HTML content or chart specification")
    category: Optional[str] = Field(None, description="Category of visualization (chart, table, metric, etc.)")
 
class QueryResponse(BaseModel):
    answer: str
    executive_summary: str
    confidence_indicator: ConfidenceIndicator
    kpis: Dict[str, Any]
    evidence: List[str]
    confidence_score: float = Field(..., ge=0, le=1)
    errors: List[str]
    total_execution_time: float
    iterations: int
    session_id: str
    conversation_history: Optional[List[Dict[str, Any]]] = Field(None, description="Recent conversation history")
    blocks: List[Dict[str, Any]] = Field(default_factory=list, description="UI-ready content blocks")
    visualizations: List[Dict[str, Any]] = Field(default_factory=list, description="HTML visualizations for direct UI consumption")
    data_sources: List[Dict[str, Any]] = Field(default_factory=list, description="Sources used for the query, including details on queries executed")

class HealthResponse(BaseModel):
    status: str
    timestamp: str
    connections: Dict[str, str]
    version: str = "2.0.0"

class ErrorResponse(BaseModel):
    detail: str
    error_type: str
    suggestions: Optional[List[str]] = None


@app.on_event("startup")
async def startup_event():
    """Initialize the query engine on startup"""
    global query_engine
    try:
        logger.info("Initializing CRO query engine...")
        query_engine = CROQueryEngine()
        logger.info("CRO query engine initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize query engine: {e}")
        raise

@app.on_event("shutdown")
async def shutdown_event():
    """Clean up resources on shutdown"""
    global query_engine
    if query_engine:
        logger.info("Closing query engine connections...")
        query_engine.close()

@app.get("/", response_model=Dict[str, str])
async def root():
    """Root endpoint with API information"""
    return {
        "message": "CRO Analytics API",
        "version": "2.1.0",
        "description": "Executive-optimized analytics with visualizations and data tables",
        "endpoints": {
            "health": "/health",
            "query": "/qa",
            "stream": "/qa/stream",
            "examples": "/qa/examples",
            "render": "/qa/render-visualization"
        },
        "features": {
            "visualizations": "Charts, gauges, heatmaps, and funnels",
            "data_tables": "Formatted HTML tables with all data",
            "streaming": "Real-time response streaming",
            "memory": "Conversation history tracking"
        }
    }

@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint"""
    connections = {}
    
    try:
        # Check DuckDB
        try:
            query_engine.base_engine.duckdb_con.execute("SELECT 1").fetchone()
            connections["duckdb"] = "healthy"
        except:
            connections["duckdb"] = "unhealthy"
        
        # Check Neo4j
        try:
            with query_engine.base_engine.neo4j_driver.session() as session:
                session.run("RETURN 1").single()
            connections["neo4j"] = "healthy"
        except:
            connections["neo4j"] = "unhealthy"
        
        # Check ChromaDB
        try:
            query_engine.base_engine.chroma_client.list_collections()
            connections["chromadb"] = "healthy"
        except:
            connections["chromadb"] = "unhealthy"
        
        # Check OpenAI
        try:
            # Simple test to ensure LLM is accessible
            test_response = query_engine.base_engine.llm.invoke("test")
            connections["openai"] = "healthy"
        except:
            connections["openai"] = "unhealthy"
        
        overall_status = "healthy" if all(v == "healthy" for v in connections.values()) else "degraded"
        
    except Exception as e:
        logger.error(f"Health check error: {e}")
        overall_status = "unhealthy"
        connections["error"] = str(e)
    
    return HealthResponse(
        status=overall_status,
        timestamp=datetime.now().isoformat(),
        connections=connections
    )

@app.post("/qa", response_model=QueryResponse)
async def query_answer(
    request: QueryRequest,
    x_session_id: Optional[str] = Header(None, description="Session ID from header")
):
    """Main query endpoint with CRO-optimized responses and visualizations"""
    try:
        # Use session ID from request body or header
        session_id = request.session_id
        if x_session_id and request.session_id == "default":
            session_id = x_session_id
            
        logger.info(f"Processing CRO query: {request.question} (session: {session_id})")
        
        # OPTIMIZED: Single call to CRO engine which internally calls base engine once
        cro_result = query_engine.query(
            question=request.question,
            max_iterations=request.max_iterations,
            session_id=session_id  # Pass session through
        )
        # print(cro_result)
        
        # Get conversation history for this session
        conversation_history = query_engine.base_engine.memory.get(session_id, [])
        
        # data_sources = None
        # if cro_result.get("data_sources"):
        #     # Check if sources is a list (raw format) or already processed
        #     sources_data = cro_result["data_sources"].get("sources", [])
        #     if sources_data and isinstance(sources_data[0], dict) and "tool" in sources_data[0]:
        #         # Raw format - need to process
        #         data_sources = DataSourceInfo.from_raw_sources(sources_data)
        #     else:
        #         # Already processed format
        #         data_sources = DataSourceInfo(**cro_result["data_sources"])
        
        # Prepare response with visualizations and source information
        response = QueryResponse(
            answer=cro_result["answer"],
            executive_summary=cro_result.get("executive_summary", ""),
            confidence_indicator=ConfidenceIndicator(**cro_result.get("confidence_indicator", {
                "emoji": "🟡", "text": "Medium confidence", "color": "yellow"
            })),
            kpis=cro_result.get("kpis", {}),
            evidence=cro_result.get("evidence", []),
            confidence_score=cro_result.get("confidence_score", 0.5),
            errors=cro_result.get("errors", []),
            total_execution_time=cro_result.get("total_execution_time", 0.0),
            iterations=cro_result.get("iterations", 0),
            session_id=session_id,
            conversation_history=conversation_history,
            blocks=cro_result.get("blocks", []),  # Include UI blocks
            visualizations=cro_result.get("visualizations", []),  # Include visualizations
            data_sources=cro_result.get("sources", [])
        )

        print(response.visualizations)

        # Log source information
        # if response.data_sources:
        #     logger.info(f"Data sources used: {response.data_sources.sources}")
        #     logger.info(f"Total records analyzed: {response.data_sources.total_records_analyzed}")
        #     logger.info(f"Query types: {response.data_sources.query_types}")
        
        # Log visualization info for debugging
        if response.visualizations:
            logger.info(f"Response includes {len(response.visualizations)} visualizations")
            for viz in response.visualizations:
                logger.info(f"  - Visualization: {viz.get('id')} ({viz.get('type')}) - {viz.get('title')}")
                # Categorize visualizations for logging
                if 'table' in viz.get('id', '').lower() or 'table' in viz.get('title', '').lower():
                    logger.info(f"    Category: Data Table")
                elif any(chart in viz.get('id', '').lower() for chart in ['chart', 'gauge', 'heatmap', 'funnel']):
                    logger.info(f"    Category: Chart/Visualization")
        print(response)
        return response
        
    except NoResultError as e:
        logger.warning(f"No results found: {e}")
        raise HTTPException(status_code=404, detail=str(e))
    except BadQuestionError as e:
        logger.warning(f"Bad question: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error processing query: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/qa/stream")
async def stream_query(
    request: StreamQueryRequest,
    x_session_id: Optional[str] = Header(None, description="Session ID from header")
):
    """Stream the final answer only, not intermediate steps"""
    
    # Use session ID from request body or header
    session_id = request.session_id
    if x_session_id and request.session_id == "default":
        session_id = x_session_id
    
    async def generate_response():
        try:
            # Execute the full query first (non-streaming)
            loop = asyncio.get_event_loop()
            
            # OPTIMIZED: Single call to CRO engine
            cro_result = await loop.run_in_executor(
                None,
                lambda: query_engine.query(
                    request.question,
                    request.max_iterations,
                    session_id
                )
            )
            
            # Stream the formatted answer in chunks
            answer = cro_result.get("answer", "")
            
            # Send metadata first (including blocks, visualizations, and data sources)
            metadata = {
                "type": "metadata",
                "executive_summary": cro_result.get("executive_summary", ""),
                "confidence_indicator": cro_result.get("confidence_indicator", {}),
                "kpis": cro_result.get("kpis", {}),
                "blocks": cro_result.get("blocks", []),  # Include UI blocks
                "visualizations": cro_result.get("visualizations", []),  # Include visualizations
                "data_sources": cro_result.get("data_sources", {}),  # Include data sources
                "session_id": session_id
            }
            yield f"data: {json.dumps(metadata)}\n\n"
            
            # Stream answer in chunks
            chunk_size = 100
            for i in range(0, len(answer), chunk_size):
                chunk = answer[i:i + chunk_size]
                chunk_data = {
                    "type": "answer_chunk",
                    "content": chunk
                }
                yield f"data: {json.dumps(chunk_data)}\n\n"
                await asyncio.sleep(0.02)  # Small delay for streaming effect
            
            # Send completion with final metadata
            completion_data = {
                "type": "complete",
                "confidence_score": cro_result.get("confidence_score", 0.0),
                "total_execution_time": cro_result.get("total_execution_time", 0.0),
                "iterations": cro_result.get("iterations", 0),
                "evidence_count": len(cro_result.get("evidence", [])),
                "visualization_count": len(cro_result.get("visualizations", [])),
                "visualization_types": {
                    "charts": len([v for v in cro_result.get("visualizations", []) if 'chart' in v.get('id', '').lower() or 'gauge' in v.get('id', '').lower()]),
                    "tables": len([v for v in cro_result.get("visualizations", []) if 'table' in v.get('id', '').lower()])
                },
                "sources_used": cro_result.get("data_sources", {}).get("sources", [])
            }
            yield f"data: {json.dumps(completion_data)}\n\n"
            
        except NoResultError as e:
            error_data = {
                "type": "error",
                "error_type": "no_results",
                "message": str(e)
            }
            yield f"data: {json.dumps(error_data)}\n\n"
        except BadQuestionError as e:
            error_data = {
                "type": "error",
                "error_type": "bad_question",
                "message": str(e)
            }
            yield f"data: {json.dumps(error_data)}\n\n"
        except Exception as e:
            error_data = {
                "type": "error",
                "error_type": "internal_error",
                "message": str(e)
            }
            yield f"data: {json.dumps(error_data)}\n\n"
            logger.error(f"Streaming error: {e}")
        
        # Send done signal
        yield "data: [DONE]\n\n"
    
    return StreamingResponse(
        generate_response(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
            "Access-Control-Allow-Origin": "*"
        }
    )

import pandas as pd
import numpy as np
from datetime import datetime
import json

@app.get("/qa/summary-stats")
async def get_summary_stats():
    """Get summary statistics for dashboard visualization"""
    try:
        logger.info("Generating summary statistics for dashboard")
        
        # Query to get all data from opportunities table
        query = """
        SELECT * FROM opportunities
        LIMIT 10000
        """
        
        # Execute query
        df = query_engine.base_engine.duckdb_con.execute(query).df()
        
        # Identify column types
        numeric_columns = df.select_dtypes(include=['int64', 'float64']).columns.tolist()
        categorical_columns = df.select_dtypes(include=['object']).columns.tolist()
        date_columns = []
        
        # Common date patterns to try
        date_formats = [
            '%Y-%m-%d',
            '%m/%d/%Y',
            '%d/%m/%Y',
            '%Y/%m/%d',
            '%m-%d-%Y',
            '%d-%m-%Y',
            '%Y-%m-%d %H:%M:%S',
            '%m/%d/%Y %H:%M:%S'
        ]
        
        # Try to identify date columns
        for col in categorical_columns[:]:  # Use slice to avoid modifying list during iteration
            try:
                sample = df[col].dropna().head(10)
                if len(sample) > 0:
                    # Try each date format
                    for fmt in date_formats:
                        try:
                            parsed = pd.to_datetime(sample, format=fmt, errors='coerce')
                            if parsed.notna().sum() > len(sample) * 0.5:
                                date_columns.append(col)
                                break
                        except:
                            continue
                    
                    # If no format worked, try generic parsing
                    if col not in date_columns:
                        try:
                            parsed = pd.to_datetime(sample, errors='coerce', infer_datetime_format=True)
                            if parsed.notna().sum() > len(sample) * 0.5:
                                date_columns.append(col)
                        except:
                            pass
            except:
                continue
        
        # Remove date columns from categorical
        categorical_columns = [col for col in categorical_columns if col not in date_columns]
        
        # Calculate summary statistics
        summary_stats = {
            "total_records": len(df),
            "numeric_stats": {},
            "categorical_stats": {},
            "date_stats": {}
        }
        
        # Numeric column statistics
        for col in numeric_columns:
            if 'prob' in col.lower():
                continue
            col_data = df[col].dropna()
            if len(col_data) > 0:
                summary_stats["numeric_stats"][col] = {
                    "count": int(col_data.count()),
                    "mean": float(col_data.mean()),
                    "median": float(col_data.median()),
                    "std": float(col_data.std()) if len(col_data) > 1 else 0,
                    "min": float(col_data.min()),
                    "max": float(col_data.max()),
                    "sum": float(col_data.sum()),
                    "q25": float(col_data.quantile(0.25)),
                    "q75": float(col_data.quantile(0.75)),
                    "skewness": float(col_data.skew()) if len(col_data) > 2 else 0,
                    "null_count": int(df[col].isna().sum()),
                    "null_percentage": float((df[col].isna().sum() / len(df)) * 100)
                }
                
                # Calculate trend (mock - in real scenario, compare with previous period)
                summary_stats["numeric_stats"][col]["trend"] = float(np.random.uniform(-10, 10))
        
        # Categorical column statistics
        for col in categorical_columns:
            value_counts = df[col].value_counts()
            summary_stats["categorical_stats"][col] = {
                "unique_count": int(df[col].nunique()),
                "most_common": str(value_counts.index[0]) if len(value_counts) > 0 else None,
                "most_common_count": int(value_counts.iloc[0]) if len(value_counts) > 0 else 0,
                "null_count": int(df[col].isna().sum()),
                "null_percentage": float((df[col].isna().sum() / len(df)) * 100),
                "top_values": [
                    {"value": str(val), "count": int(count)} 
                    for val, count in value_counts.head(10).items()
                ]
            }
        
        # Date column statistics
        for col in date_columns:
            try:
                date_data = pd.to_datetime(df[col], errors='coerce').dropna()
                if len(date_data) > 0:
                    summary_stats["date_stats"][col] = {
                        "min_date": date_data.min().strftime('%Y-%m-%d'),
                        "max_date": date_data.max().strftime('%Y-%m-%d'),
                        "date_range_days": int((date_data.max() - date_data.min()).days),
                        "null_count": int(df[col].isna().sum())
                    }
            except:
                continue
        
        # Prepare sample data for visualization
        sample_data = []
        sample_df = df.head(1000).copy()
        
        # Convert all data to JSON-serializable format
        for idx, row in sample_df.iterrows():
            record = {}
            for col in sample_df.columns:
                # if 'prob' not in str(col).lower():
                # Skip probability columns
                if '%' in str(col).lower():
                    continue
                value = row[col]
                
                # Handle different data types
                if pd.isna(value):
                    record[col] = None
                elif isinstance(value, (np.integer, np.int64, np.int32)) and not "PROB" in str(col):
                    record[col] = int(value)
                elif isinstance(value, (np.floating, np.float64, np.float32)):
                    record[col] = float(value)
                elif isinstance(value, (pd.Timestamp, datetime)):
                    record[col] = value.strftime('%Y-%m-%d %H:%M:%S')
                elif hasattr(value, 'isoformat'):
                    record[col] = value.isoformat()
                elif isinstance(value, (np.bool_, bool)):
                    record[col] = bool(value)
                else:
                    # Convert everything else to string
                    record[col] = str(value)
            
            sample_data.append(record)
        
        # Metadata for dashboard
        metadata = {
            "numeric_columns": numeric_columns,
            "categorical_columns": categorical_columns,
            "date_columns": date_columns,
            "total_columns": len(df.columns),
            "column_names": list(df.columns)
        }
        
        response = {
            "summary_stats": summary_stats,
            "metadata": metadata,
            "sample_data": sample_data,
            "generated_at": datetime.now().isoformat()
        }
        
        logger.info(f"Summary stats generated successfully: {len(df)} records analyzed")
        
        # Ensure the response is JSON serializable
        return JSONResponse(content=json.loads(json.dumps(response, default=str)))
        
    except Exception as e:
        logger.error(f"Error generating summary statistics: {str(e)}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=str(e))

# Add these endpoints to your cro_api_server.py file

# 1. Dynamic Filters Endpoint - Get available filter values based on current selection
@app.post("/qa/filters")
async def get_dynamic_filters(filters: Dict[str, Any] = {}):
    """Get available filter values based on current filter selection"""
    try:
        # Build WHERE clause from existing filters
        where_conditions = []
        for col, val in filters.items():
            if val is not None:
                where_conditions.append(f"{col} = '{val}'")
        
        where_clause = f"WHERE {' AND '.join(where_conditions)}" if where_conditions else ""
        
        # Get unique values for each categorical column
        filter_options = {}
        
        query = f"""
        SELECT * FROM opportunities
        {where_clause}
        LIMIT 10000
        """
        
        df = query_engine.base_engine.duckdb_con.execute(query).df()
        
        # Get categorical columns
        categorical_columns = df.select_dtypes(include=['object']).columns.tolist()
        
        for col in categorical_columns:
            value_counts = df[col].value_counts().head(20)
            filter_options[col] = [
                {
                    "value": str(val),
                    "count": int(count),
                    "percentage": float((count / len(df)) * 100)
                }
                for val, count in value_counts.items()
            ]
        
        return JSONResponse(content={
            "filters": filter_options,
            "total_records": len(df),
            "applied_filters": filters
        })
        
    except Exception as e:
        logger.error(f"Error getting filters: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# 2. Trend Analysis Endpoint - Get time-based trends with predictions
@app.post("/qa/trends")
async def get_trend_analysis(request: Dict[str, Any]):
    """Get trend analysis with historical data and simple predictions"""
    try:
        metric = request.get("metric")
        dimension = request.get("dimension")
        date_column = request.get("date_column", "Close Date")
        filters = request.get("filters", {})
        
        # Build query
        where_conditions = []
        for col, val in filters.items():
            if val is not None:
                where_conditions.append(f"{col} = '{val}'")
        
        where_clause = f"WHERE {' AND '.join(where_conditions)}" if where_conditions else ""
        
        query = f"""
        SELECT 
            DATE_TRUNC('month', "{date_column}") as period,
            {f'"{dimension}",' if dimension else ''}
            COUNT(*) as count,
            AVG("{metric}") as avg_value,
            SUM("{metric}") as total_value,
            MIN("{metric}") as min_value,
            MAX("{metric}") as max_value
        FROM opportunities
        {where_clause}
        GROUP BY period{f', "{dimension}"' if dimension else ''}
        ORDER BY period
        """
        
        df = query_engine.base_engine.duckdb_con.execute(query).df()
        
        # Calculate growth rates
        trends = []
        for _, row in df.iterrows():
            trends.append({
                "period": row['period'].strftime('%Y-%m-%d'),
                "dimension": row.get(dimension) if dimension else "All",
                "metrics": {
                    "count": int(row['count']),
                    "average": float(row['avg_value']),
                    "total": float(row['total_value']),
                    "min": float(row['min_value']),
                    "max": float(row['max_value'])
                }
            })
        
        # Calculate simple growth prediction
        if len(df) > 1:
            recent_avg = df.tail(3)['total_value'].mean()
            previous_avg = df.head(3)['total_value'].mean()
            growth_rate = ((recent_avg - previous_avg) / previous_avg * 100) if previous_avg > 0 else 0
        else:
            growth_rate = 0
        
        return JSONResponse(content={
            "trends": trends,
            "growth_rate": float(growth_rate),
            "prediction": "increasing" if growth_rate > 5 else "decreasing" if growth_rate < -5 else "stable"
        })
        
    except Exception as e:
        logger.error(f"Error analyzing trends: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# 3. Correlation Analysis Endpoint
@app.post("/qa/correlations")
async def get_correlations(columns: List[str] = None):
    """Get correlation matrix for numeric columns"""
    try:
        query = "SELECT * FROM opportunities LIMIT 10000"
        df = query_engine.base_engine.duckdb_con.execute(query).df()
        
        # Get numeric columns
        numeric_cols = columns or df.select_dtypes(include=['int64', 'float64']).columns.tolist()
        
        # Calculate correlations
        corr_matrix = df[numeric_cols].corr()
        
        # Find strong correlations
        strong_correlations = []
        for i in range(len(numeric_cols)):
            for j in range(i+1, len(numeric_cols)):
                corr_value = corr_matrix.iloc[i, j]
                if abs(corr_value) > 0.5:  # Strong correlation threshold
                    strong_correlations.append({
                        "column1": numeric_cols[i],
                        "column2": numeric_cols[j],
                        "correlation": float(corr_value),
                        "strength": "strong" if abs(corr_value) > 0.7 else "moderate"
                    })
        
        return JSONResponse(content={
            "correlation_matrix": corr_matrix.to_dict(),
            "strong_correlations": strong_correlations,
            "columns": numeric_cols
        })
        
    except Exception as e:
        logger.error(f"Error calculating correlations: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# 4. AI-Powered Insights Endpoint
@app.post("/qa/ai-insights")
async def get_ai_insights(request: Dict[str, Any]):
    """Generate AI-powered insights about the data"""
    try:
        stats = request.get("stats", {})
        focus_area = request.get("focus_area", "general")  # general, performance, trends, anomalies
        
        # Prepare context for LLM
        context = f"""
        Analyze the following business data statistics and provide actionable insights:
        
        Total Records: {stats.get('total_records', 0)}
        
        Numeric Statistics:
        {json.dumps(stats.get('numeric_stats', {}), indent=2)}
        
        Categorical Distribution:
        {json.dumps(stats.get('categorical_stats', {}), indent=2)}
        
        Date Range Statistics:
        {json.dumps(stats.get('date_stats', {}), indent=2)}
        
        Focus Area: {focus_area}
        """
        
        # Use the query engine's LLM to generate insights
        prompt = f"""
        {context}
        
        Based on this data, provide 3-5 specific, actionable insights focusing on {focus_area}.
        Format each insight as:
        - Title: [Brief title]
        - Insight: [Specific observation]
        - Action: [Recommended action]
        
        Be specific with numbers and percentages. Focus on business impact.
        """
        
        response = query_engine.base_engine.llm.invoke(prompt)
        
        # Parse the response into structured insights
        insights = []
        lines = response.content.split('\n')
        current_insight = {}
        
        for line in lines:
            if line.strip().startswith('- Title:'):
                if current_insight:
                    insights.append(current_insight)
                current_insight = {'title': line.replace('- Title:', '').strip()}
            elif line.strip().startswith('- Insight:'):
                current_insight['insight'] = line.replace('- Insight:', '').strip()
            elif line.strip().startswith('- Action:'):
                current_insight['action'] = line.replace('- Action:', '').strip()
        
        if current_insight:
            insights.append(current_insight)
        
        # Generate a summary
        summary_prompt = f"""
        Based on the data statistics provided, write a 2-3 sentence executive summary 
        highlighting the most important finding and its business impact.
        
        {context}
        """
        
        summary_response = query_engine.base_engine.llm.invoke(summary_prompt)
        
        return JSONResponse(content={
            "insights": insights,
            "executive_summary": summary_response.content,
            "focus_area": focus_area,
            "generated_at": datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error generating AI insights: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# 5. Predictive Analytics Endpoint
@app.post("/qa/predictions")
async def get_predictions(request: Dict[str, Any]):
    """Simple predictive analytics based on historical trends"""
    try:
        metric = request.get("metric", "Amount")
        period = request.get("period", "month")  # day, week, month, quarter
        dimension = request.get("dimension")
        
        # Get historical data
        query = f"""
        SELECT 
            DATE_TRUNC('{period}', "Close Date") as period,
            {f'"{dimension}",' if dimension else ''}
            SUM("{metric}") as total,
            COUNT(*) as count,
            AVG("{metric}") as average
        FROM opportunities
        WHERE "Close Date" IS NOT NULL
        GROUP BY period{f', "{dimension}"' if dimension else ''}
        ORDER BY period
        """
        
        df = query_engine.base_engine.duckdb_con.execute(query).df()
        
        if len(df) < 3:
            return JSONResponse(content={
                "error": "Insufficient data for predictions",
                "min_required": 3,
                "available": len(df)
            })
        
        # Simple moving average prediction
        predictions = []
        
        if dimension:
            for dim_value in df[dimension].unique():
                dim_df = df[df[dimension] == dim_value].tail(6)
                if len(dim_df) >= 3:
                    # Calculate trend
                    recent_avg = dim_df.tail(3)['total'].mean()
                    older_avg = dim_df.head(3)['total'].mean()
                    trend = (recent_avg - older_avg) / older_avg if older_avg > 0 else 0
                    
                    # Simple projection
                    last_value = dim_df.iloc[-1]['total']
                    next_value = last_value * (1 + trend)
                    
                    predictions.append({
                        "dimension": str(dim_value),
                        "current_value": float(last_value),
                        "predicted_value": float(next_value),
                        "trend_percentage": float(trend * 100),
                        "confidence": "high" if abs(trend) < 0.2 else "medium" if abs(trend) < 0.5 else "low"
                    })
        else:
            # Overall prediction
            recent_avg = df.tail(3)['total'].mean()
            older_avg = df.head(3)['total'].mean()
            trend = (recent_avg - older_avg) / older_avg if older_avg > 0 else 0
            
            last_value = df.iloc[-1]['total']
            next_value = last_value * (1 + trend)
            
            predictions.append({
                "dimension": "Overall",
                "current_value": float(last_value),
                "predicted_value": float(next_value),
                "trend_percentage": float(trend * 100),
                "confidence": "high" if abs(trend) < 0.2 else "medium" if abs(trend) < 0.5 else "low"
            })
        
        return JSONResponse(content={
            "predictions": predictions,
            "metric": metric,
            "period": period,
            "method": "moving_average_trend"
        })
        
    except Exception as e:
        logger.error(f"Error generating predictions: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# 6. Anomaly Detection Endpoint
@app.post("/qa/anomalies")
async def detect_anomalies(request: Dict[str, Any]):
    """Detect anomalies in the data"""
    try:
        metric = request.get("metric", "Amount")
        threshold = request.get("threshold", 2)  # Standard deviations
        
        query = f"""
        SELECT *
        FROM opportunities
        WHERE "{metric}" IS NOT NULL
        """
        
        df = query_engine.base_engine.duckdb_con.execute(query).df()
        
        # Calculate statistics
        mean = df[metric].mean()
        std = df[metric].std()
        
        # Find anomalies
        df['z_score'] = (df[metric] - mean) / std
        anomalies = df[abs(df['z_score']) > threshold]
        
        # Group anomalies by category
        anomaly_summary = []
        for col in df.select_dtypes(include=['object']).columns[:5]:  # Top 5 categorical columns
            if col in anomalies.columns:
                grouped = anomalies.groupby(col).agg({
                    metric: ['count', 'mean', 'sum'],
                    'z_score': 'mean'
                }).reset_index()
                
                for _, row in grouped.head(10).iterrows():
                    anomaly_summary.append({
                        "dimension": col,
                        "value": str(row[col]),
                        "anomaly_count": int(row[metric]['count']),
                        "avg_value": float(row[metric]['mean']),
                        "total_value": float(row[metric]['sum']),
                        "avg_deviation": float(abs(row['z_score']['mean']))
                    })
        
        return JSONResponse(content={
            "total_anomalies": len(anomalies),
            "percentage": float((len(anomalies) / len(df)) * 100),
            "threshold_used": threshold,
            "metric_analyzed": metric,
            "anomaly_summary": sorted(anomaly_summary, key=lambda x: x['avg_deviation'], reverse=True)[:10]
        })
        
    except Exception as e:
        logger.error(f"Error detecting anomalies: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    
@app.delete("/qa/session/{session_id}")
async def clear_session(session_id: str):
    """Clear conversation memory for a specific session"""
    try:
        if session_id in query_engine.base_engine.memory:
            del query_engine.base_engine.memory[session_id]
            return {"message": f"Session {session_id} cleared successfully"}
        else:
            return {"message": f"Session {session_id} not found or already empty"}
    except Exception as e:
        logger.error(f"Error clearing session: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/qa/sessions")
async def list_sessions():
    """List all active sessions with memory"""
    try:
        sessions = []
        for session_id, history in query_engine.base_engine.memory.items():
            sessions.append({
                "session_id": session_id,
                "turn_count": len(history),
                "last_interaction": history[-1]["timestamp"] if history else None
            })
        return {"sessions": sessions, "total": len(sessions)}
    except Exception as e:
        logger.error(f"Error listing sessions: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/qa/visualization/{viz_id}")
async def get_visualization(viz_id: str):
    """Get a specific visualization by ID (if stored)"""
    # This endpoint could be used to retrieve cached visualizations
    # For now, visualizations are embedded in the response
    return {
        "message": "Visualizations are embedded in query responses",
        "viz_id": viz_id,
        "note": "Each visualization includes both charts and data tables"
    }

@app.get("/qa/examples")
async def get_examples():
    """Get example queries and expected visualization types"""
    return {
        "examples": [
            {
                "question": "What are the top 5 accounts by total opportunity value?",
                "expected_visualizations": [
                    {"type": "chart", "description": "Doughnut chart showing account distribution"},
                    {"type": "table", "description": "Detailed table with account metrics and actions"}
                ]
            },
            {
                "question": "How many opportunities are closing this month?",
                "expected_visualizations": [
                    {"type": "gauge", "description": "Visual gauge showing closing status"},
                    {"type": "table", "description": "Summary table with metrics and targets"}
                ]
            },
            {
                "question": "Show me at-risk opportunities that need immediate attention",
                "expected_visualizations": [
                    {"type": "heatmap", "description": "Risk heatmap visualization"},
                    {"type": "table", "description": "Detailed at-risk opportunities table"}
                ]
            },
            {
                "question": "What percentage of opportunities are in each stage?",
                "expected_visualizations": [
                    {"type": "funnel", "description": "Sales pipeline funnel chart"},
                    {"type": "table", "description": "Stage distribution table with health indicators"}
                ]
            },
            {
                "question": "What is the average deal size by stage?",
                "expected_visualizations": [
                    {"type": "bar", "description": "Bar chart comparing deal sizes"},
                    {"type": "table", "description": "Metrics table with variance analysis"}
                ]
            }
        ]
    }

@app.post("/qa/render-visualization")
async def render_visualization(viz_content: Dict[str, str]):
    """Render a visualization as a standalone HTML page"""
    try:
        content = viz_content.get("content", "")
        title = viz_content.get("title", "Visualization")
        
        # Wrap in a complete HTML document
        full_html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>{title}</title>
            <style>
                body {{
                    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                    margin: 0;
                    padding: 20px;
                    background-color: #f9fafb;
                }}
                .container {{
                    max-width: 1200px;
                    margin: 0 auto;
                    background-color: white;
                    padding: 20px;
                    border-radius: 8px;
                    box-shadow: 0 1px 3px rgba(0,0,0,0.1);
                }}
            </style>
        </head>
        <body>
            <div class="container">
                {content}
            </div>
        </body>
        </html>
        """
        
        from fastapi.responses import HTMLResponse
        return HTMLResponse(content=full_html)
        
    except Exception as e:
        logger.error(f"Error rendering visualization: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Exception handlers
@app.exception_handler(NoResultError)
async def no_result_handler(request, exc):
    return JSONResponse(
        status_code=404,
        content={
            "detail": str(exc),
            "error_type": "no_results",
            "suggestions": [
                "Try broadening your search criteria",
                "Check if the data exists in the specified time range",
                "Verify entity names are spelled correctly"
            ]
        }
    )

@app.exception_handler(BadQuestionError)
async def bad_question_handler(request, exc):
    return JSONResponse(
        status_code=400,
        content={
            "detail": str(exc),
            "error_type": "invalid_question",
            "suggestions": [
                "Ensure your question is complete",
                "Try rephrasing with specific entity names",
                "Include relevant context or time frames"
            ]
        }
    )

@app.exception_handler(DataSourceError)
async def data_source_handler(request, exc):
    return JSONResponse(
        status_code=503,
        content={
            "detail": str(exc),
            "error_type": "data_source_unavailable",
            "affected_sources": str(exc).split(":")[0] if ":" in str(exc) else "unknown"
        }
    )

if __name__ == "__main__":
    uvicorn.run(
        "cro_api_server:app",
        host=API_HOST,
        port=API_PORT,
        reload=True,
        log_level="info"
    )


