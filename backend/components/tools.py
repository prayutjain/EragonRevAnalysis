# tools.py - All tool execution methods
import json
import logging
import re
from typing import Dict, List, Any
import duckdb
from neo4j import GraphDatabase
import chromadb
from langchain_core.messages import HumanMessage

import os

logger = logging.getLogger(__name__)


    
class ToolExecutor:
    """Handles execution of all data retrieval tools"""
    
    def __init__(self, duckdb_con, neo4j_driver, chroma_client, llm):
        self.duckdb_con = duckdb_con
        self.neo4j_driver = neo4j_driver
        self.chroma_client = chroma_client
        self.llm = llm
        
        
        
    
    def extract_search_terms_from_sql(self, sql_query: str) -> str:
        """Extract search terms from SQL for vector fallback"""
        # Look for LIKE patterns
        like_pattern = r"LIKE\s+'%([^%]+)%'"
        like_matches = re.findall(like_pattern, sql_query, re.IGNORECASE)
        
        # Look for equality conditions
        eq_pattern = r"=\s+'([^']+)'"
        eq_matches = re.findall(eq_pattern, sql_query, re.IGNORECASE)
        
        # Combine terms
        terms = like_matches + eq_matches
        
        if terms:
            return " ".join(terms)
        
        # If no specific terms found, try to extract from table/column names mentioned
        if "cloud" in sql_query.lower() or "infrastructure" in sql_query.lower():
            return "cloud infrastructure"
        
        return ""
    
    def hydrate_vector_hits(self, vector_results: List[Dict]) -> List[Dict]:
        """Hydrate vector search results with full records from DuckDB"""
        hydrated = []
        
        # Extract IDs from vector results
        ids = []
        for result in vector_results:
            if result.get('id'):
                ids.append(result['id'])
        
        if not ids:
            return hydrated
        
        # Query DuckDB for full records
        try:
            # Build safe parameterized query
            placeholders = ",".join(["?" for _ in ids])
            query = f"SELECT * FROM opportunities WHERE id IN ({placeholders})"
            
            # Execute with parameters
            result_df = self.duckdb_con.execute(query, ids).fetchdf()
            hydrated = result_df.to_dict('records')
            
        except Exception as e:
            logger.error(f"Failed to hydrate vector results: {e}")
        
        return hydrated
    
    def auto_fix_sql(self, query: str, error: str) -> str:
        """Use LLM to fix SQL query based on error message"""
        fix_prompt = f"""Fix this SQL query for DuckDB. The query failed with this error:

Query: {query}
Error: {error}

Common issues:
- Column names with spaces/special chars must be in double quotes
- Date functions: use strftime(date_column, format) not strftime(format, date_column)
- Use proper date comparisons for DATE type columns

Return ONLY the fixed SQL query, nothing else."""
        
        response = self.llm.invoke([HumanMessage(content=fix_prompt)])
        return response.content.strip()
    
    def execute_sql(self, query: str) -> List[Dict[str, Any]]:
        """Execute SQL query on DuckDB with auto-repair"""
        logger.info(f"Executing SQL: {query}")
        
        try:
            result = self.duckdb_con.execute(query).fetchdf()
            return result.to_dict('records')
        except Exception as e:
            logger.warning(f"SQL failed: {e}")
            
            # Try to auto-fix the query
            if "Binder Error" in str(e) or "Parser Error" in str(e):
                try:
                    fixed_query = self.auto_fix_sql(query, str(e))
                    logger.info(f"Retrying with fixed query: {fixed_query}")
                    result = self.duckdb_con.execute(fixed_query).fetchdf()
                    return result.to_dict('records')
                except Exception as e2:
                    logger.error(f"Auto-fix failed: {e2}")
                    raise e
            else:
                raise e
    
    def auto_fix_cypher(self, query: str, error: str) -> str:
        """Use LLM to fix Cypher query based on error message"""
        fix_prompt = f"""Fix this Cypher query for Neo4j. The query failed with this error:

Query: {query}
Error: {error}

Common issues:
- WHERE must come BEFORE RETURN
- For aggregations, use WITH ... WHERE ... RETURN pattern
- Node labels are case-sensitive: Opportunities, AccountAndContact1

Return ONLY the fixed Cypher query, nothing else."""
        
        response = self.llm.invoke([HumanMessage(content=fix_prompt)])
        return response.content.strip()
    
    def execute_cypher(self, query: str) -> List[Dict[str, Any]]:
        """Execute Cypher query on Neo4j with auto-repair"""
        logger.info(f"Executing Cypher: {query}")
        
        try:
            with self.neo4j_driver.session() as session:
                result = session.run(query)
                records = []
                for record in result:
                    # Convert Neo4j record to plain dict
                    record_dict = {}
                    for key, value in record.items():
                        if hasattr(value, '__dict__'):  # Neo4j Node/Relationship
                            # Extract properties from node
                            record_dict[key] = dict(value)
                        else:
                            record_dict[key] = value
                    records.append(record_dict)
                return records
        except Exception as e:
            logger.warning(f"Cypher failed: {e}")
            
            # Try to auto-fix the query
            if "SyntaxError" in str(e) or "WHERE" in str(e):
                try:
                    fixed_query = self.auto_fix_cypher(query, str(e))
                    logger.info(f"Retrying with fixed query: {fixed_query}")
                    with self.neo4j_driver.session() as session:
                        result = session.run(fixed_query)
                        records = []
                        for record in result:
                            record_dict = {}
                            for key, value in record.items():
                                if hasattr(value, '__dict__'):
                                    record_dict[key] = dict(value)
                                else:
                                    record_dict[key] = value
                            records.append(record_dict)
                        return records
                except Exception as e2:
                    logger.error(f"Auto-fix failed: {e2}")
                    raise e
            else:
                raise e
    def _get_collection(self, name: str):
        """
        Return a collection. Do NOT pass a conflicting embedding_function.
        Let Chroma use its default EF (e.g. all-MiniLM-L6-v2) if that’s how it was created.
        """
        if not hasattr(self, "_collections"):
            self._collections = {}

        if name in self._collections:
            return self._collections[name]

        try:
            # Do NOT pass embedding_function
            col = self.chroma_client.get_collection(name=name)
        except Exception as e:
            logger.error(f"Failed to get collection '{name}': {e}")
            raise

        self._collections[name] = col
        return col
    
    def execute_vector_search(self, query_str: str) -> List[Dict[str, Any]]:
        """Execute vector search on ChromaDB"""
        logger.info(f"Executing vector search: {query_str}")
        
        # Parse the query string (expecting JSON format)
        try:
            if isinstance(query_str, str) and query_str.strip().startswith('{'):
                query_params = json.loads(query_str)
            else:
                # Simple text search
                query_params = {"text": query_str}
        except:
            query_params = {"text": query_str}
        
        search_text = query_params.get("text", "")
        where_clause = query_params.get("where", {})
        n_results = query_params.get("n_results", 10)
        collection_name = query_params.get("collection", "opportunities")
        
        try:
            # Get collection
            collection = self._get_collection(collection_name)
            
            # Log collection info for debugging
            logger.info(f"Using collection: {collection_name}")
            
            # Perform search
            if search_text:
                results = collection.query(
                    query_texts=[search_text],
                    where=where_clause if where_clause else None,
                    n_results=n_results
                )
                logger.debug(f"Query results structure: {list(results.keys())}")
            else:
                # If no search text, just filter by metadata
                results = collection.get(
                    where=where_clause if where_clause else None,
                    limit=n_results
                )
                logger.debug(f"Get results structure: {list(results.keys())}")
            
            # Format results - FIXED to handle empty results properly
            formatted_results = []
            
            # ChromaDB returns empty lists when no results found
            # Check if we have any results by looking at the ids
            if (results and 
                'ids' in results and 
                results['ids'] and 
                isinstance(results['ids'], list) and 
                len(results['ids']) > 0):
                
                # Handle both query and get result formats
                # For query: results['ids'][0] is the list of IDs
                # For get: results['ids'] is the list of IDs directly
                ids_list = results['ids'][0] if isinstance(results['ids'][0], list) else results['ids']
                
                if len(ids_list) > 0:
                    for i in range(len(ids_list)):
                        result = {
                            'id': ids_list[i],
                            'document': results['documents'][0][i] if 'documents' in results and isinstance(results['documents'][0], list) else results['documents'][i] if 'documents' in results else None,
                            'metadata': results['metadatas'][0][i] if 'metadatas' in results and isinstance(results['metadatas'][0], list) else results['metadatas'][i] if 'metadatas' in results else {},
                            'distance': results['distances'][0][i] if 'distances' in results and isinstance(results['distances'][0], list) else None
                        }
                        formatted_results.append(result)
                    
                    # Log what we found
                    logger.info(f"Vector search found {len(formatted_results)} results")
                    
                    # Log first few results for debugging
                    for idx, res in enumerate(formatted_results[:3]):
                        logger.debug(f"  Result {idx+1}: ID={res['id']}, metadata keys={list(res.get('metadata', {}).keys())}")
                else:
                    logger.info("Vector search returned empty results list")
            else:
                logger.info("Vector search returned no results - empty response structure")
                # Log the actual results structure for debugging
                if results:
                    logger.debug(f"Results keys: {list(results.keys())}")
                    logger.debug(f"IDs content: {results.get('ids', 'No ids key')}")
            
        except Exception as e:
            logger.error(f"Error in vector search: {e}", exc_info=True)
            formatted_results = []
        
        return formatted_results