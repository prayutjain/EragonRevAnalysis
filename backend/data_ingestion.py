# data_ingestion.py - Enhanced ingestion pipeline with date fixes and safety checks
import pandas as pd
import duckdb
from neo4j import GraphDatabase
import chromadb
from chromadb.config import Settings
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional
import json
from tqdm import tqdm
from datetime import datetime
import re

from config import *
from schema_discovery import SchemaDiscovery

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataIngestionPipeline:
    """Ingests CSV data into DuckDB, Neo4j, and ChromaDB with enhanced safety and date handling"""
    
    def __init__(self):
        self.schema_discovery = SchemaDiscovery(CONFIG_DIR)
        self.schemas = {}
        self.relationships = []
        
        # Initialize connections (lazy loading)
        self._duckdb_con = None
        self._neo4j_driver = None
        self._chroma_client = None
        self._embeddings = None
        
    @property
    def duckdb_con(self):
        if self._duckdb_con is None:
            self._duckdb_con = duckdb.connect(str(DUCKDB_PATH))
            logger.info(f"Connected to DuckDB at {DUCKDB_PATH}")
        return self._duckdb_con
    
    @property
    def neo4j_driver(self):
        if self._neo4j_driver is None:
            self._neo4j_driver = GraphDatabase.driver(
                NEO4J_URI, 
                auth=(NEO4J_USER, NEO4J_PASSWORD)
            )
            logger.info(f"Connected to Neo4j at {NEO4J_URI}")
        return self._neo4j_driver
    
    @property
    def chroma_client(self):
        if self._chroma_client is None:
            self._chroma_client = chromadb.PersistentClient(
                path=str(CHROMA_PERSIST_DIR),
                settings=Settings(anonymized_telemetry=False)
            )
            logger.info(f"Connected to ChromaDB at {CHROMA_PERSIST_DIR}")
        return self._chroma_client
    
    
    def run_full_pipeline(self, csv_files: List[Path] = None, skip_vector_creation: bool = False):
        """Run the complete ingestion pipeline with optional vector skip"""
        if csv_files is None:
            csv_files = list(RAW_DATA_DIR.glob("*.csv"))
            
        if not csv_files:
            raise ValueError(f"No CSV files found in {RAW_DATA_DIR}")
            
        logger.info(f"Starting ingestion for {len(csv_files)} files")
        
        # Step 1: Discover schemas
        self.discover_schemas(csv_files)
        
        # Step 2: Ingest to DuckDB with proper date handling
        self.ingest_to_duckdb(csv_files)
        
        # Step 3: Fix date columns if needed
        self.fix_date_columns()
        
        # Step 4: Build Knowledge Graph
        self.build_knowledge_graph(csv_files)
        
        # Step 5: Create Vector Indexes (with safety check)
        if not skip_vector_creation:
            self.create_vector_indexes(csv_files)
        else:
            logger.info("Skipping vector creation as requested")
        
        logger.info("Ingestion pipeline completed successfully")
    
    def fix_date_columns(self):
        """Fix date columns in all tables to be proper DATE types"""
        logger.info("Checking and fixing date columns...")
        
        for table_name, schema in self.schemas.items():
            date_columns = []
            
            # Find date columns based on schema
            for col_name, col_info in schema["columns"].items():
                if col_info["dtype"] == "date" or "date" in col_name.lower():
                    date_columns.append(col_name)
            
            if not date_columns:
                continue
                
            # Check current column types
            try:
                current_schema = self.duckdb_con.execute(f"DESCRIBE {table_name}").fetchdf()
                
                for date_col in date_columns:
                    col_type = current_schema[current_schema['column_name'] == date_col]['column_type'].values
                    
                    if col_type and col_type[0] != 'DATE':
                        logger.info(f"Fixing date column '{date_col}' in table '{table_name}'")
                        self._fix_single_date_column(table_name, date_col)
                        
            except Exception as e:
                logger.warning(f"Could not check/fix date columns for {table_name}: {e}")
    
    def _fix_single_date_column(self, table_name: str, date_column: str):
        """Fix a single date column to be proper DATE type"""
        try:
            # First, analyze the date format
            sample = self.duckdb_con.execute(f"""
                SELECT "{date_column}" 
                FROM {table_name} 
                WHERE "{date_column}" IS NOT NULL 
                AND "{date_column}" != ''
                LIMIT 10
            """).fetchdf()
            
            if sample.empty:
                logger.warning(f"No non-null dates found in {table_name}.{date_column}")
                return
            
            # Detect date format
            date_format = self._detect_date_format(sample[date_column].iloc[0])
            
            if not date_format:
                logger.warning(f"Could not detect date format for {table_name}.{date_column}")
                return
            
            # Create backup
            backup_name = f"{table_name}_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            self.duckdb_con.execute(f"CREATE TABLE {backup_name} AS SELECT * FROM {table_name}")
            
            # Update the column
            safe_column = f'"{date_column}"'
            query = f"""
                ALTER TABLE {table_name} 
                ADD COLUMN {safe_column}_new DATE;
                
                UPDATE {table_name} 
                SET {safe_column}_new = CASE 
                    WHEN {safe_column} IS NOT NULL AND {safe_column} != '' 
                    THEN TRY_CAST(strptime({safe_column}, '{date_format}') AS DATE)
                    ELSE NULL 
                END;
                
                ALTER TABLE {table_name} DROP COLUMN {safe_column};
                ALTER TABLE {table_name} RENAME COLUMN {safe_column}_new TO {safe_column};
            """
            
            self.duckdb_con.execute(query)
            logger.info(f"Successfully converted {table_name}.{date_column} to DATE type")
            
            # Drop backup if successful
            self.duckdb_con.execute(f"DROP TABLE {backup_name}")
            
        except Exception as e:
            logger.error(f"Failed to fix date column {table_name}.{date_column}: {e}")
    
    def _detect_date_format(self, sample_date: str) -> Optional[str]:
        """Detect date format from a sample date string"""
        if not sample_date or pd.isna(sample_date):
            return None
            
        sample_date = str(sample_date).strip()
        
        # Common date formats
        formats = [
            (r'^\d{1,2}/\d{1,2}/\d{4}$', '%m/%d/%Y'),  # MM/DD/YYYY
            (r'^\d{4}-\d{2}-\d{2}$', '%Y-%m-%d'),      # YYYY-MM-DD
            (r'^\d{1,2}-\d{1,2}-\d{4}$', '%m-%d-%Y'),  # MM-DD-YYYY
            (r'^\d{4}/\d{2}/\d{2}$', '%Y/%m/%d'),      # YYYY/MM/DD
        ]
        
        for pattern, format_string in formats:
            if re.match(pattern, sample_date):
                return format_string
                
        return None
        
    def discover_schemas(self, csv_files: List[Path]):
        """Discover and save schemas for all CSV files"""
        logger.info("Starting schema discovery...")
        
        schemas = []
        for csv_file in csv_files:
            schema = self.schema_discovery.discover_csv_schema(csv_file)
            schemas.append(schema)
            self.schemas[schema["table_name"]] = schema
            
        # Discover relationships
        self.relationships = self.schema_discovery.discover_relationships(schemas)
        
        # Save configurations
        self.schema_discovery.save_schema_config(schemas, self.relationships)
        
        logger.info(f"Discovered {len(schemas)} tables and {len(self.relationships)} relationships")
        
    def ingest_to_duckdb(self, csv_files: List[Path]):
        """Ingest CSV files into DuckDB with date awareness"""
        logger.info("Ingesting data to DuckDB...")
        
        for csv_file in csv_files:
            table_name = self.schemas[csv_file.stem.lower().replace(" ", "_")]["table_name"]
            schema = self.schemas[table_name]
            
            # Drop table if exists
            self.duckdb_con.execute(f"DROP TABLE IF EXISTS {table_name}")
            
            # Check for date columns
            date_columns = {}
            for col_name, col_info in schema["columns"].items():
                if col_info["dtype"] == "date" or "date" in col_name.lower():
                    # Try to detect format from sample
                    sample_df = pd.read_csv(csv_file, nrows=10)
                    if col_name in sample_df.columns:
                        format_str = self._detect_date_format(str(sample_df[col_name].dropna().iloc[0]) if not sample_df[col_name].dropna().empty else None)
                        if format_str:
                            date_columns[col_name] = format_str
            
            # Create table with proper date handling
            if date_columns:
                # Read CSV and convert dates
                df = pd.read_csv(csv_file)
                for col, format_str in date_columns.items():
                    if col in df.columns:
                        df[col] = pd.to_datetime(df[col], format=format_str, errors='coerce')
                
                # Register as temporary view
                self.duckdb_con.register('temp_df', df)
                self.duckdb_con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM temp_df")
                self.duckdb_con.unregister('temp_df')
            else:
                # Let DuckDB automatically infer schema
                self.duckdb_con.execute(
                    f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('{csv_file}')"
                )
            
            # Add row_id column for tracking
            self.duckdb_con.execute(
                f"ALTER TABLE {table_name} ADD COLUMN row_id INTEGER"
            )
            self.duckdb_con.execute(
                f"UPDATE {table_name} SET row_id = rowid"
            )
            
            count = self.duckdb_con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            logger.info(f"Loaded {count} rows into {table_name}")
            
            # Log date columns if any
            if date_columns:
                logger.info(f"  Converted date columns: {list(date_columns.keys())}")
            
    def build_knowledge_graph(self, csv_files: List[Path]):
        """Build Neo4j knowledge graph from CSV data"""
        logger.info("Building knowledge graph in Neo4j...")
        
        with self.neo4j_driver.session() as session:
            # Clear existing data
            session.run("MATCH (n) DETACH DELETE n")
            
            for csv_file in csv_files:
                table_name = self.schemas[csv_file.stem.lower().replace(" ", "_")]["table_name"]
                label = self._table_to_label(table_name)
                
                # Read data from DuckDB
                df = self.duckdb_con.execute(f"SELECT * FROM {table_name}").fetchdf()
                
                # Create nodes in batches
                logger.info(f"Creating {len(df)} {label} nodes...")
                for idx in tqdm(range(0, len(df), CHUNK_SIZE)):
                    batch = df.iloc[idx:idx + CHUNK_SIZE]
                    self._create_nodes_batch(session, batch, label, table_name)
            
            # Create additional nodes for entities mentioned in opportunities
            self.debug_table_columns("opportunities")
            self._create_entity_nodes(session)
                    
            # Create relationships
            logger.info("Creating relationships...")
            self._create_relationships(session)
            
            # Create additional business relationships
            self._create_business_relationships(session)

    def debug_table_columns(self, table_name: str):
        """Debug method to inspect available columns in a table"""
        try:
            # Check DuckDB table structure
            result = self.duckdb_con.execute(f"DESCRIBE {table_name}").fetchdf()
            logger.info(f"DuckDB columns for {table_name}:")
            for idx, row in result.iterrows():
                logger.info(f"  {row['column_name']} ({row['column_type']})")
            
            # Check schema discovery results
            if table_name in self.schemas:
                schema = self.schemas[table_name]
                logger.info(f"Schema discovery results for {table_name}:")
                for orig_col, col_info in schema["columns"].items():
                    logger.info(f"  Original: '{orig_col}' -> Clean: '{col_info['clean_name']}' ({col_info['dtype']})")
            else:
                logger.warning(f"No schema found for table {table_name}")
                
            # Sample some actual data
            sample = self.duckdb_con.execute(f"SELECT * FROM {table_name} LIMIT 3").fetchdf()
            logger.info(f"Sample data from {table_name}:")
            logger.info(f"Columns: {list(sample.columns)}")
            
        except Exception as e:
            logger.error(f"Error debugging table {table_name}: {e}")
    
    def _create_entity_nodes(self, session):
        """Create additional entity nodes based on patterns in the data"""
        opportunities_schema = self.schemas.get("opportunities")
        if not opportunities_schema:
            return
        
        # Helper to find column and get clean name
        def find_and_clean_column(potential_names):
            for col_name, col_info in opportunities_schema["columns"].items():
                for potential in potential_names:
                    if potential.lower() in col_name.lower():
                        return col_name  # Return original column name for SQL
            return None
        
        # 1. Create Stakeholder nodes from Next Step mentions
        next_step_col = find_and_clean_column(["next step", "next_step"])
        if next_step_col:
            try:
                stakeholder_patterns = {
                    'CTO': ['cto', 'chief technology officer'],
                    'CEO': ['ceo', 'chief executive officer'],
                    'VP': ['vp ', 'vice president'],
                    'Director': ['director'],
                    'Manager': ['manager'],
                    'President': ['president']
                }
                
                for stakeholder_type, patterns in stakeholder_patterns.items():
                    # Create stakeholder type nodes
                    session.run(
                        "MERGE (s:StakeholderType {name: $name, category: $category})",
                        name=stakeholder_type,
                        category='Executive' if stakeholder_type in ['CEO', 'CTO', 'VP', 'President'] else 'Management'
                    )
                
                logger.info("Created StakeholderType nodes")
            except Exception as e:
                logger.warning(f"Failed to create stakeholder nodes: {e}")
        
        # 2. Create Deal Size Category nodes
        amount_col = find_and_clean_column(["amount", "value"])
        if amount_col:
            try:
                deal_categories = [
                    {'name': 'Small Deal', 'min': 0, 'max': 50000},
                    {'name': 'Medium Deal', 'min': 50000, 'max': 200000},
                    {'name': 'Large Deal', 'min': 200000, 'max': 500000},
                    {'name': 'Enterprise Deal', 'min': 500000, 'max': float('inf')}
                ]
                
                for category in deal_categories:
                    session.run(
                        "MERGE (dc:DealCategory {name: $name, min_amount: $min, max_amount: $max})",
                        name=category['name'],
                        min=category['min'],
                        max=category['max'] if category['max'] != float('inf') else 999999999
                    )
                
                logger.info("Created DealCategory nodes")
            except Exception as e:
                logger.warning(f"Failed to create deal category nodes: {e}")
        
        # 3. Create Risk Level nodes
        probability_col = find_and_clean_column(["probability", "prob"])
        if probability_col:
            try:
                risk_levels = [
                    {'name': 'High Risk', 'min': 0, 'max': 25},
                    {'name': 'Medium Risk', 'min': 25, 'max': 50},
                    {'name': 'Low Risk', 'min': 50, 'max': 75},
                    {'name': 'Very Low Risk', 'min': 75, 'max': 100}
                ]
                
                for risk in risk_levels:
                    session.run(
                        "MERGE (rl:RiskLevel {name: $name, min_probability: $min, max_probability: $max})",
                        name=risk['name'],
                        min=risk['min'],
                        max=risk['max']
                    )
                
                logger.info("Created RiskLevel nodes")
            except Exception as e:
                logger.warning(f"Failed to create risk level nodes: {e}")
        
        # 4. Create Contact Level nodes
        contact_title_col = find_and_clean_column(["contact: title", "contact_title", "title"])
        if contact_title_col:
            try:
                contact_levels = ['Executive', 'Director', 'Manager', 'Individual Contributor']
                
                for level in contact_levels:
                    session.run(
                        "MERGE (cl:ContactLevel {name: $name, seniority_order: $order})",
                        name=level,
                        order=['Individual Contributor', 'Manager', 'Director', 'Executive'].index(level)
                    )
                
                logger.info("Created ContactLevel nodes")
            except Exception as e:
                logger.warning(f"Failed to create contact level nodes: {e}")
        
        # 5. Create Time Period nodes  
        close_date_col = find_and_clean_column(["close date", "close_date"])
        if close_date_col:
            try:
                # Check if the column is already a DATE type
                col_info = self.duckdb_con.execute(f"""
                    SELECT column_type 
                    FROM (DESCRIBE opportunities) 
                    WHERE column_name = '{close_date_col}'
                """).fetchone()
                
                if col_info and col_info[0] == 'DATE':
                    # Use proper date functions
                    months_data = self.duckdb_con.execute(f"""
                        SELECT DISTINCT 
                            EXTRACT(MONTH FROM "{close_date_col}") as month,
                            EXTRACT(YEAR FROM "{close_date_col}") as year
                        FROM opportunities 
                        WHERE "{close_date_col}" IS NOT NULL
                    """).fetchdf()
                else:
                    # Fall back to string parsing
                    months_data = self.duckdb_con.execute(f"""
                        SELECT DISTINCT 
                            CAST(split_part("{close_date_col}", '/', 1) AS INTEGER) as month,
                            CAST(split_part("{close_date_col}", '/', 3) AS INTEGER) as year
                        FROM opportunities 
                        WHERE "{close_date_col}" IS NOT NULL AND "{close_date_col}" != ''
                    """).fetchdf()
                
                for _, row in months_data.iterrows():
                    month_num = int(row['month'])
                    year = int(row['year'])
                    quarter = ((month_num - 1) // 3) + 1
                    
                    # Create month node
                    session.run(
                        "MERGE (m:Month {number: $month, year: $year, quarter: $quarter, name: $name})",
                        month=month_num,
                        year=year,
                        quarter=quarter,
                        name=f"{['', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'][month_num]} {year}"
                    )
                    
                    # Create quarter node
                    session.run(
                        "MERGE (q:Quarter {number: $quarter, year: $year, name: $name})",
                        quarter=quarter,
                        year=year,
                        name=f"Q{quarter}-{year}"
                    )
                
                logger.info("Created Month and Quarter nodes")
            except Exception as e:
                logger.warning(f"Failed to create time period nodes: {e}")
        
        # 6. Create Fiscal Period nodes (already exists but enhance)
        fiscal_period_col = find_and_clean_column(["fiscal period", "fiscal_period"])
        if fiscal_period_col:
            try:
                periods = self.duckdb_con.execute(f"""
                    SELECT DISTINCT "{fiscal_period_col}" as period
                    FROM opportunities 
                    WHERE "{fiscal_period_col}" IS NOT NULL
                """).fetchdf()
                
                for period in periods['period']:
                    session.run(
                        "MERGE (fp:FiscalPeriod {name: $name})",
                        name=str(period)
                    )
                
                logger.info("Created FiscalPeriod nodes")
            except Exception as e:
                logger.warning(f"Failed to create fiscal period nodes: {e}")
    
    def _create_business_relationships(self, session):
        """Create enhanced business relationships based on schema"""
        opportunities_schema = self.schemas.get("opportunities")
        if not opportunities_schema:
            return
        
        # Helper function to get clean property name
        def get_clean_prop(column_name):
            if column_name in opportunities_schema["columns"]:
                clean = opportunities_schema["columns"][column_name]["clean_name"]
                if any(char in clean for char in [' ', '-', '(', ')', '%', ':', '.']):
                    return f"`{clean}`"
                return clean
            return None
        
        # Get property names
        amount_prop = get_clean_prop("Amount")
        probability_prop = get_clean_prop("Probability (%)")
        next_step_prop = get_clean_prop("Next Step")
        close_date_prop = get_clean_prop("Close Date")
        contact_title_prop = get_clean_prop("Contact: Title")
        stage_prop = get_clean_prop("Stage")
        
        # 1. Amount-based relationships
        if amount_prop:
            # Similar value relationships (already exists, but let's enhance)
            try:
                query = f"""
                MATCH (o1:Opportunities), (o2:Opportunities)
                WHERE o1.id < o2.id
                AND o1.{amount_prop} IS NOT NULL AND o2.{amount_prop} IS NOT NULL
                AND o1.{amount_prop} > 0 AND o2.{amount_prop} > 0
                AND abs(o1.{amount_prop} - o2.{amount_prop}) / o1.{amount_prop} < 0.15
                CREATE (o1)-[:SIMILAR_VALUE {{
                    diff_pct: abs(o1.{amount_prop} - o2.{amount_prop}) / o1.{amount_prop},
                    avg_amount: (o1.{amount_prop} + o2.{amount_prop}) / 2
                }}]->(o2)
                """
                result = session.run(query)
                logger.info(f"Created {result.consume().counters.relationships_created} enhanced SIMILAR_VALUE relationships")
            except Exception as e:
                logger.warning(f"Failed to create enhanced similar value relationships: {e}")
            
            # Competitive deals (large deals closing same period)
            if close_date_prop:
                try:
                    query = f"""
                    MATCH (o1:Opportunities), (o2:Opportunities)
                    WHERE o1.id < o2.id
                    AND o1.{amount_prop} > 100000 AND o2.{amount_prop} > 100000
                    AND o1.{close_date_prop} = o2.{close_date_prop}
                    AND o1.account_name <> o2.account_name
                    CREATE (o1)-[:COMPETITIVE_PERIOD {{
                        close_date: o1.{close_date_prop},
                        combined_value: o1.{amount_prop} + o2.{amount_prop}
                    }}]->(o2)
                    """
                    result = session.run(query)
                    logger.info(f"Created {result.consume().counters.relationships_created} COMPETITIVE_PERIOD relationships")
                except Exception as e:
                    logger.warning(f"Failed to create competitive period relationships: {e}")
        
        # 2. Probability-based relationships
        if probability_prop:
            # Risk bands
            try:
                query = f"""
                MATCH (o1:Opportunities), (o2:Opportunities)
                WHERE o1.id < o2.id
                AND abs(o1.{probability_prop} - o2.{probability_prop}) <= 10
                CREATE (o1)-[:SIMILAR_RISK {{
                    prob_diff: abs(o1.{probability_prop} - o2.{probability_prop}),
                    risk_band: CASE 
                        WHEN o1.{probability_prop} < 25 THEN 'High Risk'
                        WHEN o1.{probability_prop} < 50 THEN 'Medium Risk'
                        WHEN o1.{probability_prop} < 75 THEN 'Low Risk'
                        ELSE 'Very Low Risk'
                    END
                }}]->(o2)
                """
                result = session.run(query)
                logger.info(f"Created {result.consume().counters.relationships_created} SIMILAR_RISK relationships")
            except Exception as e:
                logger.warning(f"Failed to create similar risk relationships: {e}")
        
        # 3. Next Step pattern analysis
        if next_step_prop:
            # Similar next steps (keyword matching)
            try:
                query = f"""
                MATCH (o1:Opportunities), (o2:Opportunities)
                WHERE o1.id < o2.id
                AND o1.{next_step_prop} IS NOT NULL AND o2.{next_step_prop} IS NOT NULL
                WITH o1, o2,
                    split(toLower(o1.{next_step_prop}), ' ') as words1,
                    split(toLower(o2.{next_step_prop}), ' ') as words2
                WITH o1, o2, words1, words2,
                    size([w IN words1 WHERE w IN words2 | w]) as common_words,
                    size(words1 + words2) as total_words
                WHERE common_words >= 2 AND toFloat(common_words) / total_words > 0.3
                CREATE (o1)-[:SIMILAR_NEXT_STEPS {{
                    common_words: common_words,
                    similarity_score: toFloat(common_words) / total_words
                }}]->(o2)
                """
                result = session.run(query)
                logger.info(f"Created {result.consume().counters.relationships_created} SIMILAR_NEXT_STEPS relationships")
            except Exception as e:
                logger.warning(f"Failed to create similar next steps relationships: {e}")
            
            # Stakeholder mentions in next steps
            try:
                stakeholder_titles = ['CTO', 'CEO', 'VP', 'Director', 'Manager', 'President']
                for title in stakeholder_titles:
                    query = f"""
                    MATCH (o1:Opportunities), (o2:Opportunities)
                    WHERE o1.id < o2.id
                    AND toLower(o1.{next_step_prop}) CONTAINS '{title.lower()}'
                    AND toLower(o2.{next_step_prop}) CONTAINS '{title.lower()}'
                    CREATE (o1)-[:SAME_STAKEHOLDER_TYPE {{stakeholder_type: '{title}'}}]->(o2)
                    """
                    result = session.run(query)
                    created = result.consume().counters.relationships_created
                    if created > 0:
                        logger.info(f"Created {created} SAME_STAKEHOLDER_TYPE relationships for {title}")
            except Exception as e:
                logger.warning(f"Failed to create stakeholder relationships: {e}")
        
        # 4. Contact seniority level clustering
        if contact_title_prop:
            try:
                query = f"""
                MATCH (o1:Opportunities), (o2:Opportunities)
                WHERE o1.id < o2.id
                AND o1.{contact_title_prop} IS NOT NULL AND o2.{contact_title_prop} IS NOT NULL
                WITH o1, o2,
                    CASE 
                        WHEN toLower(o1.{contact_title_prop}) CONTAINS 'vp' OR toLower(o1.{contact_title_prop}) CONTAINS 'vice president' THEN 'Executive'
                        WHEN toLower(o1.{contact_title_prop}) CONTAINS 'director' THEN 'Director'
                        WHEN toLower(o1.{contact_title_prop}) CONTAINS 'manager' THEN 'Manager'
                        ELSE 'Other'
                    END as level1,
                    CASE 
                        WHEN toLower(o2.{contact_title_prop}) CONTAINS 'vp' OR toLower(o2.{contact_title_prop}) CONTAINS 'vice president' THEN 'Executive'
                        WHEN toLower(o2.{contact_title_prop}) CONTAINS 'director' THEN 'Director'
                        WHEN toLower(o2.{contact_title_prop}) CONTAINS 'manager' THEN 'Manager'
                        ELSE 'Other'
                    END as level2
                WHERE level1 = level2 AND level1 <> 'Other'
                CREATE (o1)-[:SAME_CONTACT_LEVEL {{contact_level: level1}}]->(o2)
                """
                result = session.run(query)
                logger.info(f"Created {result.consume().counters.relationships_created} SAME_CONTACT_LEVEL relationships")
            except Exception as e:
                logger.warning(f"Failed to create contact level relationships: {e}")
        
        # 5. Multi-dimensional deal health
        if amount_prop and probability_prop and stage_prop:
            try:
                query = f"""
                MATCH (o1:Opportunities), (o2:Opportunities)
                WHERE o1.id < o2.id
                WITH o1, o2,
                    o1.{amount_prop} * o1.{probability_prop} / 100.0 as expected_value1,
                    o2.{amount_prop} * o2.{probability_prop} / 100.0 as expected_value2
                WHERE abs(expected_value1 - expected_value2) / expected_value1 < 0.2
                CREATE (o1)-[:SIMILAR_EXPECTED_VALUE {{
                    expected_value_diff: abs(expected_value1 - expected_value2),
                    avg_expected_value: (expected_value1 + expected_value2) / 2
                }}]->(o2)
                """
                result = session.run(query)
                logger.info(f"Created {result.consume().counters.relationships_created} SIMILAR_EXPECTED_VALUE relationships")
            except Exception as e:
                logger.warning(f"Failed to create expected value relationships: {e}")
        
        # 6. Quarter-end deal patterns
        if close_date_prop and amount_prop:
            try:
                # Check if close_date is already a DATE type
                col_info = self.duckdb_con.execute(f"""
                    SELECT column_type 
                    FROM (DESCRIBE opportunities) 
                    WHERE column_name = 'Close Date'
                """).fetchone()
                
                if col_info and col_info[0] == 'DATE':
                    # Use date functions
                    query = f"""
                    MATCH (o:Opportunities)
                    WHERE o.{close_date_prop} IS NOT NULL
                    WITH o, 
                        date(o.{close_date_prop}).month as month,
                        date(o.{close_date_prop}).day as day
                    WHERE month IN [3, 6, 9, 12] AND day >= 25
                    AND o.{amount_prop} > 50000
                    SET o:QuarterEndDeal
                    """
                else:
                    # Fall back to string parsing
                    query = f"""
                    MATCH (o:Opportunities)
                    WHERE o.{close_date_prop} IS NOT NULL
                    WITH o, 
                        toInteger(split(o.{close_date_prop}, '/')[0]) as month,
                        toInteger(split(o.{close_date_prop}, '/')[1]) as day
                    WHERE month IN [3, 6, 9, 12] AND day >= 25
                    AND o.{amount_prop} > 50000
                    SET o:QuarterEndDeal
                    """
                
                result = session.run(query)
                logger.info(f"Marked {result.consume().counters.labels_added} QuarterEndDeal opportunities")
                
                # Connect quarter-end deals
                query = f"""
                MATCH (o1:QuarterEndDeal), (o2:QuarterEndDeal)
                WHERE o1.id < o2.id
                AND o1.{close_date_prop} = o2.{close_date_prop}
                CREATE (o1)-[:SAME_QUARTER_END {{close_date: o1.{close_date_prop}}}]->(o2)
                """
                result = session.run(query)
                logger.info(f"Created {result.consume().counters.relationships_created} SAME_QUARTER_END relationships")
            except Exception as e:
                logger.warning(f"Failed to create quarter-end relationships: {e}")
        
        # 7. Deal intervention opportunities
        if amount_prop and probability_prop and next_step_prop:
            try:
                query = f"""
                MATCH (o:Opportunities)
                WHERE o.{amount_prop} > 200000  // High value
                AND o.{probability_prop} < 50   // Low probability
                AND toLower(o.{next_step_prop}) CONTAINS 'review'  // Needs review
                SET o:NeedsIntervention
                """
                result = session.run(query)
                logger.info(f"Marked {result.consume().counters.labels_added} NeedsIntervention opportunities")
            except Exception as e:
                logger.warning(f"Failed to mark intervention opportunities: {e}")
            
    def create_vector_indexes(self, csv_files: List[Path], force_recreate: bool = False):
        """Create vector embeddings and store in ChromaDB with safety checks"""
        logger.info("Creating vector indexes...")
        
        # Check if we should skip to preserve existing data
        if not force_recreate:
            existing_collections = {col.name for col in self.chroma_client.list_collections()}
            logger.info(f"Found existing collections: {existing_collections}")
        
        for csv_file in csv_files:
            table_name = self.schemas[csv_file.stem.lower().replace(" ", "_")]["table_name"]
            collection_name = f"{table_name}_vectors"
            
            # Check if collection exists and has data
            if not force_recreate and collection_name in existing_collections:
                try:
                    collection = self.chroma_client.get_collection(collection_name)
                    count = collection.count()
                    if count > 0:
                        logger.info(f"Collection {collection_name} already has {count} documents. Skipping...")
                        continue
                except Exception as e:
                    logger.warning(f"Error checking collection {collection_name}: {e}")
            
            # Delete existing collection if force_recreate or empty
            try:
                self.chroma_client.delete_collection(collection_name)
                logger.info(f"Deleted existing collection {collection_name}")
            except:
                pass
                
            collection = self.chroma_client.create_collection(
                name=collection_name,
                metadata={"table": table_name}
            )
            
            # Read data from DuckDB
            df = self.duckdb_con.execute(f"SELECT * FROM {table_name}").fetchdf()
            
            # Create embeddings in batches
            logger.info(f"Creating embeddings for {len(df)} rows from {table_name}...")
            successfully_added = 0
            
            for idx in tqdm(range(0, len(df), VECTOR_BATCH_SIZE)):
                batch = df.iloc[idx:idx + VECTOR_BATCH_SIZE]
                try:
                    added = self._create_embeddings_batch(collection, batch, table_name)
                    successfully_added += added
                except Exception as e:
                    logger.error(f"Error creating embeddings for batch {idx//VECTOR_BATCH_SIZE}: {e}")
                    
            logger.info(f"Successfully added {successfully_added} documents to {collection_name}")
                
    def _table_to_label(self, table_name: str) -> str:
        """Convert table name to Neo4j label"""
        return ''.join(word.capitalize() for word in table_name.split('_'))
    
    def _create_nodes_batch(self, session, df_batch: pd.DataFrame, label: str, table_name: str):
        """Create a batch of nodes in Neo4j"""
        nodes_data = []
        schema = self.schemas[table_name]
        
        for idx, row in df_batch.iterrows():
            node_id = f"{label}-{row['row_id']}"
            props = {}
            
            # Use clean column names for Neo4j properties
            for col, value in row.items():
                if pd.notna(value) and col != 'row_id':
                    # Use the clean column name from schema
                    clean_col = schema["columns"][col]["clean_name"]
                    # Convert numpy types to Python types for Neo4j
                    if hasattr(value, 'item'):  # numpy scalar
                        value = value.item()
                    props[clean_col] = value
                    
            nodes_data.append({"id": node_id, "row_id": int(row['row_id']), "props": props})
            
        query = f"""
        UNWIND $nodes AS node
        CREATE (n:{label} {{id: node.id, row_id: node.row_id}})
        SET n += node.props
        """
        session.run(query, nodes=nodes_data)
        
    def _create_relationships(self, session):
        """Create relationships based on discovered schema"""
        for rel in self.relationships:
            if rel.get("relationship_type") == "intra_table":
                self._create_intra_table_relationships(session, rel)
            else:
                self._create_inter_table_relationships(session, rel)
    
    def _create_inter_table_relationships(self, session, rel):
        """Create relationships between different tables"""
        from_label = self._table_to_label(rel["from_table"])
        to_label = self._table_to_label(rel["to_table"])
        
        # Get clean column names from schemas
        from_schema = self.schemas[rel["from_table"]]
        to_schema = self.schemas[rel["to_table"]]
        
        from_col_clean = from_schema["columns"][rel["from_column"]]["clean_name"]
        to_col_clean = to_schema["columns"][rel["to_column"]]["clean_name"]
        
        # Create valid relationship type
        rel_type = (rel["from_column"].upper()
                   .replace(" ", "_")
                   .replace(":", "_")
                   .replace("-", "_")
                   .replace("_ID", "")
                   .strip("_"))
        
        if not rel_type:
            rel_type = "RELATED_TO"
        
        query = f"""
        MATCH (a:{from_label}), (b:{to_label})
        WHERE a.{from_col_clean} = b.{to_col_clean}
        CREATE (a)-[:{rel_type}]->(b)
        """
        
        try:
            result = session.run(query)
            created = result.consume().counters.relationships_created
            logger.info(f"Created {created} {rel_type} relationships between {from_label} and {to_label}")
        except Exception as e:
            logger.warning(f"Failed to create relationship {rel_type}: {str(e)}")
            logger.debug(f"Query was: {query}")
    
    def _create_intra_table_relationships(self, session, rel):
        """Create relationships within the same table"""
        label = self._table_to_label(rel["from_table"])
        rel_type = rel["type"].upper()
        
        if rel_type == "HAS_CONTACT":
            # Create virtual relationships based on Primary Contact
            self._create_contact_relationships(session, label)
        elif rel_type == "STAGE_PROGRESSION":
            # Create temporal relationships between opportunities
            self._create_stage_progression_relationships(session, label)
        elif rel_type == "TEMPORAL":
            # Create time-based relationships
            self._create_temporal_relationships(session, label, rel["via_columns"][0])
    
    def _create_contact_relationships(self, session, label):
        """Create relationships between opportunities that share contacts"""
        query = f"""
        MATCH (o1:{label}), (o2:{label})
        WHERE o1.primary_contact = o2.primary_contact 
        AND o1.primary_contact IS NOT NULL
        AND o1.id < o2.id
        CREATE (o1)-[:SHARES_CONTACT {{contact: o1.primary_contact}}]->(o2)
        """
        
        try:
            result = session.run(query)
            created = result.consume().counters.relationships_created
            logger.info(f"Created {created} SHARES_CONTACT relationships")
        except Exception as e:
            logger.warning(f"Failed to create contact relationships: {str(e)}")
    
    def _create_stage_progression_relationships(self, session, label):
        """Create relationships for opportunities in different stages"""
        # Connect opportunities from same account that are in sequential stages
        stage_order = {
            "Qualify": 1,
            "Develop": 2,
            "Propose": 3,
            "Negotiate": 4,
            "Closed Won": 5,
            "Closed Lost": 5
        }
        
        query = f"""
        MATCH (o1:{label}), (o2:{label})
        WHERE o1.account_name = o2.account_name
        AND o1.id < o2.id
        AND o1.stage IN {list(stage_order.keys())}
        AND o2.stage IN {list(stage_order.keys())}
        WITH o1, o2,
             CASE o1.stage {' '.join([f"WHEN '{k}' THEN {v}" for k, v in stage_order.items()])} END as stage1_order,
             CASE o2.stage {' '.join([f"WHEN '{k}' THEN {v}" for k, v in stage_order.items()])} END as stage2_order
        WHERE stage1_order < stage2_order
        CREATE (o1)-[:PROGRESSED_TO {{from_stage: o1.stage, to_stage: o2.stage}}]->(o2)
        """
        
        try:
            result = session.run(query)
            created = result.consume().counters.relationships_created
            logger.info(f"Created {created} PROGRESSED_TO relationships")
        except Exception as e:
            logger.warning(f"Failed to create stage progression relationships: {str(e)}")
    
    def _create_temporal_relationships(self, session, label, date_column):
        """Create relationships based on temporal proximity"""
        clean_col = self.schemas[label.lower()]["columns"][date_column]["clean_name"]
        
        # Connect opportunities created on the same date
        query = f"""
        MATCH (o1:{label}), (o2:{label})
        WHERE o1.{clean_col} = o2.{clean_col}
        AND o1.id < o2.id
        CREATE (o1)-[:SAME_PERIOD {{period_type: '{clean_col}'}}]->(o2)
        """
        
        try:
            result = session.run(query)
            created = result.consume().counters.relationships_created
            logger.info(f"Created {created} SAME_PERIOD relationships for {clean_col}")
        except Exception as e:
            logger.warning(f"Failed to create temporal relationships: {str(e)}")
            
    def _create_embeddings_batch(self, collection, df_batch: pd.DataFrame, table_name: str) -> int:
        try:
            documents, metadatas, ids = [], [], []
            schema = self.schemas[table_name]

            for idx, row in df_batch.iterrows():
                parts = []
                metadata = {"table": table_name, "row_id": int(row['row_id'])}

                for col, val in row.items():
                    if pd.notna(val) and col != "row_id":
                        parts.append(f"{col}: {val}")
                        col_info = schema["columns"].get(col, {})
                        if col_info.get("is_categorical") or col_info.get("is_potential_key"):
                            metadata[col] = str(val)

                documents.append(" | ".join(parts))
                metadatas.append(metadata)
                ids.append(f"{table_name}-{row['row_id']}")

            if not documents:
                return 0

            # Let Chroma embed automatically
            collection.add(documents=documents, metadatas=metadatas, ids=ids)
            return len(documents)

        except Exception as e:
            logger.error(f"Error in _create_embeddings_batch: {e}")
            return 0
        
    def close(self):
        """Close all connections"""
        if self._duckdb_con:
            self._duckdb_con.close()
        if self._neo4j_driver:
            self._neo4j_driver.close()
            
            
if __name__ == "__main__":
    # Run the ingestion pipeline
    pipeline = DataIngestionPipeline()
    try:
        # Check if vector collections already exist
        existing_collections = [col.name for col in pipeline.chroma_client.list_collections()]
        vector_collections = [col for col in existing_collections if col.endswith('_vectors')]
        
        if vector_collections:
            print(f"Found existing vector collections: {vector_collections}")
            response = input("Do you want to recreate vector indexes? This will delete existing data. (yes/no): ")
            skip_vectors = response.lower() not in ['yes', 'y']
        else:
            skip_vectors = False
            
        pipeline.run_full_pipeline(skip_vector_creation=skip_vectors)
        
    finally:
        pipeline.close()