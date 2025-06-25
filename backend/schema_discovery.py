# schema_discovery.py - Automatic schema discovery from CSVs
import pandas as pd
import json
from pathlib import Path
from typing import Dict, List, Any
import logging

logger = logging.getLogger(__name__)

class SchemaDiscovery:
    """Discovers schema and relationships from CSV files automatically"""
    
    def __init__(self, config_dir: Path):
        self.config_dir = Path(config_dir)
        self.config_dir.mkdir(exist_ok=True)
        
    def discover_csv_schema(self, csv_path: Path, sample_size: int = 100) -> Dict[str, Any]:
        """Analyze CSV and return schema information"""
        logger.info(f"Discovering schema for {csv_path.name}")
        
        # Read sample of data
        df_sample = pd.read_csv(csv_path, nrows=sample_size)
        df_dtypes = pd.read_csv(csv_path, nrows=5).dtypes
        
        schema = {
            "file_name": csv_path.name,
            "table_name": csv_path.stem.lower().replace(" ", "_"),
            "row_count": len(pd.read_csv(csv_path)),
            "columns": {}
        }
        
        for col in df_sample.columns:
            col_info = {
                "name": col,
                "clean_name": self._clean_column_name(col),
                "dtype": str(df_dtypes[col]),
                "nullable": df_sample[col].isna().any(),
                "unique_count": df_sample[col].nunique(),
                "sample_values": df_sample[col].dropna().unique()[:5].tolist()
            }
            
            # Detect column type and characteristics
            if col_info["dtype"] == "object":
                col_info["is_categorical"] = col_info["unique_count"] < len(df_sample) * 0.5
                col_info["max_length"] = df_sample[col].astype(str).str.len().max()
            elif "int" in col_info["dtype"] or "float" in col_info["dtype"]:
                col_info["min"] = df_sample[col].min()
                col_info["max"] = df_sample[col].max()
                col_info["mean"] = df_sample[col].mean()
            
            # Detect potential keys and references
            col_info["is_potential_key"] = self._is_potential_key(col, df_sample)
            col_info["is_potential_reference"] = self._is_potential_reference(col)
            
            schema["columns"][col] = col_info
            
        return schema
    
    def discover_relationships(self, schemas: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Detect potential relationships between and within tables"""
        relationships = []
        
        # Inter-table relationships
        for i, schema1 in enumerate(schemas):
            for j, schema2 in enumerate(schemas[i+1:], i+1):
                # Check for matching column names that could indicate relationships
                for col1, info1 in schema1["columns"].items():
                    for col2, info2 in schema2["columns"].items():
                        if self._columns_match(col1, col2, info1, info2):
                            rel = {
                                "from_table": schema1["table_name"],
                                "from_column": col1,
                                "to_table": schema2["table_name"],
                                "to_column": col2,
                                "type": "foreign_key",
                                "relationship_type": "inter_table"
                            }
                            relationships.append(rel)
        
        # Intra-table relationships (within same table)
        for schema in schemas:
            intra_rels = self._discover_intra_table_relationships(schema)
            relationships.extend(intra_rels)
                            
        return relationships
    
    def _discover_intra_table_relationships(self, schema: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Discover relationships within a single table - enhanced version"""
        relationships = []
        table_name = schema["table_name"]
        
        # For opportunities table specifically
        if table_name == "opportunities":
            # Existing relationships
            if "Primary Contact" in schema["columns"] and "Account Name" in schema["columns"]:
                relationships.append({
                    "from_table": table_name,
                    "from_column": "row_id",
                    "to_table": table_name,
                    "to_column": "row_id",
                    "type": "has_contact",
                    "relationship_type": "intra_table",
                    "via_columns": ["Account Name", "Primary Contact"]
                })
            
            if "Stage" in schema["columns"]:
                relationships.append({
                    "from_table": table_name,
                    "from_column": "row_id",
                    "to_table": table_name,
                    "to_column": "row_id",
                    "type": "stage_progression",
                    "relationship_type": "intra_table",
                    "via_columns": ["Stage", "Close Date"]
                })
            
            # NEW: Amount-based relationships
            if "Amount" in schema["columns"]:
                relationships.extend([
                    {
                        "from_table": table_name,
                        "from_column": "row_id",
                        "to_table": table_name,
                        "to_column": "row_id",
                        "type": "similar_value",
                        "relationship_type": "intra_table",
                        "via_columns": ["Amount"],
                        "criteria": "amount_similarity"
                    },
                    {
                        "from_table": table_name,
                        "from_column": "row_id",
                        "to_table": table_name,
                        "to_column": "row_id",
                        "type": "competitive_deals",
                        "relationship_type": "intra_table",
                        "via_columns": ["Amount", "Close Date", "Account Name"],
                        "criteria": "competing_opportunities"
                    }
                ])
            
            # NEW: Probability-based relationships
            if "Probability (%)" in schema["columns"]:
                relationships.extend([
                    {
                        "from_table": table_name,
                        "from_column": "row_id",
                        "to_table": table_name,
                        "to_column": "row_id",
                        "type": "risk_similarity",
                        "relationship_type": "intra_table",
                        "via_columns": ["Probability (%)"],
                        "criteria": "probability_bands"
                    },
                    {
                        "from_table": table_name,
                        "from_column": "row_id",
                        "to_table": table_name,
                        "to_column": "row_id",
                        "type": "risk_progression",
                        "relationship_type": "intra_table",
                        "via_columns": ["Probability (%)", "Created Date"],
                        "criteria": "probability_changes"
                    }
                ])
            
            # NEW: Next Step patterns
            if "Next Step" in schema["columns"]:
                relationships.extend([
                    {
                        "from_table": table_name,
                        "from_column": "row_id",
                        "to_table": table_name,
                        "to_column": "row_id",
                        "type": "similar_next_steps",
                        "relationship_type": "intra_table",
                        "via_columns": ["Next Step"],
                        "criteria": "next_step_similarity"
                    },
                    {
                        "from_table": table_name,
                        "from_column": "row_id",
                        "to_table": table_name,
                        "to_column": "row_id",
                        "type": "stakeholder_overlap",
                        "relationship_type": "intra_table",
                        "via_columns": ["Next Step", "Primary Contact"],
                        "criteria": "stakeholder_mentions"
                    }
                ])
            
            # NEW: Close Date clustering
            if "Close Date" in schema["columns"]:
                relationships.extend([
                    {
                        "from_table": table_name,
                        "from_column": "row_id",
                        "to_table": table_name,
                        "to_column": "row_id",
                        "type": "same_close_period",
                        "relationship_type": "intra_table",
                        "via_columns": ["Close Date"],
                        "criteria": "temporal_clustering"
                    },
                    {
                        "from_table": table_name,
                        "from_column": "row_id",
                        "to_table": table_name,
                        "to_column": "row_id",
                        "type": "quarter_end_deals",
                        "relationship_type": "intra_table",
                        "via_columns": ["Close Date", "Amount"],
                        "criteria": "quarter_end_patterns"
                    }
                ])
            
            # NEW: Multi-dimensional business relationships
            if all(col in schema["columns"] for col in ["Amount", "Probability (%)", "Stage"]):
                relationships.extend([
                    {
                        "from_table": table_name,
                        "from_column": "row_id",
                        "to_table": table_name,
                        "to_column": "row_id",
                        "type": "deal_health_similarity",
                        "relationship_type": "intra_table",
                        "via_columns": ["Amount", "Probability (%)", "Stage"],
                        "criteria": "health_score_similarity"
                    },
                    {
                        "from_table": table_name,
                        "from_column": "row_id",
                        "to_table": table_name,
                        "to_column": "row_id",
                        "type": "optimization_candidates",
                        "relationship_type": "intra_table",
                        "via_columns": ["Amount", "Probability (%)", "Next Step"],
                        "criteria": "intervention_opportunities"
                    }
                ])
            
            # NEW: Contact and account clustering
            if "Contact: Title" in schema["columns"]:
                relationships.append({
                    "from_table": table_name,
                    "from_column": "row_id",
                    "to_table": table_name,
                    "to_column": "row_id",
                    "type": "similar_contact_level",
                    "relationship_type": "intra_table",
                    "via_columns": ["Contact: Title"],
                    "criteria": "seniority_level"
                })
        
        # Generic entity relationships for any table
        for col_name, col_info in schema["columns"].items():
            # Hierarchical relationships (e.g., parent_id, manager_id)
            if any(pattern in col_name.lower() for pattern in ["parent", "manager", "supervisor"]):
                relationships.append({
                    "from_table": table_name,
                    "from_column": col_name,
                    "to_table": table_name,
                    "to_column": "row_id",
                    "type": "hierarchical",
                    "relationship_type": "intra_table"
                })
            
            # Time-based relationships
            if "date" in col_name.lower() and col_name != "Close Date":
                relationships.append({
                    "from_table": table_name,
                    "from_column": "row_id",
                    "to_table": table_name,
                    "to_column": "row_id",
                    "type": "temporal",
                    "relationship_type": "intra_table",
                    "via_columns": [col_name]
                })
                
        return relationships
    
    def save_schema_config(self, schemas: List[Dict[str, Any]], relationships: List[Dict[str, Any]]):
        """Save discovered schema to configuration files"""
        config = {
            "tables": {s["table_name"]: s for s in schemas},
            "relationships": relationships,
            "metadata": {
                "discovery_timestamp": pd.Timestamp.now().isoformat(),
                "version": "1.0"
            }
        }
        
        # Save main schema config
        with open(self.config_dir / "schema_config.json", "w") as f:
            json.dump(config, f, indent=2, default=str)
            
        # Save simplified schema for LLM context
        llm_schema = self._create_llm_schema(schemas, relationships)
        with open(self.config_dir / "llm_schema.json", "w") as f:
            json.dump(llm_schema, f, indent=2)
            
        logger.info(f"Schema configuration saved to {self.config_dir}")
        
    def _clean_column_name(self, col: str) -> str:
        """Clean column name for use in SQL/Cypher - more aggressive cleaning"""
        import re
        
        # Convert to lowercase
        clean = col.lower()
        
        # Replace common patterns
        clean = re.sub(r'[(%)]', '', clean)  # Remove parentheses and percent signs
        clean = re.sub(r'[:\-\s]+', '_', clean)  # Replace colons, hyphens, spaces with underscore
        clean = re.sub(r'[^a-z0-9_]', '', clean)  # Remove any non-alphanumeric chars except underscore
        clean = re.sub(r'_+', '_', clean)  # Replace multiple underscores with single
        clean = clean.strip('_')  # Remove leading/trailing underscores
        
        # Ensure it starts with a letter or underscore (SQL/Cypher requirement)
        if clean and clean[0].isdigit():
            clean = f"col_{clean}"
        
        # Handle empty result
        if not clean:
            clean = "unknown_column"
        
        return clean
    
    def _is_potential_key(self, col: str, df: pd.DataFrame) -> bool:
        """Check if column could be a primary/foreign key"""
        col_lower = col.lower()
        # Check naming patterns
        if any(pattern in col_lower for pattern in ["_id", "id_", "_key", "_code"]):
            return True
        # Check uniqueness
        if df[col].nunique() == len(df):
            return True
        return False
    
    def _is_potential_reference(self, col: str) -> bool:
        """Check if column name suggests it references another table"""
        col_lower = col.lower()
        return any(pattern in col_lower for pattern in ["_id", "_ref", "_fk", "contact", "account", "parent"])
    
    def _columns_match(self, col1: str, col2: str, info1: Dict, info2: Dict) -> bool:
        """Check if two columns likely represent a relationship"""
        # Direct name match
        if col1.lower() == col2.lower():
            return True
        # ID reference pattern
        if col1.lower().replace("_id", "") == col2.lower().replace("_id", ""):
            return True
        # Table name in column name
        for table_indicator in ["account", "contact", "opportunity", "user", "owner"]:
            if table_indicator in col1.lower() and table_indicator in col2.lower():
                return True
        return False
    
    def _create_llm_schema(self, schemas: List[Dict[str, Any]], relationships: List[Dict[str, Any]]) -> Dict:
        """Create simplified schema for LLM context"""
        llm_schema = {
            "tables": {},
            "relationships": [],
            "entity_nodes": [],
            "business_rules": []
        }
        
        for schema in schemas:
            table_def = {
                "columns": [
                    {
                        "name": info["clean_name"],
                        "type": self._simplify_dtype(info["dtype"]),
                        "description": self._generate_column_description(col, info)
                    }
                    for col, info in schema["columns"].items()
                ]
            }
            llm_schema["tables"][schema["table_name"]] = table_def
        
        # Add relationships
        for r in relationships:
            if r.get("relationship_type") == "inter_table":
                # Clean relationship type for display
                rel_type = (r['from_column'].upper()
                           .replace(" ", "_")
                           .replace(":", "_")
                           .replace("-", "_")
                           .replace("_ID", "")
                           .strip("_"))
                if not rel_type:
                    rel_type = "RELATED_TO"
                llm_schema["relationships"].append(
                    f"({r['from_table']})-[:{rel_type}]->({r['to_table']})"
                )
            elif r.get("relationship_type") == "intra_table":
                llm_schema["relationships"].append(
                    f"({r['from_table']})-[:{r['type'].upper()}]->({r['to_table']}) // Within same table"
                )
        
        # Add entity nodes (for opportunities data)
        if any(s["table_name"] == "opportunities" for s in schemas):
            llm_schema["entity_nodes"] = [
                "(:Stage) - Opportunity stages (Qualify, Propose, etc.)",
                "(:LeadSource) - Lead sources (Web, Referral, etc.)",
                "(:OpportunityType) - Opportunity types (New Business, Renewal, etc.)"
            ]
            
            llm_schema["business_rules"] = [
                "(:Opportunities)-[:IN_STAGE]->(:Stage)",
                "(:Opportunities)-[:ORIGINATED_FROM]->(:LeadSource)",
                "(:Opportunities)-[:OF_TYPE]->(:OpportunityType)",
                "(:Opportunities)-[:SHARES_CONTACT]->(:Opportunities) // Same primary contact",
                "(:Opportunities)-[:SIMILAR_VALUE]->(:Opportunities) // Amount within 10%",
                "(:Opportunities:HighValue) // Opportunities > $300k",
                "(:Opportunities:AtRisk) // High value but low probability"
            ]
        
        return llm_schema
    
    def _simplify_dtype(self, dtype: str) -> str:
        """Convert pandas dtype to simple type for LLM"""
        if "int" in dtype:
            return "INTEGER"
        elif "float" in dtype:
            return "DECIMAL"
        elif "object" in dtype:
            return "TEXT"
        elif "datetime" in dtype:
            return "TIMESTAMP"
        elif "bool" in dtype:
            return "BOOLEAN"
        return "TEXT"
    
    def _generate_column_description(self, col_name: str, info: Dict) -> str:
        """Generate helpful description for column"""
        desc = []
        if info.get("is_potential_key"):
            desc.append("Primary key")
        if info.get("is_potential_reference"):
            desc.append("Foreign key reference")
        if info.get("is_categorical"):
            desc.append(f"Categorical with {info['unique_count']} unique values")
        if "sample_values" in info and len(info["sample_values"]) > 0:
            desc.append(f"e.g. {', '.join(str(v) for v in info['sample_values'][:3])}")
        return "; ".join(desc) if desc else ""