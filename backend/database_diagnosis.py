#!/usr/bin/env python3
"""
Database Diagnostics Script
Checks the contents of DuckDB, Neo4j, and ChromaDB
"""

import json
import logging
from pathlib import Path
from datetime import datetime
import os

import duckdb
from neo4j import GraphDatabase
import chromadb
from chromadb.config import Settings
from tabulate import tabulate
from langchain.embeddings import OpenAIEmbeddings
embedding_function = OpenAIEmbeddings()
# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Import your config (adjust path as needed)
from config import (
    DUCKDB_PATH, CHROMA_PERSIST_DIR, 
    NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD,
    CONFIG_DIR
)


class DatabaseDiagnostics:
    """Comprehensive database diagnostics tool"""
    
    def __init__(self):
        self.duckdb_con = None
        self.neo4j_driver = None
        self.chroma_client = None
        self.embedding_function = embedding_function
        
    def connect_all(self):
        """Connect to all databases"""
        try:
            # DuckDB
            self.duckdb_con = duckdb.connect(str(DUCKDB_PATH))
            print("✓ Connected to DuckDB")
            
            # Neo4j
            self.neo4j_driver = GraphDatabase.driver(
                NEO4J_URI, 
                auth=(NEO4J_USER, NEO4J_PASSWORD)
            )
            print("✓ Connected to Neo4j")
            
            # ChromaDB
            self.chroma_client = chromadb.PersistentClient(
                path=str(CHROMA_PERSIST_DIR),
                settings=Settings(anonymized_telemetry=False)
            )
            print("✓ Connected to ChromaDB")
            
        except Exception as e:
            print(f"❌ Connection error: {e}")
            raise
    
    def diagnose_duckdb(self):
        """Diagnose DuckDB contents"""
        print("\n" + "="*80)
        print("DUCKDB DIAGNOSTICS")
        print("="*80)
        
        try:
            # List all tables
            tables_query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'main'
            """
            tables = self.duckdb_con.execute(tables_query).fetchall()
            
            print(f"\n📊 Found {len(tables)} tables:")
            
            for (table_name,) in tables:
                print(f"\n📋 Table: {table_name}")
                
                # Get row count
                count = self.duckdb_con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                print(f"   Rows: {count:,}")
                
                # Get columns
                columns_query = f"PRAGMA table_info({table_name})"
                columns = self.duckdb_con.execute(columns_query).fetchall()
                print(f"   Columns ({len(columns)}):")
                for col in columns[:10]:  # Show first 10 columns
                    print(f"     - {col[1]} ({col[2]})")
                if len(columns) > 10:
                    print(f"     ... and {len(columns) - 10} more columns")
                
                # Show sample data
                if count > 0:
                    print(f"\n   Sample data (first 3 rows):")
                    sample = self.duckdb_con.execute(f"SELECT * FROM {table_name} LIMIT 3").fetchdf()
                    print(tabulate(sample, headers='keys', tablefmt='grid', maxcolwidths=20))
                
                # Special checks for specific tables
                if table_name == "opportunities" and count > 0:
                    # Check for AT&T related records
                    att_check = self.duckdb_con.execute("""
                        SELECT COUNT(*) as count
                        FROM opportunities 
                        WHERE "Account Name" LIKE '%AT&T%' 
                           OR "Account Name" LIKE '%ATT%'
                           OR "Account Name" LIKE '%AT and T%'
                    """).fetchone()
                    print(f"\n   🔍 AT&T related opportunities: {att_check[0]}")
                    
                    # Show some AT&T records if they exist
                    if att_check[0] > 0:
                        att_samples = self.duckdb_con.execute("""
                            SELECT "ID", "Account Name", "Opportunity Name", "Amount"
                            FROM opportunities 
                            WHERE "Account Name" LIKE '%AT&T%' 
                               OR "Account Name" LIKE '%ATT%'
                            LIMIT 3
                        """).fetchdf()
                        print("\n   AT&T Sample Records:")
                        print(tabulate(att_samples, headers='keys', tablefmt='grid'))
                
        except Exception as e:
            print(f"❌ DuckDB error: {e}")
            import traceback
            traceback.print_exc()
    
    def diagnose_neo4j(self):
        """Diagnose Neo4j contents"""
        print("\n" + "="*80)
        print("NEO4J DIAGNOSTICS")
        print("="*80)
        
        try:
            with self.neo4j_driver.session() as session:
                # Get all node labels
                labels_result = session.run("CALL db.labels()")
                labels = [record[0] for record in labels_result]
                
                print(f"\n🏷️  Found {len(labels)} node labels:")
                
                for label in labels:
                    # Count nodes
                    count_result = session.run(f"MATCH (n:{label}) RETURN count(n) as count")
                    count = count_result.single()["count"]
                    print(f"\n📍 Label: {label}")
                    print(f"   Nodes: {count:,}")
                    
                    # Get sample properties
                    if count > 0:
                        sample_result = session.run(f"MATCH (n:{label}) RETURN n LIMIT 1")
                        sample_node = sample_result.single()["n"]
                        print(f"   Properties: {list(sample_node.keys())}")
                        
                        # Show sample data
                        sample_data = session.run(f"MATCH (n:{label}) RETURN n LIMIT 3").data()
                        print(f"   Sample nodes:")
                        for i, record in enumerate(sample_data):
                            node = record['n']
                            print(f"     {i+1}. ID: {node.get('id', 'N/A')}, Name: {node.get('name', node.get('account_name', 'N/A'))}")
                    
                    # Check for AT&T in Opportunities
                    if label == "Opportunities" and count > 0:
                        att_check = session.run("""
                            MATCH (n:Opportunities)
                            WHERE n.account_name CONTAINS 'AT&T' 
                               OR n.account_name CONTAINS 'ATT'
                            RETURN count(n) as count
                        """).single()
                        print(f"\n   🔍 AT&T related nodes: {att_check['count']}")
                
                # Get all relationship types
                print(f"\n🔗 Relationship types:")
                rel_result = session.run("CALL db.relationshipTypes()")
                rel_types = [record[0] for record in rel_result]
                
                for rel_type in rel_types:
                    count_result = session.run(
                        f"MATCH ()-[r:{rel_type}]->() RETURN count(r) as count"
                    )
                    count = count_result.single()["count"]
                    print(f"   - {rel_type}: {count:,} relationships")
                
        except Exception as e:
            print(f"❌ Neo4j error: {e}")
            import traceback
            traceback.print_exc()
    
    def diagnose_chromadb(self):
        """Diagnose ChromaDB contents"""
        print("\n" + "="*80)
        print("CHROMADB DIAGNOSTICS")
        print("="*80)
        
        try:
            # List all collections
            collections = self.chroma_client.list_collections()
            print(f"\n📚 Found {len(collections)} collections:")
            
            for collection in collections:
                print(f"\n📁 Collection: {collection.name}")
                
                # Get collection
                if self.embedding_function:
                    col = self.chroma_client.get_collection(
                        collection.name, 
                        embedding_function=self.embedding_function
                    )
                else:
                    col = self.chroma_client.get_collection(collection.name)
                # col = self.chroma_client.get_collection(collection.name)
                
                # Count items
                count = col.count()
                print(f"   Documents: {count:,}")
                
                if count > 0:
                    # Get sample documents
                    sample = col.get(limit=3, include=['metadatas', 'documents'])
                    
                    print(f"   Sample IDs: {sample['ids'][:3]}")
                    
                    # Show metadata structure
                    if sample.get('metadatas') and len(sample['metadatas']) > 0:
                        print(f"   Metadata keys: {list(sample['metadatas'][0].keys())}")
                        print(f"   Sample metadata:")
                        for i, metadata in enumerate(sample['metadatas'][:2]):
                            print(f"     {i+1}. {json.dumps(metadata, indent=6)[:200]}...")
                    
                    # Show document samples
                    if sample.get('documents') and len(sample['documents']) > 0:
                        print(f"   Sample documents:")
                        for i, doc in enumerate(sample['documents'][:2]):
                            print(f"     {i+1}. {doc[:100]}...")
                    
                    # Test search functionality
                    print(f"\n   🔍 Testing search functionality:")
                    
                    # Test 1: Simple text search
                    test_results = col.query(
                        query_texts=["AT&T"],
                        n_results=3
                    )
                    found = len(test_results['ids'][0]) if test_results['ids'] else 0
                    print(f"     - Search for 'AT&T': {found} results")
                    
                    if found > 0:
                        print(f"       Top result distance: {test_results['distances'][0][0]:.4f}")
                        if test_results.get('metadatas'):
                            print(f"       Top result metadata: {test_results['metadatas'][0][0]}")
                    
                    # Test 2: Search with different variations
                    for term in ["ATT", "telecommunications", "account"]:
                        test_results = col.query(query_texts=[term], n_results=1)
                        found = len(test_results['ids'][0]) if test_results['ids'] else 0
                        print(f"     - Search for '{term}': {found} results")
                
        except Exception as e:
            print(f"❌ ChromaDB error: {e}")
            import traceback
            traceback.print_exc()
    
    def check_data_consistency(self):
        """Check data consistency across databases"""
        print("\n" + "="*80)
        print("DATA CONSISTENCY CHECKS")
        print("="*80)
        
        try:
            # Check if the same tables exist in different forms
            print("\n🔄 Checking table/collection consistency:")
            
            # Get DuckDB tables
            tables = self.duckdb_con.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'main'
            """).fetchall()
            duckdb_tables = {t[0] for t in tables}
            
            # Get ChromaDB collections
            chroma_collections = {c.name for c in self.chroma_client.list_collections()}
            
            # Get Neo4j labels
            with self.neo4j_driver.session() as session:
                labels_result = session.run("CALL db.labels()")
                neo4j_labels = {record[0] for record in labels_result}
            
            print(f"\n   DuckDB tables: {duckdb_tables}")
            print(f"   ChromaDB collections: {chroma_collections}")
            print(f"   Neo4j labels: {neo4j_labels}")
            
            # Check for expected vector collections
            print("\n   Expected vector collections:")
            for table in duckdb_tables:
                expected_vector = f"{table}_vectors"
                if expected_vector in chroma_collections:
                    print(f"     ✓ {table} → {expected_vector}")
                else:
                    print(f"     ❌ {table} → {expected_vector} (MISSING)")
            
        except Exception as e:
            print(f"❌ Consistency check error: {e}")
    
    def generate_summary_report(self):
        """Generate a summary report"""
        print("\n" + "="*80)
        print("SUMMARY REPORT")
        print("="*80)
        
        report = {
            "timestamp": datetime.now().isoformat(),
            "databases": {
                "duckdb": {"status": "unknown"},
                "neo4j": {"status": "unknown"},
                "chromadb": {"status": "unknown"}
            }
        }
        
        # DuckDB summary
        try:
            tables = self.duckdb_con.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'main'
            """).fetchall()
            
            total_rows = 0
            for (table,) in tables:
                count = self.duckdb_con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                total_rows += count
            
            report["databases"]["duckdb"] = {
                "status": "connected",
                "tables": len(tables),
                "total_rows": total_rows
            }
            print(f"\n✓ DuckDB: {len(tables)} tables, {total_rows:,} total rows")
            
        except Exception as e:
            report["databases"]["duckdb"]["error"] = str(e)
            print(f"\n❌ DuckDB: Error - {e}")
        
        # Neo4j summary
        try:
            with self.neo4j_driver.session() as session:
                node_count = session.run("MATCH (n) RETURN count(n) as count").single()["count"]
                rel_count = session.run("MATCH ()-[r]->() RETURN count(r) as count").single()["count"]
                
                report["databases"]["neo4j"] = {
                    "status": "connected",
                    "nodes": node_count,
                    "relationships": rel_count
                }
                print(f"✓ Neo4j: {node_count:,} nodes, {rel_count:,} relationships")
                
        except Exception as e:
            report["databases"]["neo4j"]["error"] = str(e)
            print(f"❌ Neo4j: Error - {e}")
        
        # ChromaDB summary
        try:
            collections = self.chroma_client.list_collections()
            total_docs = sum(c.count() for c in collections)
            
            report["databases"]["chromadb"] = {
                "status": "connected",
                "collections": len(collections),
                "total_documents": total_docs
            }
            print(f"✓ ChromaDB: {len(collections)} collections, {total_docs:,} total documents")
            
        except Exception as e:
            report["databases"]["chromadb"]["error"] = str(e)
            print(f"❌ ChromaDB: Error - {e}")
        
        # Save report
        report_path = Path("database_diagnostics_report.json")
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2)
        print(f"\n📄 Full report saved to: {report_path}")
        
        return report
    
    def close_all(self):
        """Close all database connections"""
        if self.duckdb_con:
            self.duckdb_con.close()
        if self.neo4j_driver:
            self.neo4j_driver.close()
        print("\n✓ All connections closed")


def main():
    """Run the diagnostics"""
    print("🔍 Starting Database Diagnostics...")
    print(f"   DuckDB Path: {DUCKDB_PATH}")
    print(f"   ChromaDB Path: {CHROMA_PERSIST_DIR}")
    print(f"   Neo4j URI: {NEO4J_URI}")
    
    diagnostics = DatabaseDiagnostics()
    
    try:
        # Connect to all databases
        diagnostics.connect_all()
        
        # Run individual diagnostics
        diagnostics.diagnose_duckdb()
        diagnostics.diagnose_neo4j()
        diagnostics.diagnose_chromadb()
        
        # Check consistency
        diagnostics.check_data_consistency()
        
        # Generate summary
        diagnostics.generate_summary_report()
        
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        diagnostics.close_all()


if __name__ == "__main__":
    main()