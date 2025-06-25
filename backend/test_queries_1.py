# test_queries.py - Test various query patterns
import requests
import json
from typing import Dict, Any
import time

API_URL = "http://localhost:8085"

def test_query(question: str, max_iterations: int = 3, include_history: bool = False) -> Dict[str, Any]:
    """Test a single query against the API"""
    print(f"\n{'='*80}")
    print(f"QUESTION: {question}")
    print(f"{'='*80}")
    
    payload = {
        "question": question,
        "max_iterations": max_iterations,
        "include_full_history": include_history
    }
    
    try:
        start_time = time.time()
        response = requests.post(f"{API_URL}/qa", json=payload)
        response.raise_for_status()
        
        result = response.json()
        
        # Display CRO-optimized fields if available
        if 'confidence_indicator' in result:
            print(f"\n{result['confidence_indicator']['emoji']} CONFIDENCE: {result['confidence_indicator']['text']}")
        else:
            print(f"\nCONFIDENCE: {result.get('confidence_score', 0):.2%}")
        
        if 'executive_summary' in result:
            print(f"\nEXECUTIVE SUMMARY: {result['executive_summary']}")
        
        if 'kpis' in result and result['kpis']:
            print(f"\nKEY METRICS:")
            for kpi, value in result['kpis'].items():
                print(f"  - {kpi.replace('_', ' ').title()}: {value}")
        
        print(f"\nANSWER:\n{result['answer']}")
        
        print(f"\nMETADATA:")
        print(f"  Iterations: {result.get('iterations', 'N/A')}")
        print(f"  Execution Time: {result.get('total_execution_time', 0):.2f}s")
        print(f"  Evidence Count: {len(result.get('evidence', []))}")
        
        if result.get('errors'):
            print(f"\nERRORS:")
            for error in result['errors']:
                print(f"  - {error}")
        
        if include_history and result.get('reasoning_steps'):
            print(f"\nREASONING STEPS:")
            for i, step in enumerate(result['reasoning_steps']):
                print(f"  {i+1}. {step[:100]}..." if len(step) > 100 else f"  {i+1}. {step}")
        
        if include_history and result.get('execution_history'):
            print(f"\nEXECUTION HISTORY:")
            for entry in result['execution_history']:
                print(f"  - Phase: {entry.get('phase')}, Duration: {entry.get('duration', 0):.2f}s")
                if 'tool' in entry:
                    print(f"    Tool: {entry['tool']}")
                    if entry.get('query'):
                        print(f"    Query: {entry['query'][:80]}..." if len(entry['query']) > 80 else f"    Query: {entry['query']}")
        
        return result
        
    except requests.exceptions.RequestException as e:
        print(f"ERROR: Failed to query API - {e}")
        return None
    except Exception as e:
        print(f"ERROR: {e}")
        return None

def check_health():
    """Check API health"""
    try:
        response = requests.get(f"{API_URL}/health")
        response.raise_for_status()
        health = response.json()
        
        print("\nAPI HEALTH CHECK:")
        print(f"Status: {health['status']}")
        print("Connections:")
        for service, status in health['connections'].items():
            print(f"  - {service}: {status}")
        
        return health['status'] == 'healthy'
    except:
        print("ERROR: API is not responding")
        return False

def get_schema_info():
    """Get and display schema information"""
    try:
        response = requests.get(f"{API_URL}/schema")
        response.raise_for_status()
        schema = response.json()
        
        print("\nSCHEMA INFORMATION:")
        print(f"Tables: {len(schema['tables'])}")
        for table, info in schema['tables'].items():
            print(f"  - {table}: {len(info['columns'])} columns")
        
        print(f"\nRelationships: {len(schema['relationships'])}")
        for rel in schema['relationships'][:5]:
            print(f"  - {rel}")
        if len(schema['relationships']) > 5:
            print(f"  ... and {len(schema['relationships']) - 5} more")
        
        return schema
    except:
        print("ERROR: Failed to get schema")
        return None

def run_test_suite():
    """Run a comprehensive test suite"""
    print("CRO ANALYTICS QUERY ENGINE TEST SUITE")
    print("="*80)
    
    # Check health first
    if not check_health():
        print("\nAPI is not healthy. Please check the server.")
        return
    
    # Get schema info
    get_schema_info()
    
    # Test queries organized by type
    test_cases = [
        # SQL Analytics Queries
        {
            "category": "SQL Analytics",
            "queries": [
                "What are the top 5 accounts by total opportunity value?",
                "What is the average deal size by stage?",
                "How many opportunities are closing this month?",
                "Show me the total pipeline value by lead source",
                "What percentage of opportunities are in each stage?"
            ]
        },
        
        # Graph/Relationship Queries
        {
            "category": "Graph Relationships",
            "queries": [
                "Which contacts are involved in multiple opportunities?",
                "Show opportunities that share the same primary contact",
                "Find all opportunities connected to Disney accounts",
                "Which opportunities have progressed through multiple stages?"
            ]
        },
        
        # Vector/Semantic Queries
        {
            "category": "Semantic Search",
            "queries": [
                "Find opportunities similar to high-value enterprise deals",
                "Show me all cloud infrastructure related opportunities",
                "Find deals with next steps involving executive meetings"
            ]
        },
        
        # Complex Multi-Tool Queries
        {
            "category": "Complex Analysis",
            "queries": [
                "What are the common characteristics of deals that close successfully?",
                "Identify the top 3 at-risk opportunities that need immediate attention",
                "Compare the performance of different lead sources for enterprise deals",
                "Which accounts have the highest potential based on current opportunities?"
            ]
        }
    ]
    
    results_summary = []
    
    for test_group in test_cases:
        print(f"\n\n{'#'*80}")
        print(f"# {test_group['category'].upper()}")
        print(f"{'#'*80}")
        
        for question in test_group['queries']:
            result = test_query(question, max_iterations=3, include_history=False)
            
            if result:
                results_summary.append({
                    "category": test_group['category'],
                    "question": question,
                    "confidence": result['confidence_score'],
                    "iterations": result['iterations'],
                    "execution_time": result['total_execution_time'],
                    "evidence_count": len(result['evidence']),
                    "errors": len(result['errors'])
                })
            
            # Small delay between queries
            time.sleep(1)
    
    # Print summary
    print(f"\n\n{'='*80}")
    print("TEST SUMMARY")
    print(f"{'='*80}")
    print(f"Total queries tested: {len(results_summary)}")
    
    if results_summary:
        successful_queries = [r for r in results_summary if r['errors'] == 0]
        failed_queries = [r for r in results_summary if r['errors'] > 0]
        
        print(f"Successful: {len(successful_queries)}")
        print(f"Failed: {len(failed_queries)}")
        
        if successful_queries:
            avg_confidence = sum(r['confidence'] for r in successful_queries) / len(successful_queries)
            avg_time = sum(r['execution_time'] for r in successful_queries) / len(successful_queries)
            
            print(f"\nFor successful queries:")
            print(f"  Average confidence: {avg_confidence:.2%}")
            print(f"  Average execution time: {avg_time:.2f}s")
        
        print("\nResults by category:")
        categories = {}
        for r in results_summary:
            cat = r['category']
            if cat not in categories:
                categories[cat] = []
            categories[cat].append(r)
        
        for cat, results in categories.items():
            successful = [r for r in results if r['errors'] == 0]
            print(f"\n{cat}:")
            print(f"  - Total queries: {len(results)}")
            print(f"  - Successful: {len(successful)}")
            if successful:
                print(f"  - Avg confidence: {sum(r['confidence'] for r in successful) / len(successful):.2%}")
                print(f"  - Avg time: {sum(r['execution_time'] for r in successful) / len(successful):.2f}s")

def demo_cro_features():
    """Demo the CRO-optimized features"""
    print("\n🎯 CRO ANALYTICS DEMO")
    print("="*80)
    print("Showcasing executive-optimized query responses")
    
    demo_queries = [
        {
            "title": "Pipeline Overview",
            "question": "What are the top 5 accounts by total opportunity value?"
        },
        {
            "title": "Monthly Close Forecast",
            "question": "How many opportunities are closing this month?"
        },
        {
            "title": "Risk Assessment",
            "question": "Show me at-risk opportunities that need immediate attention"
        },
        {
            "title": "Pipeline Health",
            "question": "What percentage of opportunities are in each stage?"
        }
    ]
    
    for demo in demo_queries:
        print(f"\n\n{'🔹'*40}")
        print(f"📊 {demo['title'].upper()}")
        print(f"{'🔹'*40}")
        
        result = test_query(demo['question'], max_iterations=1, include_history=False)
        
        if result and result.get('errors'):
            print(f"\n⚠️  Query had errors: {result['errors']}")
        
        # Pause between demos
        input("\nPress Enter for next demo...")

def interactive_mode():
    """Run in interactive mode"""
    print("\nINTERACTIVE QUERY MODE")
    print("Type 'exit' to quit, 'help' for examples, 'demo' for CRO demo")
    print("-"*50)
    
    while True:
        question = input("\nYour question: ").strip()
        
        if question.lower() == 'exit':
            break
        elif question.lower() == 'demo':
            demo_cro_features()
            continue
        elif question.lower() == 'help':
            response = requests.get(f"{API_URL}/examples")
            if response.ok:
                examples = response.json()
                print("\nExample queries:")
                for category, queries in examples.items():
                    print(f"\n{category.replace('_', ' ').title()}:")
                    for q in queries[:2]:
                        print(f"  - {q}")
            continue
        elif not question:
            continue
        
        test_query(question, max_iterations=3, include_history=True)

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "test":
            run_test_suite()
        elif sys.argv[1] == "demo":
            demo_cro_features()
        elif sys.argv[1] == "interactive":
            interactive_mode()
        else:
            # Run single query
            question = " ".join(sys.argv[1:])
            test_query(question, include_history=True)
    else:
        print("Usage:")
        print("  python test_queries.py test          # Run full test suite")
        print("  python test_queries.py demo          # Run CRO feature demo")
        print("  python test_queries.py interactive   # Interactive mode")
        print("  python test_queries.py <question>    # Single query")
        print("\nExample:")
        print('  python test_queries.py "What are the top 5 accounts by value?"')