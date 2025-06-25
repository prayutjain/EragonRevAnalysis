# cro_utils.py - Utility functions for CRO
import re
from typing import Dict, List, Any, Optional


class CROUtils:
    """Utility functions for CRO-specific operations"""
    
    def get_confidence_indicator(self, confidence: float) -> Dict[str, Any]:
        """Get confidence indicator with emoji and color"""
        if confidence >= 0.9:
            return {"emoji": "🟢", "text": "High confidence", "color": "green"}
        elif confidence >= 0.8:
            return {"emoji": "🟡", "text": "Medium confidence", "color": "yellow"}
        else:
            return {"emoji": "🔴", "text": "Low confidence", "color": "red"}
    
    def get_escalation_path(self, amount: float) -> str:
        """Determine escalation path based on deal size"""
        if amount > 1000000:
            return "C-level executive"
        elif amount > 500000:
            return "VP Sales"
        elif amount > 100000:
            return "Sales Director"
        else:
            return "Sales Manager"
    
    def assess_funnel_health(self, stages: Dict[str, int]) -> str:
        """Assess funnel health"""
        total = sum(stages.values())
        early_stage = sum(stages.get(s, 0) for s in ["Qualify", "Develop"])
        late_stage = sum(stages.get(s, 0) for s in ["Propose", "Negotiate"])
        
        early_pct = (early_stage / total * 100) if total > 0 else 0
        late_pct = (late_stage / total * 100) if total > 0 else 0
        
        if early_pct > 60:
            return "⚠️ Top-heavy (needs acceleration)"
        elif late_pct > 50:
            return "✅ Bottom-heavy (good for closing)"
        else:
            return "👍 Balanced distribution"
    
    def get_funnel_diagnosis(self, stages: Dict[str, int], total: int) -> str:
        """Diagnose funnel issues"""
        early_stage = sum(stages.get(s, 0) for s in ["Qualify", "Develop"])
        early_pct = (early_stage / total * 100) if total > 0 else 0
        
        if early_pct > 70:
            return "significant early-stage concentration requiring acceleration focus"
        elif early_pct < 30:
            return "late-stage concentration suggesting strong closing opportunity but future pipeline risk"
        else:
            return "balanced distribution supporting sustainable growth"
    
    def get_funnel_recommendations(self, stages: Dict[str, int], total: int) -> str:
        """Get funnel-specific recommendations"""
        early_stage = sum(stages.get(s, 0) for s in ["Qualify", "Develop"])
        early_pct = (early_stage / total * 100) if total > 0 else 0
        
        if early_pct > 70:
            return """→ Focus on qualifying and advancing early-stage opportunities
→ Implement stage-gate criteria to accelerate progression
→ Consider bringing in solution consultants earlier"""
        elif early_pct < 30:
            return """→ Immediate focus on new pipeline generation
→ Protect and close late-stage opportunities
→ Launch prospecting blitz to refill top of funnel"""
        else:
            return """→ Maintain current velocity while monitoring conversion rates
→ Focus on stage-to-stage conversion improvements
→ Implement weekly pipeline reviews by stage"""
    
    def extract_metric_context(self, text: str) -> str:
        """Extract context around metrics"""
        # Simple extraction of words around numbers
        words = text.split()
        for i, word in enumerate(words):
            if any(char.isdigit() for char in word):
                # Get surrounding context
                start = max(0, i-2)
                end = min(len(words), i+3)
                return " ".join(words[start:end])
        return "data points identified"
    
    def generate_executive_summary(self, question: str, raw_results: List[Dict]) -> str:
        """Generate one-line executive summary"""
        if not raw_results or not raw_results[0].get("results"):
            return "Analysis complete but no significant findings to report."
        
        # Extract key metric from first result
        first_result = raw_results[0]["results"][0] if raw_results[0]["results"] else {}
        
        # Generate contextual summary based on question type
        question_lower = question.lower()
        if "top" in question_lower:
            return f"Pipeline concentration identified with clear leader emerging"
        elif "at risk" in question_lower:
            return f"Immediate intervention required on high-value opportunities"
        elif "average" in question_lower:
            return f"Pricing and deal size patterns reveal optimization opportunities"
        else:
            return f"Analysis reveals actionable insights for revenue optimization"
    
    def extract_kpis(self, question: str, raw_results: List[Dict]) -> Dict[str, Any]:
        """Extract relevant KPIs from results"""
        kpis = {}
        
        if not raw_results:
            return kpis
        
        # Try to calculate common KPIs
        for result_set in raw_results:
            for row in result_set.get("results", []):
                # Pipeline value
                if "total_value" in row or "sum" in str(row).lower():
                    value = row.get("total_value") or row.get("SUM(Amount)") or 0
                    if value:
                        kpis["total_pipeline"] = f"${value:,.0f}"
                
                # Deal count
                if "count" in str(row).lower():
                    count = row.get("opportunity_count") or row.get("COUNT(*)") or 0
                    if count:
                        kpis["deal_count"] = count
                
                # Average metrics
                if "average" in str(row).lower() or "avg" in str(row).lower():
                    avg = row.get("average_deal_size") or row.get("AVG(Amount)") or 0
                    if avg:
                        kpis["average_deal_size"] = f"${avg:,.0f}"
        
        return kpis
    
    def strip_private_keys(self, result: dict) -> dict:
        """Remove private keys that shouldn't be exposed to CRO interface"""
        private = {"execution_history", "raw_results", "reasoning_steps"}
        return {k: v for k, v in result.items() if k not in private}
    
    def reconstruct_raw_results(self, execution_history: List[Dict]) -> List[Dict]:
        """Attempt to reconstruct raw_results from execution history"""
        import logging
        logger = logging.getLogger(__name__)
        
        raw_results = []
        
        for entry in execution_history:
            if entry.get("phase") == "retrieval" and "tool" in entry:
                # This is a retrieval entry, but it doesn't have the actual results
                # We can at least create a placeholder
                raw_result = {
                    "tool": entry.get("tool"),
                    "query": entry.get("query"),
                    "results": [],  # Empty, but at least the structure is there
                    "result_count": entry.get("result_count", 0)
                }
                raw_results.append(raw_result)
                logger.info(f"Reconstructed entry for {entry.get('tool')} with {entry.get('result_count')} results")
        
        return raw_results