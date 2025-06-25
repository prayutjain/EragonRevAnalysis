# cro_formatters.py - Answer formatting methods for CRO
import re
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
from .cro_visualizers import Visualizer
from .cro_utils import CROUtils

logger = logging.getLogger(__name__)


class AnswerFormatter:
    """Handles formatting of query answers for CRO consumption"""
    
    def __init__(self):
        self.visualizer = Visualizer()
        self.utils = CROUtils()
    
    def format_cro_answer_with_blocks(self, question: str, raw_answer: str, raw_results: List[Dict], confidence: float) -> Dict[str, Any]:
        """Format answer in CRO-friendly way with UI blocks and visuals"""
        # Detect query type and format accordingly
        question_lower = question.lower()
        
        if "top" in question_lower and "account" in question_lower:
            return self.format_top_accounts_answer_with_blocks(raw_results, confidence)
        elif "closing this month" in question_lower:
            return self.format_closing_deals_answer_with_blocks(raw_results, confidence)
        elif "at risk" in question_lower or "at-risk" in question_lower:
            return self.format_at_risk_answer_with_blocks(raw_results, confidence)
        elif "stage" in question_lower and "percentage" in question_lower:
            return self.format_stage_distribution_answer_with_blocks(raw_results, confidence)
        elif "average" in question_lower and "deal" in question_lower:
            return self.format_average_metrics_answer_with_blocks(raw_results, confidence)
        else:
            # Generic formatting
            return self.format_generic_answer_with_blocks(raw_answer, raw_results, confidence)
    
    def format_top_accounts_answer_with_blocks(self, raw_results: List[Dict], confidence: float) -> Dict[str, Any]:
        """Format top accounts answer with executive focus and UI blocks"""
        blocks = []
        visualizations = []
        
        if not raw_results or not raw_results[0]["results"]:
            return {
                "answer": "No account data found.",
                "blocks": [{"type": "markdown", "content": "No account data found."}],
                "visualizations": []
            }
        
        results = raw_results[0]["results"]
        total_pipeline = sum(r.get("total_value", 0) for r in results)
        
        # Create the headline block
        top_account = results[0]
        headline = f"⚡ **{top_account.get('Account Name', 'Unknown')} leads with ${top_account.get('total_value', 0):,.0f} in opportunities**"
        blocks.append({"type": "headline", "text": headline})
        
        # Build the table data
        table_columns = ["Rank", "Account", "Pipeline $", "% of Total", "Action Required"]
        table_rows = []
        
        # Prepare data for visualization
        account_data = {}
        
        for i, account in enumerate(results[:5], 1):
            name = account.get("Account Name", "Unknown")
            value = account.get("total_value", 0)
            pct = (value / total_pipeline * 100) if total_pipeline > 0 else 0
            
            # Store for visualization
            account_data[name[:20] + ('...' if len(name) > 20 else '')] = value
            
            # Suggest action based on value
            if value > 1000000:
                action = "Schedule executive review"
            elif value > 500000:
                action = "Confirm close timeline"
            else:
                action = "Monitor progress"
            
            table_rows.append([
                i,
                name[:30] + ('...' if len(name) > 30 else ''),
                f"${value:,.0f}",
                f"{pct:.1f}%",
                action
            ])
        
        # Add table block
        blocks.append({
            "type": "table",
            "id": "top_accounts",
            "columns": table_columns,
            "rows": table_rows
        })
        
        # Create doughnut chart visualization
        doughnut_html = self.visualizer.generate_doughnut_chart_html(account_data, "top_accounts_doughnut")
        visualizations.append({
            "type": "html",
            "id": "top_accounts_doughnut",
            "title": "Top 5 Accounts Distribution",
            "content": doughnut_html
        })
        
        # Add data table visualization
        table_html = self.visualizer.generate_data_table_html(table_columns, table_rows, "top_accounts_table", "Top Accounts by Pipeline Value")
        visualizations.append({
            "type": "html",
            "id": "top_accounts_table_visual",
            "title": "Top Accounts Data",
            "content": table_html
        })
        
        # Add insights
        top_5_total = sum(r.get("total_value", 0) for r in results[:5])
        top_5_pct = (top_5_total / total_pipeline * 100) if total_pipeline > 0 else 0
        
        insights_text = f"""### Why it matters
These top 5 accounts represent **{top_5_pct:.0f}% of total pipeline** (${top_5_total:,.0f}). 
Accelerating any single deal by 30 days could significantly impact this quarter's results.

### Recommended next steps
→ Account teams to submit updated close plans by **Friday EOD**
→ CRO to review top 3 accounts in **Monday's pipeline review**
→ Finance to validate deal structures for accounts >$1M by **Wednesday**"""
        
        blocks.append({"type": "markdown", "content": insights_text})
        
        # Build the full markdown answer (for backward compatibility)
        answer_parts = [headline]
        answer_parts.append("\n| " + " | ".join(table_columns) + " |")
        answer_parts.append("|" + "|".join(["------" for _ in table_columns]) + "|")
        for row in table_rows:
            answer_parts.append("| " + " | ".join(str(cell) for cell in row) + " |")
        answer_parts.append(insights_text)
        
        return {
            "answer": "\n".join(answer_parts),
            "blocks": blocks,
            "visualizations": visualizations
        }
    
    def format_closing_deals_answer_with_blocks(self, raw_results: List[Dict], confidence: float) -> Dict[str, Any]:
        """Format closing deals answer with blocks and visuals"""
        blocks = []
        visualizations = []
        
        if not raw_results or not raw_results[0]["results"]:
            count = 0
        else:
            count = raw_results[0]["results"][0].get("opportunity_count", 0)
        
        # Get current month name
        current_month = datetime.now().strftime("%B")
        
        # Determine if this is good/bad/neutral
        if count > 20:
            assessment = "strong close"
            emoji = "🟢"
            color = "#10b981"
        elif count > 10:
            assessment = "moderate close"
            emoji = "🟡"
            color = "#f59e0b"
        else:
            assessment = "light close"
            emoji = "🔴"
            color = "#ef4444"
        
        headline = f"{emoji} **{count} opportunities closing in {current_month} — indicating a {assessment}**"
        blocks.append({"type": "headline", "text": headline})
        
        # Add KPI block
        blocks.append({
            "type": "kpis",
            "values": {
                "closing_count": count,
                "month": current_month,
                "assessment": assessment
            }
        })
        
        # Create a simple gauge visualization
        gauge_html = f"""
        <div style="text-align: center; padding: 20px;">
            <div style="position: relative; display: inline-block;">
                <svg width="200" height="120" viewBox="0 0 200 120">
                    <!-- Background arc -->
                    <path d="M 20 100 A 80 80 0 0 1 180 100" 
                          fill="none" 
                          stroke="#e5e7eb" 
                          stroke-width="20"
                          stroke-linecap="round"/>
                    <!-- Progress arc -->
                    <path d="M 20 100 A 80 80 0 0 1 {20 + (160 * min(count/30, 1))} {100 - (80 * min(count/30, 1))}" 
                          fill="none" 
                          stroke="{color}" 
                          stroke-width="20"
                          stroke-linecap="round"/>
                    <!-- Center text -->
                    <text x="100" y="90" text-anchor="middle" font-size="36" font-weight="bold" fill="#111827">{count}</text>
                    <text x="100" y="110" text-anchor="middle" font-size="14" fill="#6b7280">opportunities</text>
                </svg>
            </div>
            <div style="margin-top: 10px; font-size: 18px; font-weight: 600; color: {color};">{assessment.title()}</div>
        </div>
        """
        
        visualizations.append({
            "type": "html",
            "id": "closing_gauge",
            "title": f"{current_month} Close Indicator",
            "content": gauge_html
        })
        
        # Add a summary table for the month
        summary_table_columns = ["Metric", "Value", "Target", "Status"]
        summary_table_rows = [
            ["Opportunities Closing", str(count), "15+", "✅ Met" if count >= 15 else "❌ Below"],
            ["Assessment", assessment.title(), "Strong Close", emoji],
            ["Days Remaining", str((datetime.now().replace(day=28) - datetime.now()).days), "N/A", "📅"],
            ["Recommended Focus", "Execution & Risk" if count > 10 else "Pipeline Acceleration", "N/A", "🎯"]
        ]
        
        summary_table_html = self.visualizer.generate_data_table_html(
            summary_table_columns,
            summary_table_rows,
            "closing_summary_table",
            f"{current_month} Closing Summary"
        )
        visualizations.append({
            "type": "html",
            "id": "closing_summary_table_visual",
            "title": "Monthly Close Summary",
            "content": summary_table_html
        })
        
        insights = f"""### Why it matters
Month-end pipeline directly impacts quarterly attainment. With {count} deals in play, 
focus should be on {"execution and risk mitigation" if count > 10 else "pipeline acceleration"}.

### Recommended next steps
→ Sales ops to flag any deals without {"updated close plans" if count > 10 else "acceleration paths"}
→ AEs to confirm all {"technical validations complete" if count > 10 else "stakeholders engaged"}
→ Leadership to {"clear calendars for approvals" if count > 10 else "identify pull-forward candidates"}"""
        
        blocks.append({"type": "markdown", "content": insights})
        
        return {
            "answer": f"{headline}\n\n{insights}",
            "blocks": blocks,
            "visualizations": visualizations
        }
    
    def format_at_risk_answer_with_blocks(self, raw_results: List[Dict], confidence: float) -> Dict[str, Any]:
        """Format at-risk opportunities answer with blocks and visuals"""
        blocks = []
        visualizations = []
        
        if not raw_results or not raw_results[0]["results"]:
            return {
                "answer": "No at-risk opportunities identified based on current criteria.",
                "blocks": [{"type": "markdown", "content": "No at-risk opportunities identified based on current criteria."}],
                "visualizations": []
            }
        
        results = raw_results[0]["results"]
        
        # Filter out closed deals
        active_risks = [
            r for r in results 
            if r.get("Stage", "").lower() not in ["closed won", "closed lost"]
        ][:3]  # Top 3
        
        if not active_risks:
            return {
                "answer": "✅ No active opportunities currently meet risk criteria.",
                "blocks": [{"type": "markdown", "content": "✅ No active opportunities currently meet risk criteria."}],
                "visualizations": []
            }
        
        total_at_risk = sum(r.get("Amount", 0) for r in active_risks)
        
        headline = f"🔴 **${total_at_risk:,.0f} at risk across {len(active_risks)} deals**"
        blocks.append({"type": "headline", "text": headline})
        
        # Build risk table
        table_columns = ["#", "Account", "Amount", "Stage", "Risk Factor", "Action"]
        table_rows = []
        
        # Prepare data for risk visualization
        risk_data = []
        
        for i, opp in enumerate(active_risks, 1):
            amount = opp.get("Amount", 0)
            prob = opp.get("Probability (%)", 0)
            account = opp.get("Account Name", "Unknown")
            stage = opp.get("Stage", "Unknown")
            next_step = opp.get("Next Step", "No next step defined")
            
            # Determine primary risk factor
            if prob < 20:
                risk_reason = "Low confidence"
                risk_score = 90
            elif "stall" in next_step.lower() or "waiting" in next_step.lower():
                risk_reason = "Stalled progress"
                risk_score = 75
            else:
                risk_reason = "Timeline risk"
                risk_score = 60
            
            risk_data.append({
                "account": account[:20] + ('...' if len(account) > 20 else ''),
                "amount": amount,
                "risk_score": risk_score,
                "risk_reason": risk_reason
            })
            
            table_rows.append([
                i,
                account[:20] + ('...' if len(account) > 20 else ''),
                f"${amount:,.0f}",
                f"{stage} ({prob}%)",
                risk_reason,
                f"Escalate to {self.utils.get_escalation_path(amount)}"
            ])
        
        blocks.append({
            "type": "table",
            "id": "at_risk_deals",
            "columns": table_columns,
            "rows": table_rows
        })
        
        # Create risk heatmap visualization
        heatmap_html = f"""
        <div style="padding: 20px;">
            <h4 style="text-align: center; margin-bottom: 20px;">Risk Heatmap</h4>
            <div style="display: flex; flex-direction: column; gap: 10px;">
        """
        
        for item in risk_data:
            risk_color = "#ef4444" if item["risk_score"] > 80 else "#f59e0b" if item["risk_score"] > 60 else "#eab308"
            heatmap_html += f"""
                <div style="display: flex; align-items: center; gap: 10px;">
                    <div style="width: 150px; font-size: 14px;">{item['account']}</div>
                    <div style="flex: 1; background: #f3f4f6; border-radius: 4px; position: relative; height: 30px;">
                        <div style="background: {risk_color}; width: {item['risk_score']}%; height: 100%; border-radius: 4px; display: flex; align-items: center; padding: 0 10px;">
                            <span style="color: white; font-size: 12px; font-weight: 500;">${item['amount']/1000:.0f}k</span>
                        </div>
                    </div>
                    <div style="width: 100px; font-size: 12px; color: #6b7280;">{item['risk_reason']}</div>
                </div>
            """
        
        heatmap_html += """
            </div>
            <div style="margin-top: 15px; display: flex; gap: 20px; justify-content: center; font-size: 12px; color: #6b7280;">
                <div><span style="display: inline-block; width: 12px; height: 12px; background: #eab308; border-radius: 2px;"></span> Medium Risk</div>
                <div><span style="display: inline-block; width: 12px; height: 12px; background: #f59e0b; border-radius: 2px;"></span> High Risk</div>
                <div><span style="display: inline-block; width: 12px; height: 12px; background: #ef4444; border-radius: 2px;"></span> Critical Risk</div>
            </div>
        </div>
        """
        
        visualizations.append({
            "type": "html",
            "id": "risk_heatmap",
            "title": "At-Risk Deal Heatmap",
            "content": heatmap_html
        })
        
        # Add at-risk deals data table
        risk_table_html = self.visualizer.generate_data_table_html(table_columns, table_rows, "at_risk_table", "At-Risk Opportunities Detail")
        visualizations.append({
            "type": "html",
            "id": "at_risk_table_visual",
            "title": "At-Risk Deals Data",
            "content": risk_table_html
        })
        
        # Add insights
        insights = f"""### Why it matters
These {len(active_risks)} opportunities represent potential slippage that could impact quarterly attainment by 
{(total_at_risk / 5000000 * 100):.0f}% (assuming $5M quarterly target).

### Recommended interventions
→ **Today**: Executive outreach to top 2 at-risk accounts
→ **This week**: Deal review with extended teams
→ **By Friday**: Go/No-Go decision on resource allocation"""
        
        blocks.append({"type": "markdown", "content": insights})
        
        # Build markdown answer
        risk_cards = []
        for i, opp in enumerate(active_risks, 1):
            amount = opp.get("Amount", 0)
            prob = opp.get("Probability (%)", 0)
            account = opp.get("Account Name", "Unknown")
            stage = opp.get("Stage", "Unknown")
            next_step = opp.get("Next Step", "No next step defined")
            
            # Determine primary risk factor
            if prob < 20:
                risk_reason = "Low confidence"
            elif "stall" in next_step.lower() or "waiting" in next_step.lower():
                risk_reason = "Stalled progress"
            else:
                risk_reason = "Timeline risk"
            
            risk_cards.append(f"""
**#{i} {account}** — ${amount:,.0f}
- Stage: {stage} ({prob}% confidence)
- Risk: {risk_reason}
- Next step: {next_step}
- **Action: Escalate to {self.utils.get_escalation_path(amount)}**""")
        
        answer = f"{headline}\n\n### At-Risk Deal Summary\n{''.join(risk_cards)}\n\n{insights}"
        
        return {
            "answer": answer,
            "blocks": blocks,
            "visualizations": visualizations
        }
    
    def format_stage_distribution_answer_with_blocks(self, raw_results: List[Dict], confidence: float) -> Dict[str, Any]:
        """Format stage distribution as a funnel with blocks and visuals"""
        blocks = []
        visualizations = []
        
        # If no raw results but we have them in the base answer, try to parse
        if not raw_results or not any(r.get("results") for r in raw_results):
            logger.warning("No raw results available for stage distribution")
            return self.format_stage_distribution_from_text_with_blocks(confidence)
        
        # Collect all stage data
        stages = {}
        total = 0
        
        for result_set in raw_results:
            for row in result_set.get("results", []):
                stage = row.get("Stage") or row.get("stage")
                count = row.get("stage_count", 0) or row.get("count", 0) or row.get("COUNT(*)", 0)
                if stage and count:
                    stages[stage] = count
                    total += count
        
        if not stages or total == 0:
            return self.format_stage_distribution_from_text_with_blocks(confidence)
        
        # Order stages in funnel sequence
        stage_order = ["Qualify", "Develop", "Propose", "Negotiate", "Closed Won", "Closed Lost"]
        ordered_stages = []
        
        for stage in stage_order:
            if stage in stages:
                ordered_stages.append((stage, stages[stage]))
        
        # Add any stages not in our predefined order
        for stage, count in stages.items():
            if stage not in stage_order:
                ordered_stages.append((stage, count))
        
        # Create headline
        headline = f"📊 **Pipeline Distribution: {total} total opportunities**"
        blocks.append({"type": "headline", "text": headline})
        
        # Create funnel chart data
        funnel_data = []
        for stage, count in ordered_stages:
            pct = (count / total * 100) if total > 0 else 0
            funnel_data.append({
                "stage": stage,
                "count": count,
                "percentage": pct
            })
        
        blocks.append({
            "type": "chart",
            "chartType": "funnel",
            "id": "pipeline_funnel",
            "data": funnel_data
        })
        
        # Generate funnel chart HTML
        funnel_html = self.visualizer.generate_funnel_chart_html(funnel_data, "pipeline_funnel_visual")
        visualizations.append({
            "type": "html",
            "id": "pipeline_funnel_visual",
            "title": "Sales Pipeline Funnel",
            "content": funnel_html
        })
        
        # Add pipeline stage data table
        stage_table_columns = ["Stage", "Count", "Percentage", "Status"]
        stage_table_rows = []
        
        for stage, count in ordered_stages:
            pct = (count / total * 100) if total > 0 else 0
            
            # Determine status
            if stage in ["Closed Won"]:
                status = "✅ Won"
            elif stage in ["Closed Lost"]:
                status = "❌ Lost"
            elif pct > 30:
                status = "🟢 Healthy"
            elif pct > 10:
                status = "🟡 Monitor"
            else:
                status = "🔴 Low"
            
            stage_table_rows.append([stage, count, f"{pct:.1f}%", status])
        
        stage_table_html = self.visualizer.generate_data_table_html(
            stage_table_columns, 
            stage_table_rows, 
            "pipeline_stages_table",
            "Pipeline Stage Distribution"
        )
        visualizations.append({
            "type": "html",
            "id": "pipeline_stages_table_visual",
            "title": "Pipeline Stages Data",
            "content": stage_table_html
        })
        
        # Calculate conversion metrics
        if len(ordered_stages) >= 2:
            top_stage_count = ordered_stages[0][1]
            won_count = stages.get("Closed Won", 0)
            win_rate = (won_count / total * 100) if total > 0 else 0
        else:
            win_rate = 0
        
        # Add KPIs
        blocks.append({
            "type": "kpis",
            "values": {
                "win_rate": f"{win_rate:.1f}%",
                "active_pipeline": total - stages.get('Closed Won', 0) - stages.get('Closed Lost', 0),
                "conversion_quality": self.utils.assess_funnel_health(stages)
            }
        })
        
        # Add insights
        insights = f"""### Key Metrics
- **Win Rate**: {win_rate:.1f}% of all opportunities
- **Active Pipeline**: {total - stages.get('Closed Won', 0) - stages.get('Closed Lost', 0)} deals in flight
- **Conversion Quality**: {self.utils.assess_funnel_health(stages)}

### Why it matters
A healthy funnel shows progressive narrowing with 40-60% in early stages. 
Current distribution indicates {self.utils.get_funnel_diagnosis(stages, total)}.

### Recommended actions
{self.utils.get_funnel_recommendations(stages, total)}"""
        
        blocks.append({"type": "markdown", "content": insights})
        
        # Create funnel visualization for markdown
        funnel_lines = []
        funnel_lines.append("### Pipeline Funnel")
        funnel_lines.append("```")
        
        max_width = 50
        for stage, count in ordered_stages:
            pct = (count / total * 100) if total > 0 else 0
            bar_width = int(pct / 100 * max_width)
            bar = "█" * bar_width
            funnel_lines.append(f"{stage:15} {bar:<{max_width}} {count:3d} ({pct:4.1f}%)")
        
        funnel_lines.append("```")
        
        answer = f"{headline}\n\n{''.join(f'{line}\n' for line in funnel_lines)}\n{insights}"
        
        return {
            "answer": answer,
            "blocks": blocks,
            "visualizations": visualizations
        }
    
    def format_stage_distribution_from_text_with_blocks(self, confidence: float) -> Dict[str, Any]:
        """Fallback formatting when raw_results unavailable"""
        blocks = [
            {"type": "headline", "text": "📊 **Pipeline Stage Distribution**"},
            {"type": "markdown", "content": """### Current Pipeline Stages
Based on the analysis, opportunities are distributed across multiple stages in your sales pipeline.

### Why it matters
Understanding stage distribution helps identify bottlenecks and forecast revenue more accurately.

### Recommended actions
→ Review the detailed breakdown to identify stage concentration
→ Focus acceleration efforts on stages with highest value
→ Monitor conversion rates between stages weekly

*Note: For detailed visualization, ensure data access is properly configured.*"""}
        ]
        
        return {
            "answer": blocks[1]["content"],
            "blocks": blocks,
            "visualizations": []
        }
    
    def format_average_metrics_answer_with_blocks(self, raw_results: List[Dict], confidence: float) -> Dict[str, Any]:
        """Format average deal metrics with blocks and visuals"""
        blocks = []
        visualizations = []
        
        if not raw_results or not raw_results[0]["results"]:
            return {
                "answer": "No data available for average calculations.",
                "blocks": [{"type": "markdown", "content": "No data available for average calculations."}],
                "visualizations": []
            }
        
        results = raw_results[0]["results"]
        
        # Build metrics summary
        metrics_data = []
        overall_avg = None
        
        for row in results:
            stage = row.get("Stage") or row.get("stage")
            avg_amount = row.get("average_deal_size") or row.get("avg_amount") or row.get("AVG(Amount)")
            
            if stage and avg_amount:
                metrics_data.append({
                    "stage": stage,
                    "average": avg_amount
                })
                if not overall_avg:
                    overall_avg = avg_amount
        
        if not metrics_data:
            return {
                "answer": "Unable to calculate average deal sizes.",
                "blocks": [{"type": "markdown", "content": "Unable to calculate average deal sizes."}],
                "visualizations": []
            }
        
        headline = "💰 **Average Deal Size Analysis**"
        blocks.append({"type": "headline", "text": headline})
        
        # Create bar chart
        blocks.append({
            "type": "chart",
            "chartType": "bar",
            "id": "avg_deal_sizes",
            "data": metrics_data,
            "xAxis": "stage",
            "yAxis": "average",
            "yFormat": "currency"
        })
        
        # Generate bar chart HTML
        bar_chart_html = self.visualizer.generate_bar_chart_html(
            metrics_data, 
            "avg_deal_sizes_visual",
            {"xAxis": "stage", "yAxis": "average", "yFormat": "currency", "label": "Average Deal Size"}
        )
        visualizations.append({
            "type": "html",
            "id": "avg_deal_sizes_visual",
            "title": "Average Deal Size by Stage",
            "content": bar_chart_html
        })
        
        # Add metrics data table
        metrics_table_columns = ["Stage", "Average Deal Size", "Variance from Overall", "Trend"]
        metrics_table_rows = []
        
        # Calculate overall average
        overall_avg = sum(item["average"] for item in metrics_data) / len(metrics_data) if metrics_data else 0
        
        for item in metrics_data:
            avg = item["average"]
            variance = ((avg - overall_avg) / overall_avg * 100) if overall_avg > 0 else 0
            
            # Determine trend
            if variance > 10:
                trend = "📈 Above average"
            elif variance < -10:
                trend = "📉 Below average"
            else:
                trend = "➡️ On target"
            
            metrics_table_rows.append([
                item["stage"],
                f"${avg:,.0f}",
                f"{variance:+.1f}%",
                trend
            ])
        
        metrics_table_html = self.visualizer.generate_data_table_html(
            metrics_table_columns,
            metrics_table_rows,
            "avg_deal_metrics_table",
            "Deal Size Analysis by Stage"
        )
        visualizations.append({
            "type": "html",
            "id": "avg_deal_metrics_table_visual",
            "title": "Deal Size Metrics Data",
            "content": metrics_table_html
        })
        
        # Format metrics lines
        metrics_lines = []
        for item in metrics_data:
            metrics_lines.append(f"- **{item['stage']}**: ${item['average']:,.0f}")
        
        insights = f"""### By Stage
{chr(10).join(metrics_lines)}

### Why it matters
Deal size variations by stage indicate pricing pressure or scope changes through the sales cycle. 
Significant drops between stages may signal discounting or feature reduction.

### Recommended actions
→ Review pricing strategy for stages with >20% decrease
→ Analyze win/loss data for correlation with deal size
→ Train team on value preservation techniques"""
        
        blocks.append({"type": "markdown", "content": insights})
        
        answer = f"{headline}\n\n{insights}"
        
        return {
            "answer": answer,
            "blocks": blocks,
            "visualizations": visualizations
        }
    
    def format_generic_answer_with_blocks(self, raw_answer: str, raw_results: List[Dict], confidence: float) -> Dict[str, Any]:
        """Generic formatting for other query types with blocks and visuals"""
        blocks = []
        visualizations = []
        
        # Extract key numbers from the answer
        numbers = re.findall(r'\d+(?:,\d+)*(?:\.\d+)?', raw_answer)
        
        # Create a headline if possible
        if numbers:
            headline = f"📊 **Key Finding: {numbers[0]} {self.utils.extract_metric_context(raw_answer)}**"
        else:
            headline = "📊 **Analysis Results**"
        
        blocks.append({"type": "headline", "text": headline})
        
        # Try to extract any tabular data from raw_results
        if raw_results and raw_results[0].get("results"):
            results = raw_results[0]["results"]
            if len(results) > 1 and isinstance(results[0], dict):
                # We have structured data, try to create a visualization
                # Check if we can create a simple bar chart
                if all(any(isinstance(v, (int, float)) for v in row.values()) for row in results[:5]):
                    # Find numeric and text columns
                    numeric_cols = []
                    text_cols = []
                    
                    for key in results[0].keys():
                        if isinstance(results[0][key], (int, float)):
                            numeric_cols.append(key)
                        else:
                            text_cols.append(key)
                    
                    if numeric_cols and text_cols:
                        # Create a simple bar chart with first text col as x-axis and first numeric as y-axis
                        chart_data = []
                        for row in results[:10]:  # Limit to 10 items
                            chart_data.append({
                                "label": str(row.get(text_cols[0], "Unknown")),
                                "value": row.get(numeric_cols[0], 0)
                            })
                        
                        bar_html = self.visualizer.generate_bar_chart_html(
                            chart_data,
                            "generic_bar_chart",
                            {"xAxis": "label", "yAxis": "value", "label": numeric_cols[0]}
                        )
                        
                        visualizations.append({
                            "type": "html",
                            "id": "generic_visualization",
                            "title": "Data Visualization",
                            "content": bar_html
                        })
                        
                        # Also add the raw data as a table
                        generic_table_html = self.visualizer.generate_data_table_html(
                            ["Label", numeric_cols[0]],
                            [[row.get(text_cols[0], "Unknown"), row.get(numeric_cols[0], 0)] for row in results[:10]],
                            "generic_data_table",
                            "Data Summary"
                        )
                        visualizations.append({
                            "type": "html",
                            "id": "generic_data_table_visual",
                            "title": "Data Table",
                            "content": generic_table_html
                        })
        
        # Add the raw answer as markdown
        blocks.append({"type": "markdown", "content": f"""### What we found
{raw_answer}

### Why it matters
This insight helps identify trends and opportunities for revenue optimization.

### Recommended next steps
→ Review detailed results with relevant stakeholders
→ Identify specific actions based on findings
→ Track progress in next review cycle"""})
        
        return {
            "answer": f"{headline}\n\n{blocks[1]['content']}",
            "blocks": blocks,
            "visualizations": visualizations
        }