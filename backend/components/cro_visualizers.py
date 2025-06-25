# cro_visualizers.py - Chart and visualization generation methods
import json
from typing import Dict, List, Any, Optional


class Visualizer:
    """Handles generation of HTML visualizations and charts"""
    
    def generate_chart_html(self, chart_type: str, data: Any, options: Dict = None) -> str:
        """Generate HTML for various chart types using Chart.js"""
        chart_id = f"chart_{id(data)}"
        
        if chart_type == "funnel":
            return self.generate_funnel_chart_html(data, chart_id, options)
        elif chart_type == "bar":
            return self.generate_bar_chart_html(data, chart_id, options)
        elif chart_type == "doughnut":
            return self.generate_doughnut_chart_html(data, chart_id, options)
        elif chart_type == "line":
            return self.generate_line_chart_html(data, chart_id, options)
        else:
            return f"<div>Unsupported chart type: {chart_type}</div>"
    
    def generate_funnel_chart_html(self, data: List[Dict], chart_id: str, options: Dict = None) -> str:
        """Generate HTML for funnel chart visualization"""
        html = f"""
        <div class="chart-container" style="position: relative; height:400px; width:100%; max-width:600px; margin: 20px auto;">
            <h4 style="text-align: center; margin-bottom: 20px; color: #1f2937; font-weight: 600;">Sales Pipeline Funnel</h4>
            <canvas id="{chart_id}"></canvas>
        </div>
        <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
        <script>
        (function() {{
            const ctx = document.getElementById('{chart_id}').getContext('2d');
            const funnelData = {json.dumps(data)};
            
            // Calculate funnel dimensions
            const maxCount = Math.max(...funnelData.map(d => d.count));
            const funnelColors = ['#2563eb', '#3b82f6', '#60a5fa', '#93bbfc', '#c3d9fd', '#e0ecff'];
            
            new Chart(ctx, {{
                type: 'bar',
                data: {{
                    labels: funnelData.map(d => `${{d.stage}} (${{d.percentage.toFixed(1)}}%)`),
                    datasets: [{{
                        data: funnelData.map(d => d.count),
                        backgroundColor: funnelColors.slice(0, funnelData.length),
                        borderWidth: 0,
                        barPercentage: 0.9
                    }}]
                }},
                options: {{
                    indexAxis: 'y',
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {{
                        legend: {{ display: false }},
                        tooltip: {{
                            callbacks: {{
                                label: function(context) {{
                                    const item = funnelData[context.dataIndex];
                                    return `Count: ${{item.count}} (${{item.percentage.toFixed(1)}}%)`;
                                }}
                            }}
                        }},
                        datalabels: {{
                            anchor: 'end',
                            align: 'end',
                            formatter: (value, ctx) => value
                        }}
                    }},
                    scales: {{
                        x: {{
                            display: false,
                            max: maxCount * 1.2
                        }},
                        y: {{
                            grid: {{ display: false }}
                        }}
                    }}
                }}
            }});
        }})();
        </script>
        """
        return html

    def generate_bar_chart_html(self, data: List[Dict], chart_id: str, options: Dict = None) -> str:
        """Generate HTML for bar chart visualization"""
        is_currency = options and options.get("yFormat") == "currency"
        title = options.get("title", "Average Deal Size by Stage") if options else "Data Analysis"
        
        html = f"""
        <div class="chart-container" style="position: relative; height:300px; width:100%; max-width:600px; margin: 20px auto;">
            <h4 style="text-align: center; margin-bottom: 20px; color: #1f2937; font-weight: 600;">{title}</h4>
            <canvas id="{chart_id}"></canvas>
        </div>
        <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
        <script>
        (function() {{
            const ctx = document.getElementById('{chart_id}').getContext('2d');
            const chartData = {json.dumps(data)};
            
            new Chart(ctx, {{
                type: 'bar',
                data: {{
                    labels: chartData.map(d => d.{options.get('xAxis', 'stage')}),
                    datasets: [{{
                        label: '{options.get('label', 'Average Deal Size')}',
                        data: chartData.map(d => d.{options.get('yAxis', 'average')}),
                        backgroundColor: '#3b82f6',
                        borderColor: '#2563eb',
                        borderWidth: 1
                    }}]
                }},
                options: {{
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {{
                        legend: {{ display: false }},
                        tooltip: {{
                            callbacks: {{
                                label: function(context) {{
                                    let label = context.dataset.label || '';
                                    if (label) label += ': ';
                                    {'label += "$" + context.parsed.y.toLocaleString();' if is_currency else 'label += context.parsed.y;'}
                                    return label;
                                }}
                            }}
                        }}
                    }},
                    scales: {{
                        y: {{
                            beginAtZero: true,
                            ticks: {{
                                callback: function(value) {{
                                    {'return "$" + value.toLocaleString();' if is_currency else 'return value;'}
                                }}
                            }}
                        }}
                    }}
                }}
            }});
        }})();
        </script>
        """
        return html

    def generate_doughnut_chart_html(self, data: Dict, chart_id: str, options: Dict = None) -> str:
        """Generate HTML for doughnut chart visualization"""
        labels = list(data.keys())
        values = list(data.values())
        title = options.get("title", "Distribution Analysis") if options else "Distribution Analysis"
        
        html = f"""
        <div class="chart-container" style="position: relative; height:300px; width:300px; margin: 20px auto;">
            <h4 style="text-align: center; margin-bottom: 20px; color: #1f2937; font-weight: 600;">{title}</h4>
            <canvas id="{chart_id}"></canvas>
        </div>
        <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
        <script>
        (function() {{
            const ctx = document.getElementById('{chart_id}').getContext('2d');
            
            new Chart(ctx, {{
                type: 'doughnut',
                data: {{
                    labels: {json.dumps(labels)},
                    datasets: [{{
                        data: {json.dumps(values)},
                        backgroundColor: [
                            '#2563eb', '#3b82f6', '#60a5fa', '#93bbfc', 
                            '#c3d9fd', '#e0ecff', '#f3f4f6', '#9ca3af'
                        ],
                        borderWidth: 2,
                        borderColor: '#ffffff'
                    }}]
                }},
                options: {{
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {{
                        legend: {{
                            position: 'right',
                            labels: {{
                                padding: 15,
                                usePointStyle: true
                            }}
                        }},
                        tooltip: {{
                            callbacks: {{
                                label: function(context) {{
                                    const total = context.dataset.data.reduce((a, b) => a + b, 0);
                                    const percentage = ((context.parsed / total) * 100).toFixed(1);
                                    return context.label + ': $' + context.parsed.toLocaleString() + ' (' + percentage + '%)';
                                }}
                            }}
                        }}
                    }}
                }}
            }});
        }})();
        </script>
        """
        return html

    def generate_line_chart_html(self, data: List[Dict], chart_id: str, options: Dict = None) -> str:
        """Generate HTML for line chart visualization"""
        title = options.get("title", "Trend Analysis") if options else "Trend Analysis"
        
        html = f"""
        <div class="chart-container" style="position: relative; height:300px; width:100%; max-width:600px; margin: 20px auto;">
            <h4 style="text-align: center; margin-bottom: 20px; color: #1f2937; font-weight: 600;">{title}</h4>
            <canvas id="{chart_id}"></canvas>
        </div>
        <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
        <script>
        (function() {{
            const ctx = document.getElementById('{chart_id}').getContext('2d');
            const chartData = {json.dumps(data)};
            
            new Chart(ctx, {{
                type: 'line',
                data: {{
                    labels: chartData.map(d => d.{options.get('xAxis', 'label')}),
                    datasets: [{{
                        label: '{options.get('label', 'Trend')}',
                        data: chartData.map(d => d.{options.get('yAxis', 'value')}),
                        borderColor: '#3b82f6',
                        backgroundColor: 'rgba(59, 130, 246, 0.1)',
                        tension: 0.1
                    }}]
                }},
                options: {{
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {{
                        legend: {{ display: false }}
                    }},
                    scales: {{
                        y: {{
                            beginAtZero: true
                        }}
                    }}
                }}
            }});
        }})();
        </script>
        """
        return html
    
    def generate_data_table_html(self, columns: List[str], rows: List[List[Any]], table_id: str, title: str = "") -> str:
        """Generate HTML for a styled data table"""
        
        def format_cell_value(cell):
            """Format cell values, especially numbers"""
            if cell is None:
                return ''
            
            # Handle numeric values
            if isinstance(cell, (int, float)):
                if isinstance(cell, int):
                    return f"{cell:,}"  # Format integers with commas
                else:
                    # Round floats to 2 decimal places and remove trailing zeros
                    rounded = round(cell, 2)
                    if rounded == int(rounded):
                        return f"{int(rounded):,}"
                    else:
                        return f"{rounded:,.2f}".rstrip('0').rstrip('.')
            
            # Handle string values that might be numbers
            if isinstance(cell, str):
                # Check if it's a currency value
                if cell.startswith('$'):
                    try:
                        # Extract number from currency string
                        num_str = cell.replace('$', '').replace(',', '').strip()
                        num = float(num_str)
                        rounded = round(num, 2)
                        if rounded == int(rounded):
                            return f"${int(rounded):,}"
                        else:
                            return f"${rounded:,.2f}".rstrip('0').rstrip('.')
                    except:
                        return cell
                
                # Check if it's a percentage
                elif cell.endswith('%'):
                    try:
                        # Extract number from percentage string
                        num_str = cell.replace('%', '').strip()
                        num = float(num_str)
                        rounded = round(num, 2)
                        if rounded == int(rounded):
                            return f"{int(rounded)}%"
                        else:
                            return f"{rounded:.2f}%".rstrip('0').rstrip('.') 
                    except:
                        return cell
                
                # Check if it's a plain number string
                else:
                    try:
                        num = float(cell)
                        rounded = round(num, 2)
                        if rounded == int(rounded):
                            return f"{int(rounded):,}"
                        else:
                            return f"{rounded:,.2f}".rstrip('0').rstrip('.')
                    except:
                        return cell
            
            return str(cell)
        
        html = f"""
        <div style="margin: 20px auto; max-width: 100%; overflow-x: auto;">
            {f'<h4 style="margin-bottom: 15px; color: #1f2937; font-weight: 600;">{title}</h4>' if title else ''}
            <table id="{table_id}" style="width: 100%; border-collapse: collapse; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;">
                <thead>
                    <tr style="background-color: #f3f4f6; border-bottom: 2px solid #e5e7eb;">
        """
        
        # Add column headers
        for col in columns:
            html += f'<th style="padding: 12px 16px; text-align: left; font-weight: 600; color: #374151; white-space: nowrap;">{col}</th>'
        
        html += """
                    </tr>
                </thead>
                <tbody>
        """
        
        # Add rows
        for i, row in enumerate(rows):
            bg_color = "#ffffff" if i % 2 == 0 else "#f9fafb"
            html += f'<tr style="background-color: {bg_color}; border-bottom: 1px solid #e5e7eb; transition: background-color 0.2s;" onmouseover="this.style.backgroundColor=\'#f3f4f6\'" onmouseout="this.style.backgroundColor=\'{bg_color}\'">'
            
            for j, cell in enumerate(row):
                # Format the cell value
                formatted_cell = format_cell_value(cell)
                
                # Format cells based on content
                cell_style = "padding: 12px 16px; color: #1f2937;"
                
                # Right-align numeric columns
                if isinstance(cell, (int, float)) or (isinstance(formatted_cell, str) and (formatted_cell.startswith('$') or formatted_cell.endswith('%'))):
                    cell_style += " text-align: right;"
                
                # Special formatting for certain cells
                if isinstance(formatted_cell, str):
                    if formatted_cell.startswith('$'):
                        cell_style += " font-weight: 600;"
                    elif '%' in formatted_cell:
                        cell_style += " color: #059669;"
                    elif any(action in formatted_cell.lower() for action in ['schedule', 'confirm', 'escalate', 'monitor']):
                        cell_style += " color: #dc2626; font-weight: 500;"
                
                html += f'<td style="{cell_style}">{formatted_cell}</td>'
            
            html += '</tr>'
        
        html += """
                </tbody>
            </table>
        </div>
        """
        
        return html