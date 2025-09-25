#!/usr/bin/env python3
"""
Simple Web Dashboard for Converza Pipeline
Displays performance metrics using a simple HTML interface
"""

import json
import sqlite3
from pathlib import Path
from datetime import datetime
import sys
import os

# Add src to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

def get_data_from_db():
    """Get data from SQLite database"""
    db_path = Path("data/converza_pipeline.db")
    
    if not db_path.exists():
        return None, None
    
    conn = sqlite3.connect(db_path)
    
    # Get overall KPIs
    cursor = conn.execute("""
        SELECT 
            COUNT(*) as total_calls,
            SUM(is_conversion) as total_conversions,
            ROUND(SUM(is_conversion) * 100.0 / COUNT(*), 2) as overall_conversion_rate,
            COALESCE(SUM(upsell_amount), 0) as total_upsell_revenue,
            ROUND(COALESCE(AVG(upsell_amount), 0), 2) as avg_upsell_per_call
        FROM gold_performance_metrics
    """)
    
    kpi_row = cursor.fetchone()
    kpis = {
        'total_calls': kpi_row[0],
        'total_conversions': kpi_row[1],
        'overall_conversion_rate': kpi_row[2] or 0,
        'total_upsell_revenue': kpi_row[3],
        'avg_upsell_per_call': kpi_row[4]
    } if kpi_row else {}
    
    # Get agent performance
    cursor = conn.execute("""
        SELECT 
            agent_name,
            COUNT(*) as total_calls,
            SUM(is_conversion) as conversions,
            ROUND(SUM(is_conversion) * 100.0 / COUNT(*), 2) as conversion_rate,
            COALESCE(SUM(upsell_amount), 0) as total_upsell_revenue,
            SUM(CASE WHEN sentiment_score = 'Positive' THEN 1 ELSE 0 END) as positive_calls,
            SUM(CASE WHEN sentiment_score = 'Negative' THEN 1 ELSE 0 END) as negative_calls,
            SUM(CASE WHEN sentiment_score = 'Neutral' THEN 1 ELSE 0 END) as neutral_calls
        FROM gold_performance_metrics
        GROUP BY agent_name
        ORDER BY conversion_rate DESC, total_upsell_revenue DESC
    """)
    
    agents = []
    for row in cursor.fetchall():
        agents.append({
            'agent_name': row[0],
            'total_calls': row[1],
            'conversions': row[2],
            'conversion_rate': row[3] or 0,
            'total_upsell_revenue': row[4],
            'positive_calls': row[5],
            'negative_calls': row[6],
            'neutral_calls': row[7]
        })
    
    conn.close()
    return kpis, agents

def generate_html_dashboard():
    """Generate a complete HTML dashboard"""
    
    kpis, agents = get_data_from_db()
    
    if not kpis or not agents:
        return """
        <html><body>
        <h1>Converza Dashboard</h1>
        <p style="color: red;">No data available. Please run the pipeline first with: <code>python run_standalone.py</code></p>
        </body></html>
        """
    
    # Calculate sentiment totals
    total_positive = sum(agent['positive_calls'] for agent in agents)
    total_negative = sum(agent['negative_calls'] for agent in agents)
    total_neutral = sum(agent['neutral_calls'] for agent in agents)
    total_sentiment = total_positive + total_negative + total_neutral
    
    # Generate agent performance rows
    agent_rows = ""
    for i, agent in enumerate(agents):
        row_class = "top-performer" if i < 3 else ""
        rank_emoji = "🥇" if i == 0 else "🥈" if i == 1 else "🥉" if i == 2 else ""
        
        agent_rows += f"""
        <tr class="{row_class}">
            <td>{rank_emoji} {agent['agent_name']}</td>
            <td>{agent['total_calls']}</td>
            <td>{agent['conversions']}</td>
            <td>{agent['conversion_rate']:.1f}%</td>
            <td>${agent['total_upsell_revenue']:.2f}</td>
            <td>{agent['positive_calls']}</td>
            <td>{agent['negative_calls']}</td>
            <td>{agent['neutral_calls']}</td>
        </tr>
        """
    
    html_content = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Converza Performance Insight Dashboard</title>
        <style>
            body {{
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                margin: 0;
                padding: 20px;
                background-color: #f5f7fa;
                color: #2c3e50;
            }}
            
            .header {{
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: white;
                padding: 30px;
                border-radius: 10px;
                text-align: center;
                margin-bottom: 30px;
                box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
            }}
            
            .header h1 {{
                margin: 0;
                font-size: 2.5em;
                font-weight: 300;
            }}
            
            .header p {{
                margin: 10px 0 0 0;
                opacity: 0.9;
                font-size: 1.2em;
            }}
            
            .kpi-container {{
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
                gap: 20px;
                margin-bottom: 40px;
            }}
            
            .kpi-card {{
                background: white;
                padding: 25px;
                border-radius: 10px;
                box-shadow: 0 2px 10px rgba(0, 0, 0, 0.1);
                text-align: center;
                border-left: 5px solid #3498db;
                transition: transform 0.3s ease;
            }}
            
            .kpi-card:hover {{
                transform: translateY(-5px);
            }}
            
            .kpi-card.conversion {{ border-left-color: #27ae60; }}
            .kpi-card.revenue {{ border-left-color: #f39c12; }}
            .kpi-card.rate {{ border-left-color: #e74c3c; }}
            .kpi-card.avg {{ border-left-color: #9b59b6; }}
            
            .kpi-title {{
                color: #7f8c8d;
                font-size: 0.9em;
                text-transform: uppercase;
                letter-spacing: 1px;
                margin-bottom: 10px;
            }}
            
            .kpi-value {{
                font-size: 2.5em;
                font-weight: bold;
                margin: 10px 0;
                color: #2c3e50;
            }}
            
            .section {{
                background: white;
                margin: 30px 0;
                border-radius: 10px;
                overflow: hidden;
                box-shadow: 0 2px 10px rgba(0, 0, 0, 0.1);
            }}
            
            .section-header {{
                background: #34495e;
                color: white;
                padding: 20px;
                font-size: 1.3em;
                font-weight: 500;
            }}
            
            .section-content {{
                padding: 20px;
            }}
            
            table {{
                width: 100%;
                border-collapse: collapse;
                margin-top: 10px;
            }}
            
            th, td {{
                padding: 12px;
                text-align: left;
                border-bottom: 1px solid #ecf0f1;
            }}
            
            th {{
                background-color: #f8f9fa;
                font-weight: 600;
                color: #2c3e50;
            }}
            
            .top-performer {{
                background-color: #fff9e6;
                font-weight: 500;
            }}
            
            .sentiment-grid {{
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
                gap: 20px;
                margin-top: 20px;
            }}
            
            .sentiment-card {{
                background: #f8f9fa;
                padding: 20px;
                border-radius: 8px;
                text-align: center;
            }}
            
            .sentiment-card.positive {{
                background: #d5f4e6;
                color: #27ae60;
            }}
            
            .sentiment-card.negative {{
                background: #fadbd8;
                color: #e74c3c;
            }}
            
            .sentiment-card.neutral {{
                background: #eaeded;
                color: #7f8c8d;
            }}
            
            .sentiment-emoji {{
                font-size: 2em;
                margin-bottom: 10px;
            }}
            
            .timestamp {{
                text-align: center;
                color: #7f8c8d;
                margin-top: 30px;
                font-style: italic;
            }}
            
            .insight-box {{
                background: #e8f4fd;
                border-left: 4px solid #3498db;
                padding: 20px;
                margin: 20px 0;
                border-radius: 0 8px 8px 0;
            }}
            
            .insight-title {{
                font-weight: bold;
                color: #2c3e50;
                margin-bottom: 10px;
            }}
        </style>
    </head>
    <body>
        <div class="header">
            <h1>🎯 Converza Performance Insight Dashboard</h1>
            <p>Transform operational voice conversations into actionable ROI-driven KPIs</p>
        </div>
        
        <div class="kpi-container">
            <div class="kpi-card">
                <div class="kpi-title">Total Calls</div>
                <div class="kpi-value">📞 {kpis['total_calls']:,}</div>
            </div>
            
            <div class="kpi-card conversion">
                <div class="kpi-title">Conversions</div>
                <div class="kpi-value">✅ {kpis['total_conversions']:,}</div>
            </div>
            
            <div class="kpi-card rate">
                <div class="kpi-title">Conversion Rate</div>
                <div class="kpi-value">📊 {kpis['overall_conversion_rate']:.1f}%</div>
            </div>
            
            <div class="kpi-card revenue">
                <div class="kpi-title">Total Upsell Revenue</div>
                <div class="kpi-value">💰 ${kpis['total_upsell_revenue']:,.2f}</div>
            </div>
            
            <div class="kpi-card avg">
                <div class="kpi-title">Avg Upsell per Call</div>
                <div class="kpi-value">💵 ${kpis['avg_upsell_per_call']:.2f}</div>
            </div>
        </div>
        
        <div class="insight-box">
            <div class="insight-title">🎯 Key Insight</div>
            <p>Your team is generating <strong>${kpis['total_upsell_revenue']:,.2f}</strong> in additional revenue from upselling, 
            with a <strong>{kpis['overall_conversion_rate']:.1f}% conversion rate</strong>. 
            Focus on coaching agents with lower conversion rates to improve overall performance.</p>
        </div>
        
        <div class="section">
            <div class="section-header">
                👥 Agent Performance Analysis
            </div>
            <div class="section-content">
                <p><strong>How managers move beyond anecdotal evidence:</strong> This table provides 100% visibility into team performance, 
                enabling data-driven coaching decisions and identifying top performers vs those requiring training.</p>
                
                <table>
                    <thead>
                        <tr>
                            <th>Agent Name</th>
                            <th>Total Calls</th>
                            <th>Conversions</th>
                            <th>Conversion Rate</th>
                            <th>Upsell Revenue</th>
                            <th>Positive Calls</th>
                            <th>Negative Calls</th>
                            <th>Neutral Calls</th>
                        </tr>
                    </thead>
                    <tbody>
                        {agent_rows}
                    </tbody>
                </table>
            </div>
        </div>
        
        <div class="section">
            <div class="section-header">
                🎭 Sentiment Analysis Overview
            </div>
            <div class="section-content">
                <p>Customer sentiment distribution across all calls, indicating overall satisfaction and experience quality.</p>
                
                <div class="sentiment-grid">
                    <div class="sentiment-card positive">
                        <div class="sentiment-emoji">😊</div>
                        <div style="font-size: 1.5em; font-weight: bold;">{total_positive}</div>
                        <div>Positive Calls</div>
                        <div>({total_positive/total_sentiment*100:.1f}%)</div>
                    </div>
                    
                    <div class="sentiment-card neutral">
                        <div class="sentiment-emoji">😐</div>
                        <div style="font-size: 1.5em; font-weight: bold;">{total_neutral}</div>
                        <div>Neutral Calls</div>
                        <div>({total_neutral/total_sentiment*100:.1f}%)</div>
                    </div>
                    
                    <div class="sentiment-card negative">
                        <div class="sentiment-emoji">😞</div>
                        <div style="font-size: 1.5em; font-weight: bold;">{total_negative}</div>
                        <div>Negative Calls</div>
                        <div>({total_negative/total_sentiment*100:.1f}%)</div>
                    </div>
                </div>
            </div>
        </div>
        
        <div class="section">
            <div class="section-header">
                🚀 Pipeline Architecture
            </div>
            <div class="section-content">
                <p><strong>Bronze → Silver → Gold Data Pipeline:</strong></p>
                <ul>
                    <li><strong>Bronze Layer:</strong> Raw call transcripts stored as-is for full audit trail</li>
                    <li><strong>Silver Layer:</strong> Cleaned and normalized data ready for analysis</li>
                    <li><strong>Gold Layer:</strong> Business KPIs extracted (conversions, upsells, sentiment)</li>
                </ul>
                
                <p><strong>Technology Stack:</strong> Python 3.13 + UV, SQLite, Apache Airflow (Docker)</p>
                <p><strong>Real-time Processing:</strong> Airflow DAG processes calls hourly, dashboard updates automatically</p>
            </div>
        </div>
        
        <div class="timestamp">
            📅 Dashboard generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | 🗄️ Database: data/converza_pipeline.db
        </div>
        
        <script>
            // Auto-refresh every 30 seconds
            setTimeout(function(){{
                location.reload();
            }}, 30000);
        </script>
    </body>
    </html>
    """
    
    return html_content

def main():
    """Generate and save HTML dashboard"""
    html_content = generate_html_dashboard()
    
    # Save to file
    dashboard_path = Path("dashboard.html")
    with open(dashboard_path, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    print("🎯 Converza Dashboard Generated!")
    print(f"📁 Location: {dashboard_path.absolute()}")
    print(f"🌐 Open in browser: file://{dashboard_path.absolute()}")
    print("🔄 Auto-refreshes every 30 seconds")
    
    # Try to open in browser automatically
    import webbrowser
    try:
        webbrowser.open(f"file://{dashboard_path.absolute()}")
        print("🚀 Dashboard opened in your default browser!")
    except:
        print("💡 Manually open the dashboard.html file in your browser")

if __name__ == "__main__":
    main()