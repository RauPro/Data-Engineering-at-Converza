"""
Converza Performance Insight Dashboard
Interactive dashboard showing call performance analytics and KPIs
"""

import dash
from dash import dcc, html, Input, Output, dash_table
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import sys
import os
from datetime import datetime, timedelta
import logging

# Add src directory to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

try:
    from database_utils import DatabaseManager
except ImportError:
    # Fallback for when running outside Docker
    DatabaseManager = None

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Dash app
app = dash.Dash(__name__)
app.title = "Converza Performance Insight Dashboard"

# Custom CSS styling
app.layout = html.Div([
    # Header
    html.Div([
        html.H1("Converza Performance Insight Dashboard", 
                style={'text-align': 'center', 'color': '#2c3e50', 'margin': '20px'}),
        html.P("Transform operational voice conversations into actionable ROI-driven KPIs",
               style={'text-align': 'center', 'color': '#7f8c8d', 'font-size': '18px'})
    ], style={'background-color': '#ecf0f1', 'padding': '20px', 'margin-bottom': '20px'}),
    
    # Auto-refresh component
    dcc.Interval(
        id='interval-component',
        interval=30*1000,  # Update every 30 seconds
        n_intervals=0
    ),
    
    # Overall KPIs Row
    html.Div([
        html.H2("Overall Performance KPIs", style={'color': '#2c3e50', 'margin': '20px'}),
        html.Div(id='kpi-cards', children=[])
    ], style={'margin': '20px'}),
    
    # Charts Row
    html.Div([
        html.Div([
            html.H3("Agent Performance Comparison", style={'color': '#2c3e50'}),
            dcc.Graph(id='agent-performance-chart')
        ], style={'width': '50%', 'display': 'inline-block', 'padding': '10px'}),
        
        html.Div([
            html.H3("Conversion Rate vs Upsell Revenue", style={'color': '#2c3e50'}),
            dcc.Graph(id='conversion-upsell-scatter')
        ], style={'width': '50%', 'display': 'inline-block', 'padding': '10px'})
    ]),
    
    # Agent Performance Table
    html.Div([
        html.H3("Detailed Agent Performance", style={'color': '#2c3e50', 'margin': '20px'}),
        html.Div(id='agent-table-container')
    ], style={'margin': '20px'}),
    
    # Sentiment Analysis
    html.Div([
        html.Div([
            html.H3("Sentiment Distribution", style={'color': '#2c3e50'}),
            dcc.Graph(id='sentiment-pie-chart')
        ], style={'width': '50%', 'display': 'inline-block', 'padding': '10px'}),
        
        html.Div([
            html.H3("Daily Trend Analysis", style={'color': '#2c3e50'}),
            dcc.Graph(id='daily-trend-chart')
        ], style={'width': '50%', 'display': 'inline-block', 'padding': '10px'})
    ])
], style={'font-family': 'Arial, sans-serif'})

def get_sample_data():
    """Generate sample data when database is not available"""
    agents = ['Sarah Johnson', 'Michael Chen', 'Emily Rodriguez', 'David Smith', 'Lisa Thompson']
    
    sample_kpis = {
        'total_calls': 150,
        'total_conversions': 45,
        'overall_conversion_rate': 30.0,
        'total_upsell_revenue': 2750.0,
        'avg_upsell_per_call': 18.33
    }
    
    sample_agent_data = []
    for i, agent in enumerate(agents):
        sample_agent_data.append({
            'agent_name': agent,
            'total_calls': 30 + i * 5,
            'conversions': 8 + i * 2,
            'conversion_rate': (8 + i * 2) / (30 + i * 5) * 100,
            'total_upsell_revenue': 400 + i * 150,
            'positive_calls': 15 + i * 3,
            'negative_calls': 3 + i,
            'neutral_calls': 12 + i * 2
        })
    
    return sample_kpis, sample_agent_data

def create_kpi_card(title, value, subtitle="", color="#3498db"):
    """Create a KPI card component"""
    return html.Div([
        html.H3(title, style={'color': color, 'margin': '0'}),
        html.H2(value, style={'color': '#2c3e50', 'margin': '10px 0'}),
        html.P(subtitle, style={'color': '#7f8c8d', 'margin': '0'})
    ], style={
        'background-color': 'white',
        'padding': '20px',
        'border-radius': '10px',
        'box-shadow': '0 2px 4px rgba(0,0,0,0.1)',
        'text-align': 'center',
        'margin': '10px',
        'width': '200px',
        'display': 'inline-block'
    })

@app.callback(
    [Output('kpi-cards', 'children'),
     Output('agent-performance-chart', 'figure'),
     Output('conversion-upsell-scatter', 'figure'),
     Output('agent-table-container', 'children'),
     Output('sentiment-pie-chart', 'figure'),
     Output('daily-trend-chart', 'figure')],
    [Input('interval-component', 'n_intervals')]
)
def update_dashboard(n):
    """Update all dashboard components"""
    
    try:
        if DatabaseManager:
            # Try to get real data from database
            db = DatabaseManager()
            kpis = db.get_dashboard_kpis()
            agent_data = db.get_agent_performance_summary()
            agent_data = [dict(agent) for agent in agent_data] if agent_data else []
        else:
            # Use sample data
            kpis, agent_data = get_sample_data()
    except Exception as e:
        logger.warning(f"Database connection failed, using sample data: {e}")
        kpis, agent_data = get_sample_data()
    
    # Create KPI cards
    kpi_cards = [
        create_kpi_card("Total Calls", f"{kpis.get('total_calls', 0):,}", color="#3498db"),
        create_kpi_card("Conversions", f"{kpis.get('total_conversions', 0):,}", color="#27ae60"),
        create_kpi_card("Conversion Rate", f"{kpis.get('overall_conversion_rate', 0):.1f}%", color="#e74c3c"),
        create_kpi_card("Total Upsell Revenue", f"${kpis.get('total_upsell_revenue', 0):,.2f}", color="#f39c12"),
        create_kpi_card("Avg Upsell/Call", f"${kpis.get('avg_upsell_per_call', 0):.2f}", color="#9b59b6")
    ]
    
    # Agent Performance Bar Chart
    if agent_data:
        agent_df = pd.DataFrame(agent_data)
        
        agent_performance_fig = px.bar(
            agent_df, 
            x='agent_name', 
            y='conversion_rate',
            title="Agent Conversion Rates",
            color='total_upsell_revenue',
            color_continuous_scale='Viridis'
        )
        agent_performance_fig.update_layout(
            xaxis_title="Agent",
            yaxis_title="Conversion Rate (%)",
            template="plotly_white"
        )
        
        # Conversion vs Upsell Scatter Plot
        conversion_upsell_fig = px.scatter(
            agent_df,
            x='conversion_rate',
            y='total_upsell_revenue',
            size='total_calls',
            hover_name='agent_name',
            title="Conversion Rate vs Upsell Revenue by Agent"
        )
        conversion_upsell_fig.update_layout(
            xaxis_title="Conversion Rate (%)",
            yaxis_title="Total Upsell Revenue ($)",
            template="plotly_white"
        )
        
        # Agent Performance Table
        table_data = agent_df.round(2).to_dict('records')
        agent_table = dash_table.DataTable(
            data=table_data,
            columns=[
                {"name": "Agent", "id": "agent_name"},
                {"name": "Total Calls", "id": "total_calls"},
                {"name": "Conversions", "id": "conversions"},
                {"name": "Conversion Rate (%)", "id": "conversion_rate"},
                {"name": "Upsell Revenue ($)", "id": "total_upsell_revenue"},
                {"name": "Positive Calls", "id": "positive_calls"},
                {"name": "Negative Calls", "id": "negative_calls"}
            ],
            style_table={'overflowX': 'auto'},
            style_cell={'textAlign': 'left'},
            style_data_conditional=[
                {
                    'if': {'filter_query': '{conversion_rate} > 25'},
                    'backgroundColor': '#d5f4e6',
                    'color': 'black',
                },
                {
                    'if': {'filter_query': '{conversion_rate} < 15'},
                    'backgroundColor': '#ffeaa7',
                    'color': 'black',
                }
            ]
        )
        
        # Sentiment Pie Chart
        total_positive = sum(agent['positive_calls'] for agent in agent_data)
        total_negative = sum(agent['negative_calls'] for agent in agent_data)
        total_neutral = sum(agent['neutral_calls'] for agent in agent_data)
        
        sentiment_fig = px.pie(
            values=[total_positive, total_negative, total_neutral],
            names=['Positive', 'Negative', 'Neutral'],
            title="Overall Sentiment Distribution",
            color_discrete_map={'Positive': '#27ae60', 'Negative': '#e74c3c', 'Neutral': '#95a5a6'}
        )
        
        # Daily Trend Chart (simulated)
        dates = pd.date_range(start=datetime.now() - timedelta(days=7), end=datetime.now(), freq='D')
        daily_conversions = [15, 18, 22, 20, 25, 19, 23]  # Sample data
        
        daily_trend_fig = px.line(
            x=dates,
            y=daily_conversions,
            title="Daily Conversion Trend (Last 7 Days)"
        )
        daily_trend_fig.update_layout(
            xaxis_title="Date",
            yaxis_title="Conversions",
            template="plotly_white"
        )
        
    else:
        # Empty figures if no data
        agent_performance_fig = px.bar(title="No Data Available")
        conversion_upsell_fig = px.scatter(title="No Data Available")
        agent_table = html.P("No agent data available")
        sentiment_fig = px.pie(title="No Data Available")
        daily_trend_fig = px.line(title="No Data Available")
    
    return (kpi_cards, agent_performance_fig, conversion_upsell_fig, 
            agent_table, sentiment_fig, daily_trend_fig)

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8050))
    debug = os.environ.get('DEBUG', 'True').lower() == 'true'
    
    logger.info(f"Starting Converza Dashboard on port {port}")
    # Dash >= 3.0 replaces run_server with run
    app.run(debug=debug, host='0.0.0.0', port=port)