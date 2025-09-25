#!/usr/bin/env python3
"""
Standalone Pipeline Runner
Runs the complete Converza pipeline without Docker dependencies
"""

import os
import sys
import json
import sqlite3
from pathlib import Path
from datetime import datetime

# Add src to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

class SQLiteManager:
    """SQLite database manager for standalone operation"""
    
    def __init__(self, db_path="data/converza_pipeline.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.init_database()
    
    def get_connection(self):
        return sqlite3.connect(self.db_path)
    
    def init_database(self):
        """Initialize database with required tables"""
        with self.get_connection() as conn:
            # Bronze layer - Raw call data
            conn.execute("""
                CREATE TABLE IF NOT EXISTS bronze_calls (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    call_id TEXT UNIQUE NOT NULL,
                    agent_name TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    transcript_text TEXT NOT NULL,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Silver layer - Cleaned data
            conn.execute("""
                CREATE TABLE IF NOT EXISTS silver_calls (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    call_id TEXT UNIQUE NOT NULL,
                    agent_name TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    cleaned_transcript TEXT NOT NULL,
                    processed_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Gold layer - Analytics ready data
            conn.execute("""
                CREATE TABLE IF NOT EXISTS gold_performance_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    call_id TEXT UNIQUE NOT NULL,
                    agent_name TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    is_conversion INTEGER NOT NULL DEFAULT 0,
                    upsell_amount REAL DEFAULT 0.00,
                    sentiment_score TEXT NOT NULL,
                    processed_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            conn.commit()
    
    def insert_bronze_call(self, call_data):
        """Insert call data into bronze layer"""
        with self.get_connection() as conn:
            conn.execute("""
                INSERT OR IGNORE INTO bronze_calls 
                (call_id, agent_name, timestamp, transcript_text)
                VALUES (?, ?, ?, ?)
            """, (
                call_data['call_id'],
                call_data['agent_name'],
                call_data['timestamp'],
                call_data['transcript_text']
            ))
            conn.commit()
    
    def insert_silver_call(self, call_data):
        """Insert cleaned call data into silver layer"""
        with self.get_connection() as conn:
            conn.execute("""
                INSERT OR IGNORE INTO silver_calls 
                (call_id, agent_name, timestamp, cleaned_transcript)
                VALUES (?, ?, ?, ?)
            """, (
                call_data['call_id'],
                call_data['agent_name'],
                call_data['timestamp'],
                call_data['cleaned_transcript']
            ))
            conn.commit()
    
    def insert_gold_metrics(self, metrics_data):
        """Insert performance metrics into gold layer"""
        with self.get_connection() as conn:
            conn.execute("""
                INSERT OR IGNORE INTO gold_performance_metrics 
                (call_id, agent_name, timestamp, is_conversion, upsell_amount, sentiment_score)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                metrics_data['call_id'],
                metrics_data['agent_name'],
                metrics_data['timestamp'],
                1 if metrics_data['is_conversion'] else 0,
                metrics_data['upsell_amount'],
                metrics_data['sentiment_score']
            ))
            conn.commit()
    
    def get_dashboard_kpis(self):
        """Get overall KPIs for dashboard"""
        with self.get_connection() as conn:
            cursor = conn.execute("""
                SELECT 
                    COUNT(*) as total_calls,
                    SUM(is_conversion) as total_conversions,
                    ROUND(SUM(is_conversion) * 100.0 / COUNT(*), 2) as overall_conversion_rate,
                    COALESCE(SUM(upsell_amount), 0) as total_upsell_revenue,
                    ROUND(COALESCE(AVG(upsell_amount), 0), 2) as avg_upsell_per_call
                FROM gold_performance_metrics
            """)
            
            row = cursor.fetchone()
            if row:
                return {
                    'total_calls': row[0],
                    'total_conversions': row[1],
                    'overall_conversion_rate': row[2] or 0,
                    'total_upsell_revenue': row[3],
                    'avg_upsell_per_call': row[4]
                }
            return {}
    
    def get_agent_performance(self):
        """Get agent performance summary"""
        with self.get_connection() as conn:
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
            
            results = []
            for row in cursor.fetchall():
                results.append({
                    'agent_name': row[0],
                    'total_calls': row[1],
                    'conversions': row[2],
                    'conversion_rate': row[3] or 0,
                    'total_upsell_revenue': row[4],
                    'positive_calls': row[5],
                    'negative_calls': row[6],
                    'neutral_calls': row[7]
                })
            return results

def run_complete_pipeline():
    """Run the complete pipeline end-to-end"""
    print("🚀 Starting Converza Pipeline (Standalone Mode)")
    print("=" * 60)
    
    # Initialize components
    db = SQLiteManager()
    
    try:
        from src.data_generator import CallDataGenerator
        from src.etl_processors import BatchProcessor
    except ImportError as e:
        print(f"❌ Import error: {e}")
        print("💡 Make sure you're running from the project root directory")
        return False
    
    # Step 1: Generate synthetic data
    print("📞 Step 1: Generating synthetic call data...")
    generator = CallDataGenerator()
    calls = generator.generate_batch(num_calls=25, output_dir="data/raw_calls")
    print(f"   ✅ Generated {len(calls)} call records")
    
    # Step 2: Load into bronze layer
    print("\n🥉 Step 2: Loading data into Bronze layer...")
    bronze_count = 0
    for call in calls:
        db.insert_bronze_call(call)
        bronze_count += 1
    print(f"   ✅ Loaded {bronze_count} records into Bronze layer")
    
    # Step 3: Process to silver layer
    print("\n🥈 Step 3: Processing to Silver layer (cleaning)...")
    processor = BatchProcessor()
    silver_calls = processor.process_bronze_to_silver(calls)
    
    silver_count = 0
    for call in silver_calls:
        db.insert_silver_call(call)
        silver_count += 1
    print(f"   ✅ Processed {silver_count} records to Silver layer")
    
    # Step 4: Process to gold layer
    print("\n🥇 Step 4: Processing to Gold layer (KPI extraction)...")
    gold_metrics = processor.process_silver_to_gold(silver_calls)
    
    gold_count = 0
    for metrics in gold_metrics:
        db.insert_gold_metrics(metrics)
        gold_count += 1
    print(f"   ✅ Extracted KPIs for {gold_count} records to Gold layer")
    
    # Step 5: Generate performance report
    print("\n📊 Step 5: Generating performance report...")
    
    # Get overall KPIs
    kpis = db.get_dashboard_kpis()
    agent_performance = db.get_agent_performance()
    
    # Create comprehensive report
    report = {
        "report_generated_at": datetime.now().isoformat(),
        "pipeline_summary": {
            "bronze_records": bronze_count,
            "silver_records": silver_count,
            "gold_records": gold_count
        },
        "overall_kpis": kpis,
        "agent_performance": agent_performance
    }
    
    # Save report
    report_path = Path("data/performance_report.json")
    with open(report_path, 'w') as f:
        json.dump(report, f, indent=2)
    
    print(f"   ✅ Report saved to {report_path}")
    
    # Display key results
    print("\n" + "=" * 60)
    print("📈 PERFORMANCE INSIGHTS DASHBOARD")
    print("=" * 60)
    
    print(f"\n🎯 OVERALL KPIs:")
    print(f"   📞 Total Calls: {kpis.get('total_calls', 0):,}")
    print(f"   ✅ Conversions: {kpis.get('total_conversions', 0):,}")
    print(f"   📊 Conversion Rate: {kpis.get('overall_conversion_rate', 0):.1f}%")
    print(f"   💰 Total Upsell Revenue: ${kpis.get('total_upsell_revenue', 0):,.2f}")
    print(f"   💵 Avg Upsell per Call: ${kpis.get('avg_upsell_per_call', 0):.2f}")
    
    print(f"\n👥 TOP PERFORMING AGENTS:")
    if agent_performance:
        for i, agent in enumerate(agent_performance[:3]):  # Top 3
            rank = "🥇" if i == 0 else "🥈" if i == 1 else "🥉"
            print(f"   {rank} {agent['agent_name']}")
            print(f"      📞 Calls: {agent['total_calls']} | "
                  f"✅ Conv Rate: {agent['conversion_rate']:.1f}% | "
                  f"💰 Revenue: ${agent['total_upsell_revenue']:.2f}")
    
    print(f"\n🎭 SENTIMENT ANALYSIS:")
    if agent_performance:
        total_positive = sum(agent['positive_calls'] for agent in agent_performance)
        total_negative = sum(agent['negative_calls'] for agent in agent_performance)
        total_neutral = sum(agent['neutral_calls'] for agent in agent_performance)
        total_sentiment = total_positive + total_negative + total_neutral
        
        if total_sentiment > 0:
            print(f"   😊 Positive: {total_positive} ({total_positive/total_sentiment*100:.1f}%)")
            print(f"   😐 Neutral: {total_neutral} ({total_neutral/total_sentiment*100:.1f}%)")
            print(f"   😞 Negative: {total_negative} ({total_negative/total_sentiment*100:.1f}%)")
    
    print("\n" + "=" * 60)
    print("✅ Pipeline completed successfully!")
    print(f"💾 Database: data/converza_pipeline.db")
    print(f"📋 Report: {report_path}")
    print(f"📁 Raw Data: data/raw_calls/")
    
    print("\n💡 Next steps:")
    print("   1. View detailed report in performance_report.json")
    print("   2. Analyze database with any SQLite browser")
    print("   3. Run the pipeline again to process more data")
    print("   4. Deploy with Docker for production use (see README.md)")
    
    return True

if __name__ == "__main__":
    success = run_complete_pipeline()
    exit(0 if success else 1)