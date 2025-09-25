"""
Database utilities for Converza Pipeline
Handles connections and operations for PostgreSQL and MinIO
"""

import os
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from sqlalchemy import create_engine, text
import pandas as pd
from datetime import datetime
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self):
        self.postgres_config = {
            'host': os.getenv('POSTGRES_DATA_HOST', 'localhost'),
            'port': os.getenv('POSTGRES_DATA_PORT', '5433'),
            'user': os.getenv('POSTGRES_DATA_USER', 'converza'),
            'password': os.getenv('POSTGRES_DATA_PASSWORD', 'converza123'),
            'database': os.getenv('POSTGRES_DATA_DB', 'converza_data')
        }
        
        # Create SQLAlchemy engine
        self.engine = create_engine(
            f"postgresql://{self.postgres_config['user']}:{self.postgres_config['password']}@"
            f"{self.postgres_config['host']}:{self.postgres_config['port']}/{self.postgres_config['database']}"
        )
    
    def get_connection(self):
        """Get a direct psycopg2 connection"""
        return psycopg2.connect(**self.postgres_config)
    
    def execute_sql(self, sql, params=None, fetch=False):
        """Execute SQL with optional parameters"""
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(sql, params)
                if fetch:
                    return cursor.fetchall()
                conn.commit()
    
    def insert_bronze_call(self, call_data):
        """Insert call data into bronze layer"""
        sql = """
        INSERT INTO bronze_calls (call_id, agent_name, timestamp, transcript_text)
        VALUES (%(call_id)s, %(agent_name)s, %(timestamp)s, %(transcript_text)s)
        ON CONFLICT (call_id) DO NOTHING
        """
        self.execute_sql(sql, call_data)
        logger.info(f"Inserted call {call_data['call_id']} into bronze layer")
    
    def insert_silver_call(self, call_data):
        """Insert cleaned call data into silver layer"""
        sql = """
        INSERT INTO silver_calls (call_id, agent_name, timestamp, cleaned_transcript)
        VALUES (%(call_id)s, %(agent_name)s, %(timestamp)s, %(cleaned_transcript)s)
        ON CONFLICT (call_id) DO NOTHING
        """
        self.execute_sql(sql, call_data)
        logger.info(f"Inserted call {call_data['call_id']} into silver layer")
    
    def insert_gold_metrics(self, metrics_data):
        """Insert performance metrics into gold layer"""
        sql = """
        INSERT INTO gold_performance_metrics 
        (call_id, agent_name, timestamp, is_conversion, upsell_amount, sentiment_score)
        VALUES (%(call_id)s, %(agent_name)s, %(timestamp)s, %(is_conversion)s, %(upsell_amount)s, %(sentiment_score)s)
        ON CONFLICT (call_id) DO NOTHING
        """
        self.execute_sql(sql, metrics_data)
        logger.info(f"Inserted metrics for call {metrics_data['call_id']} into gold layer")
    
    def get_bronze_calls(self, limit=None):
        """Get calls from bronze layer"""
        sql = "SELECT * FROM bronze_calls ORDER BY created_at DESC"
        if limit:
            sql += f" LIMIT {limit}"
        return self.execute_sql(sql, fetch=True)
    
    def get_silver_calls(self, limit=None):
        """Get calls from silver layer"""
        sql = "SELECT * FROM silver_calls ORDER BY processed_at DESC"
        if limit:
            sql += f" LIMIT {limit}"
        return self.execute_sql(sql, fetch=True)
    
    def get_gold_metrics(self, limit=None):
        """Get performance metrics from gold layer"""
        sql = "SELECT * FROM gold_performance_metrics ORDER BY processed_at DESC"
        if limit:
            sql += f" LIMIT {limit}"
        return self.execute_sql(sql, fetch=True)
    
    def get_agent_performance_summary(self):
        """Get aggregated performance metrics by agent"""
        sql = """
        SELECT 
            agent_name,
            COUNT(*) as total_calls,
            COUNT(CASE WHEN is_conversion = true THEN 1 END) as conversions,
            ROUND(
                COUNT(CASE WHEN is_conversion = true THEN 1 END) * 100.0 / COUNT(*), 2
            ) as conversion_rate,
            COALESCE(SUM(upsell_amount), 0) as total_upsell_revenue,
            COUNT(CASE WHEN sentiment_score = 'Positive' THEN 1 END) as positive_calls,
            COUNT(CASE WHEN sentiment_score = 'Negative' THEN 1 END) as negative_calls,
            COUNT(CASE WHEN sentiment_score = 'Neutral' THEN 1 END) as neutral_calls
        FROM gold_performance_metrics
        GROUP BY agent_name
        ORDER BY conversion_rate DESC, total_upsell_revenue DESC
        """
        return self.execute_sql(sql, fetch=True)
    
    def get_dashboard_kpis(self):
        """Get overall KPIs for dashboard"""
        sql = """
        SELECT 
            COUNT(*) as total_calls,
            COUNT(CASE WHEN is_conversion = true THEN 1 END) as total_conversions,
            ROUND(
                COUNT(CASE WHEN is_conversion = true THEN 1 END) * 100.0 / COUNT(*), 2
            ) as overall_conversion_rate,
            COALESCE(SUM(upsell_amount), 0) as total_upsell_revenue,
            ROUND(COALESCE(AVG(upsell_amount), 0), 2) as avg_upsell_per_call
        FROM gold_performance_metrics
        """
        result = self.execute_sql(sql, fetch=True)
        return result[0] if result else {}

class FileManager:
    def __init__(self, data_dir="data"):
        self.data_dir = Path(data_dir)
        self.raw_calls_dir = self.data_dir / "raw_calls"
        self.processed_dir = self.data_dir / "processed"
        
        # Create directories if they don't exist
        self.raw_calls_dir.mkdir(parents=True, exist_ok=True)
        self.processed_dir.mkdir(parents=True, exist_ok=True)
    
    def load_call_files(self):
        """Load all JSON call files from raw directory"""
        call_files = list(self.raw_calls_dir.glob("*.json"))
        calls = []
        
        for file_path in call_files:
            if file_path.name != "batch_summary.json":  # Skip summary files
                try:
                    with open(file_path, 'r') as f:
                        call_data = json.load(f)
                        calls.append(call_data)
                except Exception as e:
                    logger.error(f"Error loading {file_path}: {e}")
        
        logger.info(f"Loaded {len(calls)} call files")
        return calls
    
    def mark_file_processed(self, filename):
        """Move processed file to processed directory"""
        source = self.raw_calls_dir / filename
        destination = self.processed_dir / filename
        
        if source.exists():
            source.rename(destination)
            logger.info(f"Moved {filename} to processed directory")

def test_database_connection():
    """Test database connection and setup"""
    try:
        db = DatabaseManager()
        
        # Test connection
        with db.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT version();")
                version = cursor.fetchone()
                logger.info(f"Connected to PostgreSQL: {version[0]}")
        
        # Test tables exist
        tables = db.execute_sql("""
            SELECT table_name FROM information_schema.tables 
            WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
        """, fetch=True)
        
        table_names = [table['table_name'] for table in tables]
        logger.info(f"Available tables: {table_names}")
        
        return True
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return False

if __name__ == "__main__":
    # Test the database connection
    test_database_connection()