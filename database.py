"""
Database module for PostgreSQL operations.

Handles database connections, schema creation, and provides
a connection pool for efficient database access.
"""

import logging
from contextlib import contextmanager
from typing import Generator, Optional, List, Dict, Any
import psycopg2
from psycopg2 import pool, sql, extras
from psycopg2.extensions import connection as Connection

from config import config

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Manages PostgreSQL database connections and operations."""
    
    def __init__(self):
        """Initialize database manager with connection pool."""
        self.connection_pool = self._create_connection_pool()
        self.schema = config.database.schema
    
    def _create_connection_pool(self) -> pool.SimpleConnectionPool:
        """Create a connection pool for database access."""
        try:
            return pool.SimpleConnectionPool(
                1,  # Minimum connections
                config.database.pool_size + config.database.max_overflow,  # Maximum connections
                config.database.url
            )
        except Exception as e:
            logger.error(f"Failed to create connection pool: {e}")
            raise
    
    @contextmanager
    def get_connection(self) -> Generator[Connection, None, None]:
        """
        Context manager for database connections.
        
        Yields:
            A database connection from the pool.
        """
        connection = None
        try:
            connection = self.connection_pool.getconn()
            yield connection
            connection.commit()
        except Exception as e:
            if connection:
                connection.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            if connection:
                self.connection_pool.putconn(connection)
    
    def initialize_schema(self) -> None:
        """Create database schema and tables if they don't exist."""
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                # Create schema
                cursor.execute(sql.SQL(
                    "CREATE SCHEMA IF NOT EXISTS {}"
                ).format(sql.Identifier(self.schema)))
                
                # Set search path
                cursor.execute(sql.SQL(
                    "SET search_path TO {}"
                ).format(sql.Identifier(self.schema)))
                
                # Create Silver Layer tables
                self._create_silver_tables(cursor)
                
                # Create Gold Layer tables
                self._create_gold_tables(cursor)
                
                logger.info("Database schema initialized successfully")
    
    def _create_silver_tables(self, cursor) -> None:
        """Create Silver Layer tables for cleansed data."""
        # Main call records table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS silver_call_records (
                call_id VARCHAR(50) PRIMARY KEY,
                agent_name VARCHAR(100) NOT NULL,
                call_timestamp TIMESTAMP NOT NULL,
                transcript_text TEXT NOT NULL,
                transcript_length INTEGER,
                call_duration_seconds INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create indexes for performance
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_silver_call_timestamp 
            ON silver_call_records(call_timestamp)
        """)
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_silver_agent_name 
            ON silver_call_records(agent_name)
        """)
    
    def _create_gold_tables(self, cursor) -> None:
        """Create Gold Layer tables for business-ready data."""
        # Fact table for call analytics
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS gold_call_analytics (
                call_id VARCHAR(50) PRIMARY KEY,
                agent_name VARCHAR(100) NOT NULL,
                call_timestamp TIMESTAMP NOT NULL,
                is_conversion BOOLEAN DEFAULT FALSE,
                conversion_phrase VARCHAR(255),
                upsell_amount DECIMAL(10, 2) DEFAULT 0.00,
                upsell_product VARCHAR(255),
                sentiment_score VARCHAR(20),
                sentiment_confidence DECIMAL(3, 2),
                positive_keywords_count INTEGER DEFAULT 0,
                negative_keywords_count INTEGER DEFAULT 0,
                transcript_length INTEGER,
                call_duration_seconds INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (call_id) REFERENCES silver_call_records(call_id)
            )
        """)
        
        # Agent performance summary table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS gold_agent_performance (
                agent_name VARCHAR(100) PRIMARY KEY,
                total_calls INTEGER DEFAULT 0,
                total_conversions INTEGER DEFAULT 0,
                conversion_rate DECIMAL(5, 2) DEFAULT 0.00,
                total_upsell_amount DECIMAL(12, 2) DEFAULT 0.00,
                avg_upsell_per_call DECIMAL(10, 2) DEFAULT 0.00,
                positive_sentiment_pct DECIMAL(5, 2) DEFAULT 0.00,
                negative_sentiment_pct DECIMAL(5, 2) DEFAULT 0.00,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Daily metrics table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS gold_daily_metrics (
                metric_date DATE PRIMARY KEY,
                total_calls INTEGER DEFAULT 0,
                total_conversions INTEGER DEFAULT 0,
                conversion_rate DECIMAL(5, 2) DEFAULT 0.00,
                total_upsell_amount DECIMAL(12, 2) DEFAULT 0.00,
                avg_sentiment_score DECIMAL(3, 2),
                positive_calls INTEGER DEFAULT 0,
                negative_calls INTEGER DEFAULT 0,
                neutral_calls INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create indexes for Gold Layer tables
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_gold_call_timestamp 
            ON gold_call_analytics(call_timestamp)
        """)
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_gold_sentiment 
            ON gold_call_analytics(sentiment_score)
        """)
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_gold_conversion 
            ON gold_call_analytics(is_conversion)
        """)
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
        """
        Execute a SELECT query and return results.
        
        Args:
            query: SQL query to execute
            params: Optional query parameters
        
        Returns:
            List of dictionaries containing query results
        """
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=extras.RealDictCursor) as cursor:
                cursor.execute(query, params)
                return cursor.fetchall()
    
    def execute_batch_insert(self, table: str, data: List[Dict[str, Any]]) -> int:
        """
        Perform batch insert into a table.
        
        Args:
            table: Table name
            data: List of dictionaries to insert
        
        Returns:
            Number of records inserted
        """
        if not data:
            return 0
        
        columns = data[0].keys()
        
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                # Set search path
                cursor.execute(sql.SQL(
                    "SET search_path TO {}"
                ).format(sql.Identifier(self.schema)))
                
                # Build insert query
                insert_query = sql.SQL(
                    "INSERT INTO {} ({}) VALUES %s ON CONFLICT DO NOTHING"
                ).format(
                    sql.Identifier(table),
                    sql.SQL(', ').join(map(sql.Identifier, columns))
                )
                
                # Prepare values
                values = [[d[col] for col in columns] for d in data]
                
                # Execute batch insert
                extras.execute_values(cursor, insert_query, values)
                
                return cursor.rowcount
    
    def close(self) -> None:
        """Close all database connections."""
        if self.connection_pool:
            self.connection_pool.closeall()
            logger.info("Database connections closed")
