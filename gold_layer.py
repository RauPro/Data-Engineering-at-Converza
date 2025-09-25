"""
Gold Layer module for business-ready analytics.

Handles creation of enriched datasets with business metrics
including conversion rates, upsell amounts, and sentiment analysis.
"""

import logging
from typing import List, Dict, Any
from datetime import datetime, date
from decimal import Decimal

from database import DatabaseManager
from transformations import TransformationEngine

logger = logging.getLogger(__name__)


class GoldLayer:
    """Handles business transformations and analytics (Gold Layer)."""
    
    def __init__(self, db_manager: DatabaseManager):
        """
        Initialize Gold Layer with database and transformation engine.
        
        Args:
            db_manager: Database manager instance
        """
        self.db = db_manager
        self.transformer = TransformationEngine()
        self.processed_count = 0
    
    def process_call_analytics(self, silver_records: List[Dict[str, Any]]) -> int:
        """
        Process Silver Layer records to create Gold Layer analytics.
        
        Args:
            silver_records: List of cleaned call records from Silver Layer
        
        Returns:
            Number of records processed
        """
        analytics_records = []
        
        for record in silver_records:
            try:
                # Apply transformations
                enriched = self.transformer.transform_call_record({
                    'call_id': record['call_id'],
                    'agent_name': record['agent_name'],
                    'timestamp': record['call_timestamp'],
                    'transcript_text': record['transcript_text']
                })
                
                # Add Silver Layer fields
                enriched['transcript_length'] = record['transcript_length']
                enriched['call_duration_seconds'] = record['call_duration_seconds']
                
                analytics_records.append(enriched)
                
            except Exception as e:
                logger.error(f"Error transforming record {record.get('call_id')}: {e}")
                continue
        
        if analytics_records:
            # Store in Gold Layer analytics table
            inserted = self.db.execute_batch_insert(
                'gold_call_analytics',
                analytics_records
            )
            
            self.processed_count += inserted
            logger.info(f"Created {inserted} Gold Layer analytics records")
            
            # Update aggregated tables
            self._update_agent_performance()
            self._update_daily_metrics()
            
            return inserted
        
        return 0
    
    def _update_agent_performance(self) -> None:
        """Update agent performance summary table."""
        query = """
            INSERT INTO gold_agent_performance (
                agent_name,
                total_calls,
                total_conversions,
                conversion_rate,
                total_upsell_amount,
                avg_upsell_per_call,
                positive_sentiment_pct,
                negative_sentiment_pct,
                last_updated
            )
            SELECT 
                agent_name,
                COUNT(*) as total_calls,
                SUM(CASE WHEN is_conversion THEN 1 ELSE 0 END) as total_conversions,
                ROUND(100.0 * SUM(CASE WHEN is_conversion THEN 1 ELSE 0 END) / COUNT(*), 2) as conversion_rate,
                SUM(upsell_amount) as total_upsell_amount,
                ROUND(SUM(upsell_amount) / COUNT(*), 2) as avg_upsell_per_call,
                ROUND(100.0 * SUM(CASE WHEN sentiment_score = 'positive' THEN 1 ELSE 0 END) / COUNT(*), 2) as positive_sentiment_pct,
                ROUND(100.0 * SUM(CASE WHEN sentiment_score = 'negative' THEN 1 ELSE 0 END) / COUNT(*), 2) as negative_sentiment_pct,
                CURRENT_TIMESTAMP as last_updated
            FROM gold_call_analytics
            GROUP BY agent_name
            ON CONFLICT (agent_name) DO UPDATE SET
                total_calls = EXCLUDED.total_calls,
                total_conversions = EXCLUDED.total_conversions,
                conversion_rate = EXCLUDED.conversion_rate,
                total_upsell_amount = EXCLUDED.total_upsell_amount,
                avg_upsell_per_call = EXCLUDED.avg_upsell_per_call,
                positive_sentiment_pct = EXCLUDED.positive_sentiment_pct,
                negative_sentiment_pct = EXCLUDED.negative_sentiment_pct,
                last_updated = EXCLUDED.last_updated
        """
        
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(f"SET search_path TO {self.db.schema}")
                    cursor.execute(query)
                    logger.info("Updated agent performance metrics")
        except Exception as e:
            logger.error(f"Error updating agent performance: {e}")
    
    def _update_daily_metrics(self) -> None:
        """Update daily metrics summary table."""
        query = """
            INSERT INTO gold_daily_metrics (
                metric_date,
                total_calls,
                total_conversions,
                conversion_rate,
                total_upsell_amount,
                avg_sentiment_score,
                positive_calls,
                negative_calls,
                neutral_calls,
                created_at
            )
            SELECT 
                DATE(call_timestamp) as metric_date,
                COUNT(*) as total_calls,
                SUM(CASE WHEN is_conversion THEN 1 ELSE 0 END) as total_conversions,
                ROUND(100.0 * SUM(CASE WHEN is_conversion THEN 1 ELSE 0 END) / COUNT(*), 2) as conversion_rate,
                SUM(upsell_amount) as total_upsell_amount,
                ROUND(AVG(sentiment_confidence), 2) as avg_sentiment_score,
                SUM(CASE WHEN sentiment_score = 'positive' THEN 1 ELSE 0 END) as positive_calls,
                SUM(CASE WHEN sentiment_score = 'negative' THEN 1 ELSE 0 END) as negative_calls,
                SUM(CASE WHEN sentiment_score = 'neutral' THEN 1 ELSE 0 END) as neutral_calls,
                CURRENT_TIMESTAMP as created_at
            FROM gold_call_analytics
            GROUP BY DATE(call_timestamp)
            ON CONFLICT (metric_date) DO UPDATE SET
                total_calls = EXCLUDED.total_calls,
                total_conversions = EXCLUDED.total_conversions,
                conversion_rate = EXCLUDED.conversion_rate,
                total_upsell_amount = EXCLUDED.total_upsell_amount,
                avg_sentiment_score = EXCLUDED.avg_sentiment_score,
                positive_calls = EXCLUDED.positive_calls,
                negative_calls = EXCLUDED.negative_calls,
                neutral_calls = EXCLUDED.neutral_calls,
                created_at = EXCLUDED.created_at
        """
        
        try:
            with self.db.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(f"SET search_path TO {self.db.schema}")
                    cursor.execute(query)
                    logger.info("Updated daily metrics")
        except Exception as e:
            logger.error(f"Error updating daily metrics: {e}")
    
    def get_conversion_metrics(self) -> Dict[str, Any]:
        """
        Get overall conversion metrics.
        
        Returns:
            Dictionary with conversion statistics
        """
        query = """
            SELECT 
                COUNT(*) as total_calls,
                SUM(CASE WHEN is_conversion THEN 1 ELSE 0 END) as conversions,
                ROUND(100.0 * SUM(CASE WHEN is_conversion THEN 1 ELSE 0 END) / COUNT(*), 2) as conversion_rate,
                SUM(CASE WHEN upsell_amount > 0 THEN 1 ELSE 0 END) as upsells,
                SUM(upsell_amount) as total_upsell_revenue,
                ROUND(AVG(upsell_amount), 2) as avg_upsell_amount
            FROM gold_call_analytics
        """
        
        result = self.db.execute_query(query)
        
        if result:
            return result[0]
        
        return {
            'total_calls': 0,
            'conversions': 0,
            'conversion_rate': 0.0,
            'upsells': 0,
            'total_upsell_revenue': 0.0,
            'avg_upsell_amount': 0.0
        }
    
    def get_sentiment_distribution(self) -> Dict[str, Any]:
        """
        Get sentiment distribution across all calls.
        
        Returns:
            Dictionary with sentiment statistics
        """
        query = """
            SELECT 
                sentiment_score,
                COUNT(*) as count,
                ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as percentage
            FROM gold_call_analytics
            GROUP BY sentiment_score
            ORDER BY count DESC
        """
        
        results = self.db.execute_query(query)
        
        distribution = {}
        for row in results:
            distribution[row['sentiment_score']] = {
                'count': row['count'],
                'percentage': float(row['percentage'])
            }
        
        return distribution
    
    def get_top_performers(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get top performing agents by conversion rate.
        
        Args:
            limit: Number of top performers to return
        
        Returns:
            List of top performing agents
        """
        query = """
            SELECT 
                agent_name,
                total_calls,
                total_conversions,
                conversion_rate,
                total_upsell_amount,
                positive_sentiment_pct
            FROM gold_agent_performance
            WHERE total_calls >= 5  -- Minimum calls threshold
            ORDER BY conversion_rate DESC, total_upsell_amount DESC
            LIMIT %s
        """
        
        return self.db.execute_query(query, (limit,))
    
    def get_processing_stats(self) -> Dict[str, Any]:
        """
        Get Gold Layer processing statistics.
        
        Returns:
            Dictionary with processing stats
        """
        stats = {
            'processed_count': self.processed_count,
            'conversion_metrics': self.get_conversion_metrics(),
            'sentiment_distribution': self.get_sentiment_distribution(),
            'top_performers': self.get_top_performers(5)
        }
        
        return stats
