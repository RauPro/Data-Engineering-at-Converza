"""
Silver Layer module for data cleansing and normalization.

Handles cleaning, standardizing, and storing call data
in PostgreSQL for downstream processing.
"""

import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
import re

from database import DatabaseManager

logger = logging.getLogger(__name__)


class SilverLayer:
    """Handles data cleansing and normalization (Silver Layer)."""
    
    def __init__(self, db_manager: DatabaseManager):
        """
        Initialize Silver Layer with database connection.
        
        Args:
            db_manager: Database manager instance
        """
        self.db = db_manager
        self.processed_count = 0
        self.error_count = 0
    
    def clean_text(self, text: str) -> str:
        """
        Clean and normalize text content.
        
        Args:
            text: Raw text to clean
        
        Returns:
            Cleaned text
        """
        if not text:
            return ""
        
        # Remove excessive whitespace
        text = re.sub(r'\s+', ' ', text)
        
        # Remove control characters
        text = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', text)
        
        # Normalize quotes
        text = text.replace('"', '"').replace('"', '"')
        text = text.replace(''', "'").replace(''', "'")
        
        # Trim
        text = text.strip()
        
        return text
    
    def normalize_agent_name(self, agent_name: str) -> str:
        """
        Normalize agent names for consistency.
        
        Args:
            agent_name: Raw agent name
        
        Returns:
            Normalized agent name
        """
        if not agent_name:
            return "Unknown Agent"
        
        # Clean the name
        agent_name = self.clean_text(agent_name)
        
        # Title case for consistency
        agent_name = ' '.join(word.capitalize() for word in agent_name.split())
        
        return agent_name
    
    def parse_timestamp(self, timestamp_str: str) -> Optional[datetime]:
        """
        Parse timestamp string to datetime object.
        
        Args:
            timestamp_str: Timestamp string in various formats
        
        Returns:
            Parsed datetime or None if parsing fails
        """
        if not timestamp_str:
            return None
        
        # Try common timestamp formats
        formats = [
            '%Y-%m-%dT%H:%M:%S',
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%dT%H:%M:%S.%f',
            '%Y-%m-%d %H:%M:%S.%f'
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(timestamp_str, fmt)
            except ValueError:
                continue
        
        logger.warning(f"Could not parse timestamp: {timestamp_str}")
        return None
    
    def estimate_call_duration(self, transcript: str) -> int:
        """
        Estimate call duration from transcript length.
        
        Args:
            transcript: Call transcript text
        
        Returns:
            Estimated duration in seconds
        """
        if not transcript:
            return 0
        
        # Count words (rough estimate)
        word_count = len(transcript.split())
        
        # Average speaking rate: 150 words per minute
        # Add buffer for pauses and customer responses
        minutes = (word_count / 150) * 1.3
        
        return int(minutes * 60)
    
    def clean_record(self, raw_record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Clean and normalize a single call record.
        
        Args:
            raw_record: Raw call record from Bronze Layer
        
        Returns:
            Cleaned record or None if invalid
        """
        try:
            # Extract and clean required fields
            call_id = raw_record.get('call_id', '').strip()
            if not call_id:
                logger.warning("Record missing call_id")
                return None
            
            # Normalize agent name
            agent_name = self.normalize_agent_name(
                raw_record.get('agent_name', '')
            )
            
            # Parse timestamp
            call_timestamp = self.parse_timestamp(
                raw_record.get('timestamp', '')
            )
            if not call_timestamp:
                logger.warning(f"Invalid timestamp for call {call_id}")
                return None
            
            # Clean transcript
            transcript_text = self.clean_text(
                raw_record.get('transcript_text', '')
            )
            if not transcript_text:
                logger.warning(f"Empty transcript for call {call_id}")
                return None
            
            # Calculate derived fields
            transcript_length = len(transcript_text)
            call_duration = self.estimate_call_duration(transcript_text)
            
            # Build cleaned record
            cleaned_record = {
                'call_id': call_id[:50],  # Ensure max length
                'agent_name': agent_name[:100],
                'call_timestamp': call_timestamp,
                'transcript_text': transcript_text,
                'transcript_length': transcript_length,
                'call_duration_seconds': call_duration
            }
            
            return cleaned_record
            
        except Exception as e:
            logger.error(f"Error cleaning record: {e}")
            return None
    
    def process_batch(self, records: List[Dict[str, Any]]) -> int:
        """
        Process a batch of records for Silver Layer.
        
        Args:
            records: List of raw call records
        
        Returns:
            Number of successfully processed records
        """
        cleaned_records = []
        
        for record in records:
            cleaned = self.clean_record(record)
            if cleaned:
                cleaned_records.append(cleaned)
            else:
                self.error_count += 1
        
        if cleaned_records:
            # Store in database
            inserted = self.db.execute_batch_insert(
                'silver_call_records',
                cleaned_records
            )
            
            self.processed_count += inserted
            logger.info(f"Inserted {inserted} records into Silver Layer")
            
            return inserted
        
        return 0
    
    def get_unprocessed_calls(self, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get calls that haven't been processed to Gold Layer.
        
        Args:
            limit: Maximum number of records to retrieve
        
        Returns:
            List of unprocessed call records
        """
        query = """
            SELECT s.*
            FROM silver_call_records s
            LEFT JOIN gold_call_analytics g ON s.call_id = g.call_id
            WHERE g.call_id IS NULL
            ORDER BY s.call_timestamp DESC
            LIMIT %s
        """
        
        return self.db.execute_query(query, (limit,))
    
    def get_processing_stats(self) -> Dict[str, Any]:
        """
        Get Silver Layer processing statistics.
        
        Returns:
            Dictionary with processing stats
        """
        stats_query = """
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT agent_name) as unique_agents,
                MIN(call_timestamp) as earliest_call,
                MAX(call_timestamp) as latest_call,
                AVG(transcript_length) as avg_transcript_length,
                AVG(call_duration_seconds) as avg_call_duration
            FROM silver_call_records
        """
        
        result = self.db.execute_query(stats_query)
        
        if result:
            stats = result[0]
            return {
                'total_records': stats['total_records'],
                'unique_agents': stats['unique_agents'],
                'earliest_call': stats['earliest_call'],
                'latest_call': stats['latest_call'],
                'avg_transcript_length': round(stats['avg_transcript_length'] or 0, 2),
                'avg_call_duration_seconds': round(stats['avg_call_duration'] or 0, 2),
                'processed_count': self.processed_count,
                'error_count': self.error_count
            }
        
        return {
            'total_records': 0,
            'processed_count': self.processed_count,
            'error_count': self.error_count
        }
