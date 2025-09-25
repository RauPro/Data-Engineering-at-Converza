"""
Bronze Layer module for raw data ingestion from S3.

Handles fetching and parsing raw call data from S3 bucket
without any transformations.
"""

import json
import logging
from typing import List, Dict, Any, Generator
import boto3
from botocore.exceptions import ClientError

from config import config

logger = logging.getLogger(__name__)


class BronzeLayer:
    """Handles raw data ingestion from S3 (Bronze Layer)."""
    
    def __init__(self):
        """Initialize S3 client for Bronze Layer operations."""
        self.s3_client = self._create_s3_client()
        self.bucket_name = config.s3.bucket_name
        self.prefix = config.s3.prefix
    
    def _create_s3_client(self):
        """Create and configure S3 client."""
        return boto3.client(
            's3',
            aws_access_key_id=config.s3.access_key_id,
            aws_secret_access_key=config.s3.secret_access_key,
            region_name=config.s3.region
        )
    
    def list_available_files(self, max_files: int = None) -> List[str]:
        """
        List available JSON files in S3 bucket.
        
        Args:
            max_files: Maximum number of files to list
        
        Returns:
            List of S3 object keys
        """
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=self.prefix
            )
            
            if 'Contents' not in response:
                logger.warning(f"No files found in {self.bucket_name}/{self.prefix}")
                return []
            
            files = [
                obj['Key'] for obj in response['Contents']
                if obj['Key'].endswith('.json')
            ]
            
            if max_files:
                files = files[:max_files]
            
            logger.info(f"Found {len(files)} files in Bronze Layer")
            return files
            
        except ClientError as e:
            logger.error(f"Error listing S3 objects: {e}")
            raise
    
    def fetch_file_content(self, key: str) -> Dict[str, Any]:
        """
        Fetch and parse a single file from S3.
        
        Args:
            key: S3 object key
        
        Returns:
            Parsed JSON content
        """
        try:
            response = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=key
            )
            
            content = response['Body'].read().decode('utf-8')
            return json.loads(content)
            
        except ClientError as e:
            logger.error(f"Error fetching file {key}: {e}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing JSON from {key}: {e}")
            raise
    
    def fetch_batch(self, batch_size: int = None) -> Generator[Dict[str, Any], None, None]:
        """
        Fetch a batch of call records from S3.
        
        Args:
            batch_size: Number of records to fetch
        
        Yields:
            Call record dictionaries
        """
        batch_size = batch_size or config.pipeline.batch_size
        files = self.list_available_files(max_files=batch_size)
        
        for file_key in files:
            try:
                call_data = self.fetch_file_content(file_key)
                
                # Add metadata
                call_data['source_file'] = file_key
                call_data['ingestion_timestamp'] = json.loads(
                    json.dumps(call_data, default=str)
                ).get('timestamp')
                
                yield call_data
                
            except Exception as e:
                logger.error(f"Error processing file {file_key}: {e}")
                continue
    
    def validate_record(self, record: Dict[str, Any]) -> bool:
        """
        Validate that a record has required fields.
        
        Args:
            record: Call record dictionary
        
        Returns:
            True if valid, False otherwise
        """
        required_fields = ['call_id', 'agent_name', 'timestamp', 'transcript_text']
        
        for field in required_fields:
            if field not in record or record[field] is None:
                logger.warning(f"Record missing required field: {field}")
                return False
        
        return True
    
    def get_processing_status(self) -> Dict[str, Any]:
        """
        Get status of Bronze Layer processing.
        
        Returns:
            Dictionary with processing statistics
        """
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=self.prefix
            )
            
            total_files = len(response.get('Contents', []))
            total_size_bytes = sum(
                obj['Size'] for obj in response.get('Contents', [])
            )
            
            return {
                'total_files': total_files,
                'total_size_mb': round(total_size_bytes / (1024 * 1024), 2),
                'bucket': self.bucket_name,
                'prefix': self.prefix
            }
            
        except ClientError as e:
            logger.error(f"Error getting processing status: {e}")
            return {
                'error': str(e),
                'total_files': 0,
                'total_size_mb': 0
            }
