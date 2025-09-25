"""
Configuration module for the Data Pipeline.

Centralizes environment variables and configuration settings
for database connections, AWS services, and pipeline parameters.
"""

import os
from typing import Dict, Any
from dataclasses import dataclass
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()


@dataclass
class DatabaseConfig:
    """Database connection configuration."""
    url: str
    schema: str = "call_data"
    pool_size: int = 5
    max_overflow: int = 10


@dataclass
class S3Config:
    """AWS S3 configuration."""
    bucket_name: str
    region: str
    access_key_id: str
    secret_access_key: str
    prefix: str = "bronze/"


@dataclass
class PipelineConfig:
    """Pipeline processing configuration."""
    batch_size: int
    log_level: str
    retry_attempts: int = 3
    retry_delay: int = 5  # seconds


class Config:
    """Main configuration class for the pipeline."""
    
    def __init__(self):
        self._validate_env_vars()
        self.database = self._get_database_config()
        self.s3 = self._get_s3_config()
        self.pipeline = self._get_pipeline_config()
        self._setup_logging()
    
    def _validate_env_vars(self) -> None:
        """Validate required environment variables exist."""
        required_vars = [
            'DATABASE_URL',
            'BUCKET_NAME',
            'AWS_ACCESS_KEY_ID',
            'AWS_SECRET_ACCESS_KEY'
        ]
        
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
    
    def _get_database_config(self) -> DatabaseConfig:
        """Extract database configuration from environment."""
        return DatabaseConfig(
            url=os.getenv('DATABASE_URL'),
            schema=os.getenv('DB_SCHEMA', 'call_data'),
            pool_size=int(os.getenv('DB_POOL_SIZE', '5')),
            max_overflow=int(os.getenv('DB_MAX_OVERFLOW', '10'))
        )
    
    def _get_s3_config(self) -> S3Config:
        """Extract S3 configuration from environment."""
        return S3Config(
            bucket_name=os.getenv('BUCKET_NAME'),
            region=os.getenv('AWS_DEFAULT_REGION', 'us-west-2'),
            access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            prefix=os.getenv('S3_PREFIX', 'bronze/')
        )
    
    def _get_pipeline_config(self) -> PipelineConfig:
        """Extract pipeline configuration from environment."""
        return PipelineConfig(
            batch_size=int(os.getenv('BATCH_SIZE', '100')),
            log_level=os.getenv('LOG_LEVEL', 'INFO'),
            retry_attempts=int(os.getenv('RETRY_ATTEMPTS', '3')),
            retry_delay=int(os.getenv('RETRY_DELAY', '5'))
        )
    
    def _setup_logging(self) -> None:
        """Configure logging for the application."""
        logging.basicConfig(
            level=getattr(logging, self.pipeline.log_level),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )


# Global config instance
config = Config()
