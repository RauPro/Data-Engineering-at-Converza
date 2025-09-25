"""
Main ETL Pipeline orchestrator.

Coordinates the flow of data through Bronze, Silver, and Gold layers,
managing the entire ETL process from raw data to business insights.
"""

import logging
import time
from typing import Dict, Any, Optional
from datetime import datetime

from config import config
from database import DatabaseManager
from bronze_layer import BronzeLayer
from silver_layer import SilverLayer
from gold_layer import GoldLayer

logger = logging.getLogger(__name__)


class ETLPipeline:
    """Orchestrates the complete ETL pipeline across all layers."""
    
    def __init__(self):
        """Initialize ETL pipeline with all layer components."""
        self.start_time = None
        self.stats = {
            'bronze_records_fetched': 0,
            'silver_records_processed': 0,
            'gold_records_created': 0,
            'errors': 0,
            'duration_seconds': 0
        }
        
        # Initialize components
        self._initialize_components()
    
    def _initialize_components(self) -> None:
        """Initialize all pipeline components."""
        try:
            # Database manager
            self.db_manager = DatabaseManager()
            self.db_manager.initialize_schema()
            
            # Layer handlers
            self.bronze = BronzeLayer()
            self.silver = SilverLayer(self.db_manager)
            self.gold = GoldLayer(self.db_manager)
            
            logger.info("Pipeline components initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize pipeline components: {e}")
            raise
    
    def run_full_pipeline(self, batch_size: Optional[int] = None) -> Dict[str, Any]:
        """
        Run the complete ETL pipeline.
        
        Args:
            batch_size: Number of records to process (None for config default)
        
        Returns:
            Dictionary with pipeline execution statistics
        """
        self.start_time = datetime.now()
        batch_size = batch_size or config.pipeline.batch_size
        
        logger.info(f"Starting ETL pipeline run with batch size: {batch_size}")
        
        try:
            # Step 1: Bronze Layer - Fetch raw data from S3
            raw_records = self._process_bronze_layer(batch_size)
            
            # Step 2: Silver Layer - Clean and store in PostgreSQL
            silver_count = self._process_silver_layer(raw_records)
            
            # Step 3: Gold Layer - Apply transformations and create analytics
            gold_count = self._process_gold_layer()
            
            # Calculate execution time
            self.stats['duration_seconds'] = (
                datetime.now() - self.start_time
            ).total_seconds()
            
            # Log summary
            self._log_pipeline_summary()
            
            return self.stats
            
        except Exception as e:
            logger.error(f"Pipeline execution failed: {e}")
            self.stats['errors'] += 1
            raise
        
        finally:
            # Cleanup
            self.db_manager.close()
    
    def _process_bronze_layer(self, batch_size: int) -> list:
        """
        Process Bronze Layer - fetch raw data from S3.
        
        Args:
            batch_size: Number of records to fetch
        
        Returns:
            List of raw records
        """
        logger.info("Processing Bronze Layer...")
        
        raw_records = []
        
        for record in self.bronze.fetch_batch(batch_size):
            if self.bronze.validate_record(record):
                raw_records.append(record)
                self.stats['bronze_records_fetched'] += 1
            else:
                self.stats['errors'] += 1
        
        logger.info(f"Bronze Layer: Fetched {len(raw_records)} valid records")
        
        return raw_records
    
    def _process_silver_layer(self, raw_records: list) -> int:
        """
        Process Silver Layer - clean and normalize data.
        
        Args:
            raw_records: List of raw records from Bronze Layer
        
        Returns:
            Number of records processed
        """
        logger.info("Processing Silver Layer...")
        
        if not raw_records:
            logger.warning("No records to process in Silver Layer")
            return 0
        
        # Process in batches for efficiency
        batch_size = 50
        total_processed = 0
        
        for i in range(0, len(raw_records), batch_size):
            batch = raw_records[i:i + batch_size]
            processed = self.silver.process_batch(batch)
            total_processed += processed
        
        self.stats['silver_records_processed'] = total_processed
        logger.info(f"Silver Layer: Processed {total_processed} records")
        
        return total_processed
    
    def _process_gold_layer(self) -> int:
        """
        Process Gold Layer - create business analytics.
        
        Returns:
            Number of analytics records created
        """
        logger.info("Processing Gold Layer...")
        
        # Get unprocessed records from Silver Layer
        unprocessed = self.silver.get_unprocessed_calls(limit=100)
        
        if not unprocessed:
            logger.info("No new records to process in Gold Layer")
            return 0
        
        # Apply transformations and create analytics
        gold_count = self.gold.process_call_analytics(unprocessed)
        
        self.stats['gold_records_created'] = gold_count
        logger.info(f"Gold Layer: Created {gold_count} analytics records")
        
        return gold_count
    
    def run_incremental_update(self) -> Dict[str, Any]:
        """
        Run incremental update for new data only.
        
        Returns:
            Dictionary with update statistics
        """
        logger.info("Running incremental pipeline update...")
        
        self.start_time = datetime.now()
        
        try:
            # Process only unprocessed Silver Layer records
            unprocessed = self.silver.get_unprocessed_calls(limit=50)
            
            if unprocessed:
                gold_count = self.gold.process_call_analytics(unprocessed)
                self.stats['gold_records_created'] = gold_count
                logger.info(f"Incremental update: Processed {gold_count} new records")
            else:
                logger.info("No new records to process")
            
            self.stats['duration_seconds'] = (
                datetime.now() - self.start_time
            ).total_seconds()
            
            return self.stats
            
        except Exception as e:
            logger.error(f"Incremental update failed: {e}")
            self.stats['errors'] += 1
            raise
    
    def get_pipeline_status(self) -> Dict[str, Any]:
        """
        Get comprehensive pipeline status and metrics.
        
        Returns:
            Dictionary with status information from all layers
        """
        status = {
            'pipeline_config': {
                'batch_size': config.pipeline.batch_size,
                'retry_attempts': config.pipeline.retry_attempts,
                'log_level': config.pipeline.log_level
            },
            'bronze_layer': self.bronze.get_processing_status(),
            'silver_layer': self.silver.get_processing_stats(),
            'gold_layer': self.gold.get_processing_stats(),
            'last_run_stats': self.stats
        }
        
        return status
    
    def _log_pipeline_summary(self) -> None:
        """Log pipeline execution summary."""
        logger.info("=" * 50)
        logger.info("ETL Pipeline Execution Summary")
        logger.info("=" * 50)
        logger.info(f"Bronze Layer: {self.stats['bronze_records_fetched']} records fetched")
        logger.info(f"Silver Layer: {self.stats['silver_records_processed']} records processed")
        logger.info(f"Gold Layer: {self.stats['gold_records_created']} analytics records created")
        logger.info(f"Errors: {self.stats['errors']}")
        logger.info(f"Duration: {self.stats['duration_seconds']:.2f} seconds")
        logger.info("=" * 50)


def main():
    """Main entry point for ETL pipeline execution."""
    try:
        # Initialize pipeline
        pipeline = ETLPipeline()
        
        # Run full pipeline
        stats = pipeline.run_full_pipeline(batch_size=50)
        
        # Display results
        print("\n✅ ETL Pipeline completed successfully!")
        print(f"\n📊 Pipeline Statistics:")
        print(f"  • Bronze Layer: {stats['bronze_records_fetched']} records")
        print(f"  • Silver Layer: {stats['silver_records_processed']} records")
        print(f"  • Gold Layer: {stats['gold_records_created']} records")
        print(f"  • Duration: {stats['duration_seconds']:.2f} seconds")
        
        # Get and display analytics
        status = pipeline.get_pipeline_status()
        
        print(f"\n📈 Business Metrics:")
        if 'conversion_metrics' in status['gold_layer']:
            metrics = status['gold_layer']['conversion_metrics']
            print(f"  • Conversion Rate: {metrics['conversion_rate']}%")
            print(f"  • Total Upsell Revenue: ${metrics['total_upsell_revenue']}")
        
        if 'sentiment_distribution' in status['gold_layer']:
            print(f"\n😊 Sentiment Analysis:")
            for sentiment, data in status['gold_layer']['sentiment_distribution'].items():
                print(f"  • {sentiment.capitalize()}: {data['percentage']}%")
        
    except Exception as e:
        print(f"\n❌ Pipeline failed: {e}")
        logger.error(f"Pipeline execution failed: {e}")
        raise


if __name__ == "__main__":
    main()
