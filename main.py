#!/usr/bin/env python3
"""
Main entry point for the Call Center Data Pipeline.

Provides CLI interface for running different pipeline operations:
- Full ETL pipeline execution
- Incremental updates
- Data generation
- Pipeline status monitoring
"""

import argparse
import sys
import logging
from typing import Optional

from etl_pipeline import ETLPipeline
from data_generator import CallDataGenerator

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def run_etl_pipeline(batch_size: Optional[int] = None) -> None:
    """Run the complete ETL pipeline."""
    try:
        print("\n🚀 Starting ETL Pipeline...\n")
        pipeline = ETLPipeline()
        stats = pipeline.run_full_pipeline(batch_size)
        
        print("\n✅ Pipeline completed successfully!")
        print("\n📊 Results:")
        print(f"  • Bronze: {stats['bronze_records_fetched']} records fetched")
        print(f"  • Silver: {stats['silver_records_processed']} records cleaned")
        print(f"  • Gold: {stats['gold_records_created']} analytics created")
        print(f"  • Duration: {stats['duration_seconds']:.2f} seconds")
        
    except Exception as e:
        print(f"\n❌ Pipeline failed: {e}")
        sys.exit(1)


def run_incremental_update() -> None:
    """Run incremental update for new data."""
    try:
        print("\n🔄 Running incremental update...\n")
        pipeline = ETLPipeline()
        stats = pipeline.run_incremental_update()
        
        print("\n✅ Incremental update completed!")
        print(f"  • New records: {stats['gold_records_created']}")
        print(f"  • Duration: {stats['duration_seconds']:.2f} seconds")
        
    except Exception as e:
        print(f"\n❌ Incremental update failed: {e}")
        sys.exit(1)


def generate_sample_data(num_calls: int = 50) -> None:
    """Generate sample call data to S3."""
    try:
        print(f"\n📝 Generating {num_calls} sample call records...\n")
        generator = CallDataGenerator()
        calls = generator.generate_batch(num_calls)
        
        print(f"\n✅ Generated {len(calls)} call records successfully!")
        
    except Exception as e:
        print(f"\n❌ Data generation failed: {e}")
        sys.exit(1)


def show_pipeline_status() -> None:
    """Display pipeline status and metrics."""
    try:
        print("\n📊 Pipeline Status\n")
        pipeline = ETLPipeline()
        status = pipeline.get_pipeline_status()
        
        # Bronze Layer status
        print("Bronze Layer (S3):")
        bronze = status['bronze_layer']
        print(f"  • Total files: {bronze['total_files']}")
        print(f"  • Total size: {bronze['total_size_mb']} MB")
        
        # Silver Layer status
        print("\nSilver Layer (PostgreSQL):")
        silver = status['silver_layer']
        print(f"  • Total records: {silver.get('total_records', 0)}")
        print(f"  • Unique agents: {silver.get('unique_agents', 0)}")
        
        # Gold Layer status
        print("\nGold Layer (Analytics):")
        gold = status['gold_layer']
        if 'conversion_metrics' in gold:
            metrics = gold['conversion_metrics']
            print(f"  • Conversion rate: {metrics['conversion_rate']}%")
            print(f"  • Total revenue: ${metrics['total_upsell_revenue']}")
        
    except Exception as e:
        print(f"\n❌ Failed to get status: {e}")
        sys.exit(1)


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description='Call Center Data Pipeline - ETL for call analytics'
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # ETL pipeline command
    etl_parser = subparsers.add_parser('etl', help='Run full ETL pipeline')
    etl_parser.add_argument(
        '--batch-size', type=int, default=50,
        help='Number of records to process (default: 50)'
    )
    
    # Incremental update command
    subparsers.add_parser('incremental', help='Run incremental update')
    
    # Generate data command
    gen_parser = subparsers.add_parser('generate', help='Generate sample data')
    gen_parser.add_argument(
        '--num-calls', type=int, default=50,
        help='Number of call records to generate (default: 50)'
    )
    
    # Status command
    subparsers.add_parser('status', help='Show pipeline status')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(0)
    
    if args.command == 'etl':
        run_etl_pipeline(args.batch_size)
    elif args.command == 'incremental':
        run_incremental_update()
    elif args.command == 'generate':
        generate_sample_data(args.num_calls)
    elif args.command == 'status':
        show_pipeline_status()


if __name__ == "__main__":
    main()
