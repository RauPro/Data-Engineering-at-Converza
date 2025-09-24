"""
Converza Performance Pipeline DAG
Orchestrates the complete ETL process for call performance analytics
"""

from datetime import datetime, timedelta
import sys
import os

# Add src directory to Python path for imports
sys.path.append('/opt/airflow/src')

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Default arguments for the DAG
default_args = {
    'owner': 'converza-team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'converza_performance_pipeline',
    default_args=default_args,
    description='Complete ETL pipeline for Converza call performance analytics',
    schedule_interval=timedelta(hours=1),  # Run every hour
    start_date=days_ago(1),
    catchup=False,
    tags=['etl', 'converza', 'performance', 'analytics'],
)

def generate_synthetic_data(**context):
    """Task 1: Generate synthetic call data"""
    try:
        from data_generator import CallDataGenerator
        
        logger.info("Starting synthetic data generation...")
        generator = CallDataGenerator()
        
        # Generate a small batch for regular pipeline runs
        calls = generator.generate_batch(num_calls=10, output_dir="/opt/airflow/data/raw_calls")
        
        logger.info(f"Successfully generated {len(calls)} call records")
        return {"calls_generated": len(calls), "status": "success"}
        
    except Exception as e:
        logger.error(f"Error in data generation: {str(e)}")
        raise

def ingest_to_bronze(**context):
    """Task 2: Ingest raw call data to bronze layer"""
    try:
        from database_utils import DatabaseManager, FileManager
        
        logger.info("Starting bronze layer ingestion...")
        
        # Initialize managers
        db = DatabaseManager()
        file_manager = FileManager(data_dir="/opt/airflow/data")
        
        # Load call files
        calls = file_manager.load_call_files()
        
        if not calls:
            logger.warning("No call files found to process")
            return {"calls_processed": 0, "status": "no_data"}
        
        # Insert into bronze layer
        processed_count = 0
        for call in calls:
            try:
                db.insert_bronze_call(call)
                processed_count += 1
            except Exception as e:
                logger.error(f"Error inserting call {call.get('call_id', 'unknown')}: {e}")
                continue
        
        logger.info(f"Successfully processed {processed_count} calls to bronze layer")
        return {"calls_processed": processed_count, "status": "success"}
        
    except Exception as e:
        logger.error(f"Error in bronze ingestion: {str(e)}")
        raise

def transform_bronze_to_silver(**context):
    """Task 3: Transform bronze data to silver layer (cleaning)"""
    try:
        from database_utils import DatabaseManager
        from etl_processors import BatchProcessor
        
        logger.info("Starting bronze to silver transformation...")
        
        # Initialize components
        db = DatabaseManager()
        processor = BatchProcessor()
        
        # Get unprocessed bronze calls
        bronze_calls = db.get_bronze_calls(limit=100)
        
        if not bronze_calls:
            logger.warning("No bronze calls found to process")
            return {"calls_processed": 0, "status": "no_data"}
        
        # Convert to list of dicts
        bronze_data = [dict(call) for call in bronze_calls]
        
        # Process to silver layer
        silver_calls = processor.process_bronze_to_silver(bronze_data)
        
        # Insert into silver layer
        processed_count = 0
        for call in silver_calls:
            try:
                db.insert_silver_call(call)
                processed_count += 1
            except Exception as e:
                logger.error(f"Error inserting silver call {call.get('call_id', 'unknown')}: {e}")
                continue
        
        logger.info(f"Successfully processed {processed_count} calls to silver layer")
        return {"calls_processed": processed_count, "status": "success"}
        
    except Exception as e:
        logger.error(f"Error in silver transformation: {str(e)}")
        raise

def transform_silver_to_gold(**context):
    """Task 4: Transform silver data to gold layer (KPI extraction)"""
    try:
        from database_utils import DatabaseManager
        from etl_processors import BatchProcessor
        
        logger.info("Starting silver to gold transformation...")
        
        # Initialize components
        db = DatabaseManager()
        processor = BatchProcessor()
        
        # Get unprocessed silver calls
        silver_calls = db.get_silver_calls(limit=100)
        
        if not silver_calls:
            logger.warning("No silver calls found to process")
            return {"calls_processed": 0, "status": "no_data"}
        
        # Convert to list of dicts
        silver_data = [dict(call) for call in silver_calls]
        
        # Process to gold layer
        gold_metrics = processor.process_silver_to_gold(silver_data)
        
        # Insert into gold layer
        processed_count = 0
        for metrics in gold_metrics:
            try:
                db.insert_gold_metrics(metrics)
                processed_count += 1
            except Exception as e:
                logger.error(f"Error inserting gold metrics {metrics.get('call_id', 'unknown')}: {e}")
                continue
        
        logger.info(f"Successfully processed {processed_count} metrics to gold layer")
        return {"calls_processed": processed_count, "status": "success"}
        
    except Exception as e:
        logger.error(f"Error in gold transformation: {str(e)}")
        raise

def generate_performance_report(**context):
    """Task 5: Generate performance analytics report"""
    try:
        from database_utils import DatabaseManager
        import json
        
        logger.info("Generating performance report...")
        
        db = DatabaseManager()
        
        # Get overall KPIs
        dashboard_kpis = db.get_dashboard_kpis()
        
        # Get agent performance
        agent_performance = db.get_agent_performance_summary()
        agent_data = [dict(agent) for agent in agent_performance]
        
        # Create report
        report = {
            "report_generated_at": datetime.now().isoformat(),
            "overall_kpis": dashboard_kpis,
            "agent_performance": agent_data,
            "summary": {
                "total_agents": len(agent_data),
                "top_performer": agent_data[0]['agent_name'] if agent_data else "No data",
                "avg_conversion_rate": sum(agent['conversion_rate'] for agent in agent_data) / len(agent_data) if agent_data else 0
            }
        }
        
        # Save report to file
        report_path = "/opt/airflow/data/performance_report.json"
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2)
        
        logger.info(f"Performance report generated and saved to {report_path}")
        logger.info(f"Overall conversion rate: {dashboard_kpis.get('overall_conversion_rate', 0)}%")
        logger.info(f"Total upsell revenue: ${dashboard_kpis.get('total_upsell_revenue', 0)}")
        
        return {"status": "success", "report_path": report_path}
        
    except Exception as e:
        logger.error(f"Error generating performance report: {str(e)}")
        raise

def cleanup_processed_files(**context):
    """Task 6: Clean up processed files"""
    try:
        from database_utils import FileManager
        import os
        
        logger.info("Cleaning up processed files...")
        
        file_manager = FileManager(data_dir="/opt/airflow/data")
        
        # Move processed files
        raw_files = list(file_manager.raw_calls_dir.glob("*.json"))
        processed_count = 0
        
        for file_path in raw_files:
            if file_path.name != "batch_summary.json":
                try:
                    file_manager.mark_file_processed(file_path.name)
                    processed_count += 1
                except Exception as e:
                    logger.error(f"Error processing file {file_path.name}: {e}")
        
        logger.info(f"Cleaned up {processed_count} processed files")
        return {"files_processed": processed_count, "status": "success"}
        
    except Exception as e:
        logger.error(f"Error in cleanup: {str(e)}")
        raise

# Define tasks
task_generate_data = PythonOperator(
    task_id='generate_synthetic_data',
    python_callable=generate_synthetic_data,
    dag=dag,
)

task_ingest_bronze = PythonOperator(
    task_id='ingest_to_bronze',
    python_callable=ingest_to_bronze,
    dag=dag,
)

task_transform_silver = PythonOperator(
    task_id='transform_bronze_to_silver',
    python_callable=transform_bronze_to_silver,
    dag=dag,
)

task_transform_gold = PythonOperator(
    task_id='transform_silver_to_gold',
    python_callable=transform_silver_to_gold,
    dag=dag,
)

task_generate_report = PythonOperator(
    task_id='generate_performance_report',
    python_callable=generate_performance_report,
    dag=dag,
)

task_cleanup = PythonOperator(
    task_id='cleanup_processed_files',
    python_callable=cleanup_processed_files,
    dag=dag,
)

# Define task dependencies
task_generate_data >> task_ingest_bronze >> task_transform_silver >> task_transform_gold >> task_generate_report >> task_cleanup