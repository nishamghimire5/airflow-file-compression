"""
MinIO Frequent Checker DAG: Simulates event-driven behavior by checking for new files frequently.
This DAG runs every 10 seconds to detect new files and trigger the main processing DAG.
"""

from datetime import datetime, timedelta
import time
import logging
import traceback
from airflow.models import DAG, Variable, DagRun
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
import boto3
from botocore.client import Config

# Configuration
MINIO_ENDPOINT = 'http://minio:9000'
MINIO_ACCESS_KEY = 'minioadmin'
MINIO_SECRET_KEY = 'minioadmin'
SOURCE_BUCKET = 'source-files'

# Logging configuration
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(seconds=10),
}

dag = DAG(
    'minio_frequent_checker_dag',
    default_args=default_args,
    description='Checks for new files in MinIO every 10 seconds and triggers the main processing DAG',
    schedule_interval=timedelta(seconds=10),  # Run every 10 seconds
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=['minio_processing', 'event_simulation'],
)

def get_s3_client():
    """Get MinIO S3 client."""
    return boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )

def detect_new_files(**kwargs):
    """
    Detect new files in MinIO since the last check.
    Uses Airflow variables to keep track of processed files.
    """
    try:
        s3_client = get_s3_client()
        
        # Get the list of already processed files
        try:
            processed_files = Variable.get("minio_processed_files", default_var="[]")
            processed_files = eval(processed_files)  # Convert string representation to list
            if not isinstance(processed_files, list):
                processed_files = []
        except Exception as e:
            logger.warning(f"Could not get processed files variable, initializing new one: {str(e)}")
            processed_files = []
            Variable.set("minio_processed_files", str(processed_files))
        
        # List objects in the source bucket
        try:
            response = s3_client.list_objects_v2(Bucket=SOURCE_BUCKET)
        except Exception as e:
            logger.error(f"Error listing objects in bucket {SOURCE_BUCKET}: {str(e)}")
            return None
        
        # No files in the bucket
        if 'Contents' not in response:
            return None
            
        # Find new files
        new_files = []
        for obj in response['Contents']:
            key = obj['Key']
            if key not in processed_files:
                new_files.append({
                    'key': key,
                    'size': obj['Size'],
                    'last_modified': obj['LastModified'].strftime('%Y-%m-%d %H:%M:%S')
                })
        
        if not new_files:
            return None
            
        # Update the processed files list with the new files
        for new_file in new_files:
            processed_files.append(new_file['key'])
        
        # Store the updated list
        Variable.set("minio_processed_files", str(processed_files))
        
        # Return the first new file (we'll process one at a time)
        logger.info(f"Found new file: {new_files[0]['key']}")
        return new_files[0]
        
    except Exception as e:
        logger.error(f"Error detecting new files: {str(e)}")
        logger.error(traceback.format_exc())
        return None

# Define the task to detect new files
detect_new_files_task = PythonOperator(
    task_id='detect_new_files',
    python_callable=detect_new_files,
    provide_context=True,
    dag=dag,
)

# Define the task to trigger the main processing DAG if a new file is detected
trigger_processing_dag = TriggerDagRunOperator(
    task_id='trigger_processing_dag',
    trigger_dag_id='minio_event_workflow_dag',  # The main DAG that processes files
    conf=lambda context: {
        "file_key": context['ti'].xcom_pull(task_ids='detect_new_files')['key']
        if context['ti'].xcom_pull(task_ids='detect_new_files') else None
    },
    # Removing the python_callable parameter as it's not accepted with trigger_dag_id
    trigger_rule='all_done',  # Only trigger if detect_new_files returns a value
    dag=dag,
)

# Set task dependencies
detect_new_files_task >> trigger_processing_dag