"""
MinIO Event-Driven Workflow DAG: Pure event-based pipeline that processes files uploaded to MinIO,
compresses them, and sends email notifications with file specifications.
This DAG is triggered by MinIO events through a webhook, not by scheduling or polling.
"""

from datetime import datetime, timedelta
import os
import io
import zipfile
import logging
import traceback
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import json
from flask import request, Response
import tempfile

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.dates import days_ago
from airflow.utils.email import send_email
from airflow.configuration import conf
from airflow.models import DagBag, DagRun
# Import for webhook support
from airflow.api.common.experimental.trigger_dag import trigger_dag

import boto3
from botocore.client import Config

# Configuration
MINIO_ENDPOINT = 'http://minio:9000'
MINIO_ACCESS_KEY = 'minioadmin'
MINIO_SECRET_KEY = 'minioadmin'
SOURCE_BUCKET = 'source-files'      # Source bucket for new files
PROCESSED_BUCKET = 'processed-files' # Bucket for processed files
COMPRESSED_BUCKET = 'compressed-files' # Bucket for compressed files
EMAIL_RECIPIENT = "your_email@email.com"  # Your email address

# Email configuration - use Airflow's settings or override here
EMAIL_SENDER = conf.get('smtp', 'smtp_mail_from', fallback='airflow@example.com')
SMTP_HOST = conf.get('smtp', 'smtp_host', fallback='smtp.gmail.com')
SMTP_PORT = int(conf.get('smtp', 'smtp_port', fallback=587))
SMTP_USER = conf.get('smtp', 'smtp_user', fallback='your_email@gmail.com')
SMTP_PASSWORD = conf.get('smtp', 'smtp_password', fallback='your_app_password')
USE_TLS = conf.getboolean('smtp', 'smtp_starttls', fallback=True)

# Logging configuration
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': [EMAIL_RECIPIENT],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ----------------
# Main Processing DAG - triggered by MinIO events through webhook
# ----------------
dag = DAG(
    'minio_event_workflow_dag',
    default_args=default_args,
    description='Pure event-driven pipeline triggered by MinIO events',
    schedule_interval=None,  # Triggered by events, not scheduled
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=5,  # Allow multiple files to be processed simultaneously
    tags=['minio_processing', 'event_driven'],
)

# --------- MinIO Event Handler Setup ---------
# This plugin needs to be placed in the Airflow plugins directory to handle MinIO webhook events

"""
To be placed in plugins/minio_event_handler.py:

from flask import Blueprint, request, Response
from airflow.models import DagBag
from airflow.api.common.experimental.trigger_dag import trigger_dag
import json
import logging

# Create a Flask Blueprint for the webhook
minio_event_blueprint = Blueprint('minio_event', __name__, url_prefix='/api/v1/minio-events')
logger = logging.getLogger(__name__)

@minio_event_blueprint.route('/', methods=['POST'])
def handle_minio_event():
    try:
        # Parse the MinIO event from the request
        event_data = request.json
        logger.info(f"Received MinIO event: {event_data}")
        
        # Extract bucket and object key information
        records = event_data.get('Records', [])
        if not records:
            logger.warning("No records found in event data")
            return Response("No records found", status=400)
            
        # Process each record (usually just one)
        for record in records:
            event_name = record.get('eventName', '')
            
            # Only process object creation events
            if not event_name.startswith('s3:ObjectCreated:'):
                continue
                
            s3 = record.get('s3', {})
            bucket = s3.get('bucket', {}).get('name', '')
            object_key = s3.get('object', {}).get('key', '')
            size = s3.get('object', {}).get('size', 0)
            
            # Only process events from the source bucket
            if bucket != 'source-files':
                logger.info(f"Ignoring event from bucket {bucket} (not source-files)")
                continue
            
            logger.info(f"File uploaded: {bucket}/{object_key} ({size} bytes)")
            
            # Trigger the DAG with the file information
            dagbag = DagBag()
            if 'minio_event_workflow_dag' in dagbag.dags:
                logger.info(f"Triggering DAG for file: {object_key}")
                run_id = f"minio_event_{bucket}_{object_key.replace('/', '_')}_{int(time.time())}"
                trigger_dag(
                    dag_id='minio_event_workflow_dag',
                    run_id=run_id,
                    conf={'file_key': object_key, 'bucket': bucket, 'size': size}
                )
                logger.info(f"DAG triggered with run_id: {run_id}")
            else:
                logger.error(f"DAG 'minio_event_workflow_dag' not found in DAG bag")
                return Response("DAG not found", status=404)
        
        return Response("Event processed successfully", status=200)
        
    except Exception as e:
        logger.error(f"Error processing MinIO event: {str(e)}")
        logger.error(traceback.format_exc())
        return Response(f"Error: {str(e)}", status=500)

# This is the plugin class that Airflow will use to register the blueprint
class MinioEventPlugin:
    name = "minio_event_plugin"
    
    def __init__(self):
        self.flask_blueprints = [minio_event_blueprint]
"""

def get_s3_client():
    """Get MinIO S3 client."""
    return boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'  # Default region
    )

def setup_buckets_and_notifications(**kwargs):
    """
    Create required MinIO buckets if they don't exist and set up event notifications.
    This task needs to be run once to set everything up.
    """
    try:
        s3_client = get_s3_client()
        
        # List of buckets to create
        buckets = [SOURCE_BUCKET, PROCESSED_BUCKET, COMPRESSED_BUCKET]
        
        # Check and create each bucket
        existing_buckets = [bucket['Name'] for bucket in s3_client.list_buckets()['Buckets']]
        
        for bucket in buckets:
            if bucket not in existing_buckets:
                s3_client.create_bucket(Bucket=bucket)
                logger.info(f"Created bucket: {bucket}")
                
                # Add owner tag
                s3_client.put_bucket_tagging(
                    Bucket=bucket,
                    Tagging={
                        'TagSet': [
                            {
                                'Key': 'Owner',
                                'Value': EMAIL_RECIPIENT
                            },
                        ]
                    }
                )
            else:
                logger.info(f"Bucket already exists: {bucket}")
        
        # Configure MinIO event notification (needs mc CLI inside container)
        # Note: In production, you would set this up with the MinIO admin API or mc command
        logger.info("To enable MinIO event notifications, run the following commands on the MinIO server:")
        logger.info("1. Install the MinIO client (mc)")
        logger.info("2. Configure mc: mc config host add myminio http://minio:9000 minioadmin minioadmin")
        logger.info(f"3. Set up webhook: mc event add myminio/{SOURCE_BUCKET} arn:minio:sqs::1:webhook --event put --suffix .txt,.pdf,.csv,.json")
        logger.info("4. Configure the webhook endpoint in MinIO to point to: http://airflow-webserver:8080/api/v1/minio-events/")
        
        return True
    except Exception as e:
        logger.error(f"Failed to setup buckets: {str(e)}")
        logger.error(traceback.format_exc())
        raise

def get_file_from_minio(**kwargs):
    """
    Get file information from DAG run configuration triggered by MinIO event.
    """
    # Check if DAG was triggered with a specific file
    dag_run = kwargs.get('dag_run')
    if dag_run and dag_run.conf and 'file_key' in dag_run.conf:
        file_key = dag_run.conf['file_key']
        logger.info(f"Processing file from MinIO event: {file_key}")
        return file_key
    
    # Fall back method if no trigger file was provided (shouldn't happen with webhook)
    logger.warning("No specific file provided in trigger. This shouldn't happen with webhook triggers.")
    
    try:
        s3_client = get_s3_client()
        response = s3_client.list_objects_v2(Bucket=SOURCE_BUCKET)
        
        if 'Contents' not in response or not response['Contents']:
            logger.info(f"No files found in bucket {SOURCE_BUCKET}")
            return None
        
        # Find the most recent file that hasn't been processed
        try:
            processed_response = s3_client.list_objects_v2(Bucket=PROCESSED_BUCKET)
            processed_files = {obj['Key'] for obj in processed_response.get('Contents', [])}
        except:
            processed_files = set()
        
        # Find unprocessed files
        unprocessed_files = []
        for obj in response['Contents']:
            key = obj['Key']
            if key not in processed_files:
                unprocessed_files.append((key, obj['LastModified']))
        
        if not unprocessed_files:
            logger.info("No unprocessed files found.")
            return None
        
        # Sort by last modified time (newest first)
        unprocessed_files.sort(key=lambda x: x[1], reverse=True)
        newest_file = unprocessed_files[0][0]
        
        logger.info(f"Found file to process: {newest_file}")
        return newest_file
        
    except Exception as e:
        logger.error(f"Error finding new files in MinIO: {str(e)}")
        logger.error(traceback.format_exc())
        raise

def check_file_exists(**kwargs):
    """
    Check if a file was found and skip downstream tasks if not.
    """
    ti = kwargs['ti']
    file_key = ti.xcom_pull(task_ids='get_file_from_minio')
    
    if not file_key:
        logger.info("No file to process. Skipping remaining tasks.")
        return False
    return True

def compress_minio_file(**kwargs):
    """
    Compress file from MinIO and store the compressed version back in MinIO.
    """
    ti = kwargs['ti']
    file_key = ti.xcom_pull(task_ids='get_file_from_minio')
    
    if not file_key:
        logger.info("No file to compress.")
        return None
    
    try:
        s3_client = get_s3_client()
        
        # Download the file from MinIO
        response = s3_client.get_object(Bucket=SOURCE_BUCKET, Key=file_key)
        file_content = response['Body'].read()
        file_name = os.path.basename(file_key)
        
        # Compress the file in memory
        compressed_data = io.BytesIO()
        with zipfile.ZipFile(compressed_data, 'w', zipfile.ZIP_DEFLATED) as zipf:
            zipf.writestr(file_name, file_content)
        
        # Reset pointer for reading
        compressed_data.seek(0)
        
        # Upload the compressed file to MinIO compressed bucket
        compressed_key = f"{file_name}.zip"
        s3_client.put_object(
            Bucket=COMPRESSED_BUCKET,
            Key=compressed_key,
            Body=compressed_data.getvalue()
        )
        
        # Copy the original file to the processed bucket
        s3_client.copy_object(
            CopySource={'Bucket': SOURCE_BUCKET, 'Key': file_key},
            Bucket=PROCESSED_BUCKET,
            Key=file_key
        )
        
        logger.info(f"File compressed: {compressed_key}")
        logger.info(f"Original file copied to processed bucket")
        
        # Get file metadata
        source_metadata = s3_client.head_object(Bucket=SOURCE_BUCKET, Key=file_key)
        compressed_metadata = s3_client.head_object(Bucket=COMPRESSED_BUCKET, Key=compressed_key)
        
        return {
            'original': {
                'key': file_key,
                'bucket': SOURCE_BUCKET,
                'size': source_metadata['ContentLength'],
                'last_modified': source_metadata['LastModified'].strftime('%Y-%m-%d %H:%M:%S'),
            },
            'compressed': {
                'key': compressed_key,
                'bucket': COMPRESSED_BUCKET,
                'size': compressed_metadata['ContentLength'],
                'last_modified': compressed_metadata['LastModified'].strftime('%Y-%m-%d %H:%M:%S'),
            }
        }
    except Exception as e:
        logger.error(f"Error compressing file: {str(e)}")
        logger.error(traceback.format_exc())
        raise

def get_file_specs(**kwargs):
    """
    Get file specifications for both the original and compressed files.
    """
    ti = kwargs['ti']
    file_info = ti.xcom_pull(task_ids='compress_minio_file')
    
    if not file_info:
        logger.info("No file info available.")
        return None
    
    try:
        original_key = file_info['original']['key']
        compressed_key = file_info['compressed']['key']
        
        # Get file sizes
        original_size = file_info['original']['size']
        compressed_size = file_info['compressed']['size']
        
        # Calculate compression stats
        compression_ratio = round((1 - (compressed_size / original_size)) * 100, 2) if original_size > 0 else 0
        space_saved = original_size - compressed_size
        
        # Get file extension
        file_name = os.path.basename(original_key)
        original_extension = os.path.splitext(file_name)[1] if '.' in file_name else 'None'
        
        specs = {
            'original': {
                'name': file_name,
                'key': original_key,
                'bucket': file_info['original']['bucket'],
                'size': f"{original_size} bytes",
                'size_human': f"{original_size / 1024:.2f} KB" if original_size < 1024 * 1024 else f"{original_size / (1024 * 1024):.2f} MB",
                'size_raw': original_size,
                'modified': file_info['original']['last_modified'],
                'extension': original_extension
            },
            'compressed': {
                'name': compressed_key,
                'key': compressed_key,
                'bucket': file_info['compressed']['bucket'],
                'size': f"{compressed_size} bytes",
                'size_human': f"{compressed_size / 1024:.2f} KB" if compressed_size < 1024 * 1024 else f"{compressed_size / (1024 * 1024):.2f} MB",
                'size_raw': compressed_size,
                'modified': file_info['compressed']['last_modified']
            },
            'compression_ratio': f"{compression_ratio}%",
            'space_saved': f"{space_saved} bytes",
            'space_saved_human': f"{space_saved / 1024:.2f} KB" if space_saved < 1024 * 1024 else f"{space_saved / (1024 * 1024):.2f} MB",
        }
        
        logger.info(f"File specifications collected successfully")
        return specs
    except Exception as e:
        logger.error(f"Error getting file specs: {str(e)}")
        logger.error(traceback.format_exc())
        raise

def send_email_notification(**kwargs):
    """
    Send email with file specifications.
    """
    ti = kwargs['ti']
    specs = ti.xcom_pull(task_ids='get_file_specs')
    
    if not specs:
        logger.info("No specs available. Skipping email.")
        return
    
    try:
        subject = f"MinIO File Processed: {specs['original']['name']}"
        
        # Get MinIO console URL for links
        minio_console_url = MINIO_ENDPOINT.replace(':9000', ':9001')
        
        html_content = f"""
        <html>
        <body style="font-family: Arial, sans-serif; line-height: 1.6;">
            <h2 style="color: #2C3E50;">MinIO File Processing Report</h2>
            <p>The following file has been automatically processed and compressed in MinIO:</p>
            
            <div style="background-color: #f8f9fa; padding: 15px; border-radius: 5px; margin-bottom: 20px;">
                <h3 style="color: #3498DB; margin-top: 0;">Original File (MinIO)</h3>
                <ul style="list-style-type: none; padding-left: 0;">
                    <li><strong>Name:</strong> {specs['original']['name']}</li>
                    <li><strong>Size:</strong> {specs['original']['size_human']} ({specs['original']['size']})</li>
                    <li><strong>File Type:</strong> {specs['original']['extension']}</li>
                    <li><strong>Modified:</strong> {specs['original']['modified']}</li>
                    <li><strong>Bucket:</strong> {specs['original']['bucket']}</li>
                    <li><strong>Path:</strong> {specs['original']['key']}</li>
                </ul>
            </div>
            
            <div style="background-color: #f8f9fa; padding: 15px; border-radius: 5px; margin-bottom: 20px;">
                <h3 style="color: #3498DB; margin-top: 0;">Compressed File (MinIO)</h3>
                <ul style="list-style-type: none; padding-left: 0;">
                    <li><strong>Name:</strong> {specs['compressed']['name']}</li>
                    <li><strong>Size:</strong> {specs['compressed']['size_human']} ({specs['compressed']['size']})</li>
                    <li><strong>Modified:</strong> {specs['compressed']['modified']}</li>
                    <li><strong>Bucket:</strong> {specs['compressed']['bucket']}</li>
                    <li><strong>Path:</strong> {specs['compressed']['key']}</li>
                </ul>
            </div>
            
            <div style="background-color: #eaf2f8; padding: 15px; border-radius: 5px; margin-bottom: 20px;">
                <h3 style="color: #2E86C1; margin-top: 0;">Compression Statistics</h3>
                <ul style="list-style-type: none; padding-left: 0;">
                    <li><strong>Compression Ratio:</strong> {specs['compression_ratio']}</li>
                    <li><strong>Space Saved:</strong> {specs['space_saved_human']} ({specs['space_saved']} bytes)</li>
                </ul>
            </div>
            
            <p>You can access your files in the MinIO console at: <a href="{minio_console_url}">{minio_console_url}</a></p>
            
            <p style="font-size: 0.8em; color: #7F8C8D;">This is an automated email sent by Airflow MinIO Event Workflow DAG on {datetime.now().strftime('%Y-%m-%d at %H:%M:%S')}.</p>
        </body>
        </html>
        """
        
        # First try using Airflow's built-in send_email
        try:
            logger.info(f"Attempting to send email using Airflow's built-in send_email function")
            send_email(
                to=EMAIL_RECIPIENT,
                subject=subject,
                html_content=html_content,
            )
            logger.info(f"Email sent to {EMAIL_RECIPIENT} using Airflow's send_email")
            return True
        except Exception as e:
            logger.warning(f"Failed to send email with Airflow's built-in function: {str(e)}")
            logger.warning("Trying alternative email method...")
        
        # If Airflow's send_email fails, use direct SMTP
        try:
            msg = MIMEMultipart('alternative')
            msg['Subject'] = subject
            msg['From'] = EMAIL_SENDER
            msg['To'] = EMAIL_RECIPIENT
            
            part = MIMEText(html_content, 'html')
            msg.attach(part)
            
            with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
                if USE_TLS:
                    server.starttls()
                if SMTP_USER and SMTP_PASSWORD:
                    server.login(SMTP_USER, SMTP_PASSWORD)
                server.sendmail(EMAIL_SENDER, [EMAIL_RECIPIENT], msg.as_string())
                
            logger.info(f"Email sent successfully to {EMAIL_RECIPIENT} using direct SMTP")
            return True
            
        except Exception as e:
            logger.error(f"Error sending email via direct SMTP: {str(e)}")
            logger.error(traceback.format_exc())
            # We'll log but not re-raise to prevent the DAG from failing
            logger.error("Email notification failed but continuing workflow")
            return False
            
    except Exception as e:
        logger.error(f"Error preparing email: {str(e)}")
        logger.error(traceback.format_exc())
        # We'll log but not re-raise to prevent the DAG from failing
        return False

# Define the main processing DAG tasks
setup_task = PythonOperator(
    task_id='setup_buckets_and_notifications',
    python_callable=setup_buckets_and_notifications,
    provide_context=True,
    dag=dag,
)

get_file_task = PythonOperator(
    task_id='get_file_from_minio',
    python_callable=get_file_from_minio,
    provide_context=True,
    dag=dag,
)

check_file_exists_task = ShortCircuitOperator(
    task_id='check_file_exists',
    python_callable=check_file_exists,
    provide_context=True,
    dag=dag,
)

compress_task = PythonOperator(
    task_id='compress_minio_file',
    python_callable=compress_minio_file,
    provide_context=True,
    dag=dag,
)

get_specs_task = PythonOperator(
    task_id='get_file_specs',
    python_callable=get_file_specs,
    provide_context=True,
    dag=dag,
)

send_email_task = PythonOperator(
    task_id='send_email_notification',
    python_callable=send_email_notification,
    provide_context=True,
    dag=dag,
)

# Set task dependencies for main DAG
setup_task >> get_file_task >> check_file_exists_task >> compress_task >> get_specs_task >> send_email_task