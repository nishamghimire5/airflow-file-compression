"""
Unified MinIO File Processing DAG

This single DAG replaces the multiple overlapping DAG files by:
1. Running on a schedule to check for new files (replacing minio_frequent_checker_dag)
2. Processing files with compression and email notifications (replacing minio_event_workflow_dag)
3. Supporting both scheduled runs and event-based triggers via the MinIO webhook

Best Practices:
- Single responsibility: One DAG handles the entire file processing workflow
- Clear task dependencies
- Proper error handling
- Configurable parameters
- Comprehensive logging
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

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.dates import days_ago
from airflow.utils.email import send_email
from airflow.configuration import conf
from airflow.utils.state import State

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

# Email configuration - use Airflow's settings
EMAIL_SENDER = conf.get('smtp', 'smtp_mail_from', fallback='airflow@example.com')
SMTP_HOST = conf.get('smtp', 'smtp_host', fallback='smtp.gmail.com')
SMTP_PORT = int(conf.get('smtp', 'smtp_port', fallback=587))
SMTP_USER = conf.get('smtp', 'smtp_user', fallback='your_email@email.com')
SMTP_PASSWORD = conf.get('smtp', 'smtp_password', fallback='YOUR_APP_PASSWORD')
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
    'retry_delay': timedelta(minutes=1),
}

# Create the DAG
dag = DAG(
    'unified_minio_processing_dag',
    default_args=default_args,
    description='Unified DAG for MinIO file processing - runs on schedule',
    # Ensure it runs on a schedule, e.g., every minute
    schedule_interval=timedelta(minutes=1),
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,  # Prevent overlapping runs
    tags=['minio_processing', 'unified'],
)

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

def setup_minio_infrastructure(**kwargs):
    """Create required MinIO buckets if they don't exist"""
    try:
        s3_client = get_s3_client()
        
        # Create buckets if they don't exist
        buckets = [SOURCE_BUCKET, PROCESSED_BUCKET, COMPRESSED_BUCKET]
        existing_buckets = [bucket['Name'] for bucket in s3_client.list_buckets().get('Buckets', [])]
        
        for bucket in buckets:
            if bucket not in existing_buckets:
                s3_client.create_bucket(Bucket=bucket)
                logger.info(f"Created bucket: {bucket}")
            else:
                logger.info(f"Bucket already exists: {bucket}")
                
        return True
    except Exception as e:
        logger.error(f"Error setting up MinIO infrastructure: {str(e)}")
        logger.error(traceback.format_exc())
        raise

def get_file_to_process(**kwargs):
    """
    Get a file to process by finding an unprocessed file in MinIO.
    Checks if a file from the source bucket exists in the processed bucket.
    """
    try:
        logger.info("Looking for unprocessed files in MinIO...")
        s3_client = get_s3_client()

        # List files in source bucket
        try:
            source_response = s3_client.list_objects_v2(Bucket=SOURCE_BUCKET)
        except Exception as e:
            logger.error(f"Error listing objects in source bucket {SOURCE_BUCKET}: {str(e)}")
            return None # Indicate no file found or error

        if 'Contents' not in source_response:
            logger.info(f"No files found in source bucket {SOURCE_BUCKET}")
            return None

        # Get list of keys from source bucket, sorted by modification time (optional, but often useful)
        source_files = sorted(source_response['Contents'], key=lambda x: x['LastModified'], reverse=True)
        source_keys = {obj['Key'] for obj in source_files}

        # Get list of keys from processed bucket
        processed_keys = set()
        try:
            processed_response = s3_client.list_objects_v2(Bucket=PROCESSED_BUCKET)
            if 'Contents' in processed_response:
                processed_keys = {obj['Key'] for obj in processed_response['Contents']}
        except s3_client.exceptions.NoSuchBucket:
             logger.warning(f"Processed bucket {PROCESSED_BUCKET} does not exist yet. Assuming no files processed.")
        except Exception as e:
            logger.warning(f"Could not list processed bucket {PROCESSED_BUCKET}, assuming no files processed: {str(e)}")
            # Decide if you want to proceed or fail here. Proceeding might re-process files.

        # Find the first file in source_keys that is not in processed_keys
        for file_obj in source_files:
             key = file_obj['Key']
             # Check if the base key (without potential .zip) exists in processed
             if key not in processed_keys:
                 logger.info(f"Found unprocessed file to process: {key}")
                 # Optionally, push the key to XCom if needed by downstream tasks explicitly
                 # kwargs['ti'].xcom_push(key='file_key', value=key)
                 return key # Return the key of the first unprocessed file

        logger.info("No unprocessed files found.")
        return None # No unprocessed files found

    except Exception as e:
        logger.error(f"Error finding file to process: {str(e)}")
        logger.error(traceback.format_exc())
        return None # Indicate error

def check_file_exists(**kwargs):
    """Skip downstream tasks if no file was found to process"""
    ti = kwargs['ti']
    file_key = ti.xcom_pull(task_ids='get_file_to_process')
    
    if not file_key:
        logger.info("No file to process. Skipping remaining tasks.")
        return False
    return True

def compress_minio_file(**kwargs):
    """Compress file from MinIO and store the compressed version"""
    ti = kwargs['ti']
    file_key = ti.xcom_pull(task_ids='get_file_to_process')
    
    if not file_key:
        logger.info("No file to compress.")
        return None
    
    try:
        s3_client = get_s3_client()
        
        # Download file from source bucket
        response = s3_client.get_object(Bucket=SOURCE_BUCKET, Key=file_key)
        file_content = response['Body'].read()
        file_name = os.path.basename(file_key)
        
        # Compress file in memory
        compressed_data = io.BytesIO()
        with zipfile.ZipFile(compressed_data, 'w', zipfile.ZIP_DEFLATED) as zipf:
            zipf.writestr(file_name, file_content)
        
        # Reset pointer for reading
        compressed_data.seek(0)
        
        # Upload compressed file to compressed bucket
        compressed_key = f"{file_key}.zip"
        s3_client.put_object(
            Bucket=COMPRESSED_BUCKET,
            Key=compressed_key,
            Body=compressed_data.getvalue()
        )
        
        # Copy original file to processed bucket
        s3_client.copy_object(
            CopySource={'Bucket': SOURCE_BUCKET, 'Key': file_key},
            Bucket=PROCESSED_BUCKET,
            Key=file_key
        )
        
        logger.info(f"File compressed: {compressed_key}")
        logger.info(f"Original file copied to processed bucket: {file_key}")
        
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
    """Get file specifications for both the original and compressed files"""
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
    """Send email with file specifications"""
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
            
            <p style="font-size: 0.8em; color: #7F8C8D;">This is an automated email sent by Airflow Unified MinIO Processing DAG on {datetime.now().strftime('%Y-%m-%d at %H:%M:%S')}.</p>
        </body>
        </html>
        """
        
        # Try using Airflow's built-in send_email first
        try:
            logger.info(f"Sending email to {EMAIL_RECIPIENT} using Airflow's send_email")
            send_email(
                to=EMAIL_RECIPIENT,
                subject=subject,
                html_content=html_content,
            )
            logger.info(f"Email sent successfully using Airflow's send_email")
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

# Define DAG Tasks
setup_task = PythonOperator(
    task_id='setup_minio_infrastructure',
    python_callable=setup_minio_infrastructure,
    dag=dag,
)

get_file_task = PythonOperator(
    task_id='get_file_to_process',
    python_callable=get_file_to_process,
    dag=dag,
)

check_file_exists_task = ShortCircuitOperator(
    task_id='check_file_exists',
    python_callable=check_file_exists,
    dag=dag,
)

compress_task = PythonOperator(
    task_id='compress_minio_file',
    python_callable=compress_minio_file,
    dag=dag,
)

get_specs_task = PythonOperator(
    task_id='get_file_specs',
    python_callable=get_file_specs,
    dag=dag,
)

send_email_task = PythonOperator(
    task_id='send_email_notification',
    python_callable=send_email_notification,
    dag=dag,
)

# Set task dependencies
setup_task >> get_file_task >> check_file_exists_task >> compress_task >> get_specs_task >> send_email_task