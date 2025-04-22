"""
File Workflow DAG: Event-driven pipeline that monitors a shared folder for new files,
compresses them, and sends email notifications with file specifications.
"""

from datetime import datetime, timedelta
import os
import zipfile
import shutil
import logging
import glob
from pathlib import Path
import time
import traceback
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.utils.email import send_email
from airflow.exceptions import AirflowSkipException
from airflow.configuration import conf
from airflow.sensors.filesystem import FileSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.models import DagRun, Variable
from airflow.utils.db import provide_session
from airflow.utils.state import State


# Configuration
WATCH_FOLDER = "/opt/airflow/shared_folder"  # Folder to monitor (mount this from host to container)
PROCESSED_FOLDER = "/opt/airflow/shared_folder/processed"
COMPRESSED_FOLDER = "/opt/airflow/shared_folder/compressed"
EMAIL_RECIPIENT = "your_email@gmail.com"  # Your real email address

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

# Create a file pattern variable to track when we last processed a file
try:
    Variable.set("last_processed_file_timestamp", str(int(time.time())), description="Timestamp of last processed file")
except Exception as e:
    logger.warning(f"Could not set Variable, this is expected on first run: {str(e)}")

# Define the main processing DAG - triggered by events, not scheduled
dag = DAG(
    'file_workflow_dag',
    default_args=default_args,
    description='Event-driven pipeline monitoring folder for new files',
    schedule_interval=None,  # Triggered, not scheduled (event-driven)
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=['file_processing', 'event_driven'],
)

# Define the sensor DAG that will trigger the processing DAG
sensor_dag = DAG(
    'file_sensor_dag',
    default_args=default_args,
    description='File sensor that triggers the main processing DAG when new files arrive',
    # Run the sensor every 30 seconds to check for file events
    schedule_interval='*/30 * * * * *',  # Every 30 seconds in cron format
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=['file_sensor', 'event_driven'],
)

# Create required directories
def setup_folders(**kwargs):
    """Create required directories if they don't exist."""
    try:
        os.makedirs(WATCH_FOLDER, exist_ok=True)
        os.makedirs(PROCESSED_FOLDER, exist_ok=True)
        os.makedirs(COMPRESSED_FOLDER, exist_ok=True)
        logger.info(f"Setup directories: {WATCH_FOLDER}, {PROCESSED_FOLDER}, {COMPRESSED_FOLDER}")
        
        # Test write permissions to ensure Docker volume is mounted correctly
        test_file_path = os.path.join(WATCH_FOLDER, '.airflow_test_file')
        try:
            with open(test_file_path, 'w') as f:
                f.write('test')
            os.remove(test_file_path)
            logger.info(f"Write permissions test successful on {WATCH_FOLDER}")
        except Exception as e:
            logger.error(f"Write permission test failed on {WATCH_FOLDER}: {str(e)}")
            logger.error("Ensure Docker volume is properly mounted with write permissions")
            raise
        
        return True
    except Exception as e:
        logger.error(f"Failed to setup folders: {str(e)}")
        logger.error(traceback.format_exc())
        raise

# Find new files in the watch folder
def find_new_files(**kwargs):
    """
    Get file to process, either from a trigger or by finding in the folder.
    In an event-driven setup, this will primarily get the file from the trigger.
    """
    # First check if this DAG was triggered with a specific file
    dag_run = kwargs.get('dag_run')
    if dag_run and dag_run.conf and 'file_path' in dag_run.conf:
        file_path = dag_run.conf['file_path']
        if os.path.isfile(file_path):
            logger.info(f"Processing triggered file: {file_path}")
            return file_path
    
    # Fall back to the old method if no trigger data is available
    try:
        # Check if watch folder exists
        if not os.path.exists(WATCH_FOLDER):
            logger.error(f"Watch folder {WATCH_FOLDER} does not exist!")
            raise FileNotFoundError(f"Watch folder {WATCH_FOLDER} does not exist")
            
        processed_files = set(os.path.basename(f) for f in glob.glob(f"{PROCESSED_FOLDER}/*"))
        compressed_folder_path = Path(COMPRESSED_FOLDER)
        processed_folder_path = Path(PROCESSED_FOLDER)
        
        all_files = []
        for file in Path(WATCH_FOLDER).glob('*'):
            if file.is_file() and file.name not in processed_files:
                # Skip files in processed or compressed folders
                if not (compressed_folder_path in file.parents or processed_folder_path in file.parents):
                    all_files.append(str(file))
        
        if not all_files:
            logger.info("No new files found.")
            return None
        
        # Get the most recently modified file
        newest_file = max(all_files, key=os.path.getmtime)
        logger.info(f"Found new file: {newest_file}")
        return newest_file
    except Exception as e:
        logger.error(f"Error finding new files: {str(e)}")
        logger.error(traceback.format_exc())
        raise

# Check if a file was found
def check_file_exists(**kwargs):
    """Check if a file was found and skip downstream tasks if not."""
    ti = kwargs['ti']
    file_path = ti.xcom_pull(task_ids='find_new_files')
    
    if not file_path:
        logger.info("No new files to process. Skipping remaining tasks.")
        return False
    return True

# Compress file
def compress_file(**kwargs):
    """Compress the file that was found."""
    ti = kwargs['ti']
    file_path = ti.xcom_pull(task_ids='find_new_files')
    
    if not file_path:
        logger.info("No file to compress.")
        return None
    
    try:
        file_name = os.path.basename(file_path)
        compressed_file_path = os.path.join(COMPRESSED_FOLDER, f"{file_name}.zip")
        
        # Make sure the compressed folder exists
        os.makedirs(COMPRESSED_FOLDER, exist_ok=True)
        
        with zipfile.ZipFile(compressed_file_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(file_path, arcname=file_name)
        
        logger.info(f"File compressed: {compressed_file_path}")
        
        # Make sure the processed folder exists
        os.makedirs(PROCESSED_FOLDER, exist_ok=True)
        
        # Move original file to processed folder
        processed_path = os.path.join(PROCESSED_FOLDER, file_name)
        shutil.copy2(file_path, processed_path)
        
        # Optionally remove the original file
        # Uncomment the next line if you want to remove the original file
        # os.remove(file_path)
        
        logger.info(f"Original file copied to: {processed_path}")
        
        return {
            'original_file': file_path,
            'compressed_file': compressed_file_path
        }
    except Exception as e:
        logger.error(f"Error compressing file: {str(e)}")
        logger.error(traceback.format_exc())
        raise

# Get file specifications
def get_file_specs(**kwargs):
    """Get file specifications for both the original and compressed files."""
    ti = kwargs['ti']
    file_paths = ti.xcom_pull(task_ids='compress_file')
    
    if not file_paths:
        logger.info("No file specs to get.")
        return None
    
    try:
        original_file = file_paths['original_file']
        compressed_file = file_paths['compressed_file']
        
        # Get original file specs
        original_size = os.path.getsize(original_file)
        original_modified = datetime.fromtimestamp(os.path.getmtime(original_file))
        original_name = os.path.basename(original_file)
        original_extension = os.path.splitext(original_name)[1] if '.' in original_name else 'None'
        
        # Get compressed file specs
        compressed_size = os.path.getsize(compressed_file)
        compressed_modified = datetime.fromtimestamp(os.path.getmtime(compressed_file))
        compressed_name = os.path.basename(compressed_file)
        
        compression_ratio = round((1 - (compressed_size / original_size)) * 100, 2) if original_size > 0 else 0
        
        specs = {
            'original': {
                'name': original_name,
                'path': original_file,
                'size': f"{original_size} bytes",
                'size_human': f"{original_size / 1024:.2f} KB" if original_size < 1024 * 1024 else f"{original_size / (1024 * 1024):.2f} MB",
                'size_raw': original_size,
                'modified': original_modified.strftime('%Y-%m-%d %H:%M:%S'),
                'extension': original_extension
            },
            'compressed': {
                'name': compressed_name,
                'path': compressed_file,
                'size': f"{compressed_size} bytes",
                'size_human': f"{compressed_size / 1024:.2f} KB" if compressed_size < 1024 * 1024 else f"{compressed_size / (1024 * 1024):.2f} MB",
                'size_raw': compressed_size,
                'modified': compressed_modified.strftime('%Y-%m-%d %H:%M:%S')
            },
            'compression_ratio': f"{compression_ratio}%",
            'space_saved': f"{original_size - compressed_size} bytes",
            'space_saved_human': f"{(original_size - compressed_size) / 1024:.2f} KB" if (original_size - compressed_size) < 1024 * 1024 else f"{(original_size - compressed_size) / (1024 * 1024):.2f} MB",
        }
        
        logger.info(f"File specifications collected successfully")
        return specs
    except Exception as e:
        logger.error(f"Error getting file specs: {str(e)}")
        logger.error(traceback.format_exc())
        raise

# Send email notification
def send_email_notification(**kwargs):
    """Send email with file specifications."""
    ti = kwargs['ti']
    specs = ti.xcom_pull(task_ids='get_file_specs')
    
    if not specs:
        logger.info("No specs available. Skipping email.")
        return
    
    try:
        subject = f"File Processed: {specs['original']['name']}"
        
        html_content = f"""
        <html>
        <body style="font-family: Arial, sans-serif; line-height: 1.6;">
            <h2 style="color: #2C3E50;">File Processing Report</h2>
            <p>The following file has been automatically processed and compressed:</p>
            
            <div style="background-color: #f8f9fa; padding: 15px; border-radius: 5px; margin-bottom: 20px;">
                <h3 style="color: #3498DB; margin-top: 0;">Original File</h3>
                <ul style="list-style-type: none; padding-left: 0;">
                    <li><strong>Name:</strong> {specs['original']['name']}</li>
                    <li><strong>Size:</strong> {specs['original']['size_human']} ({specs['original']['size']})</li>
                    <li><strong>File Type:</strong> {specs['original']['extension']}</li>
                    <li><strong>Modified:</strong> {specs['original']['modified']}</li>
                    <li><strong>Path:</strong> {specs['original']['path']}</li>
                </ul>
            </div>
            
            <div style="background-color: #f8f9fa; padding: 15px; border-radius: 5px; margin-bottom: 20px;">
                <h3 style="color: #3498DB; margin-top: 0;">Compressed File</h3>
                <ul style="list-style-type: none; padding-left: 0;">
                    <li><strong>Name:</strong> {specs['compressed']['name']}</li>
                    <li><strong>Size:</strong> {specs['compressed']['size_human']} ({specs['compressed']['size']})</li>
                    <li><strong>Modified:</strong> {specs['compressed']['modified']}</li>
                    <li><strong>Path:</strong> {specs['compressed']['path']}</li>
                </ul>
            </div>
            
            <div style="background-color: #eaf2f8; padding: 15px; border-radius: 5px; margin-bottom: 20px;">
                <h3 style="color: #2E86C1; margin-top: 0;">Compression Statistics</h3>
                <ul style="list-style-type: none; padding-left: 0;">
                    <li><strong>Compression Ratio:</strong> {specs['compression_ratio']}</li>
                    <li><strong>Space Saved:</strong> {specs['space_saved_human']} ({specs['space_saved']} bytes)</li>
                </ul>
            </div>
            
            <p style="font-size: 0.8em; color: #7F8C8D;">This is an automated email sent by Airflow File Workflow DAG on {datetime.now().strftime('%Y-%m-%d at %H:%M:%S')}.</p>
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

# File event detection functions
def detect_file_events(**kwargs):
    """
    Detect new files in the watch folder and return the paths of new files.
    This function will be used by the file sensor to detect events.
    """
    try:
        # Check if watch folder exists
        if not os.path.exists(WATCH_FOLDER):
            logger.error(f"Watch folder {WATCH_FOLDER} does not exist!")
            return None
            
        # Get the timestamp of the last processed file
        try:
            last_timestamp = int(float(Variable.get("last_processed_file_timestamp", default_var="0")))
        except Exception as e:
            logger.warning(f"Could not get last_processed_file_timestamp, defaulting to current time: {str(e)}")
            last_timestamp = int(time.time())
            Variable.set("last_processed_file_timestamp", str(last_timestamp))
            
        current_time = int(time.time())
        new_files = []
        
        # Skip sub-folders like processed and compressed
        compressed_folder_path = Path(COMPRESSED_FOLDER)
        processed_folder_path = Path(PROCESSED_FOLDER)
        
        # Find files newer than the last processed timestamp
        for file_path in Path(WATCH_FOLDER).glob('*'):
            if file_path.is_file():
                # Skip files in processed or compressed folders
                if not (compressed_folder_path in file_path.parents or processed_folder_path in file_path.parents):
                    file_mtime = int(os.path.getmtime(file_path))
                    # Detect files that appeared after the last check
                    if file_mtime > last_timestamp:
                        new_files.append(str(file_path))
        
        # Update the last processed timestamp
        Variable.set("last_processed_file_timestamp", str(current_time))
        
        if new_files:
            logger.info(f"Found {len(new_files)} new files: {new_files}")
            return new_files
        else:
            logger.info("No new files detected.")
            return None
            
    except Exception as e:
        logger.error(f"Error detecting file events: {str(e)}")
        logger.error(traceback.format_exc())
        return None

def trigger_dag_for_file_event(**kwargs):
    """
    Triggers the main processing DAG when a file event is detected.
    This ensures each file is processed in its own DAG run.
    """
    ti = kwargs['ti']
    new_files = ti.xcom_pull(task_ids='detect_file_events')
    
    if not new_files:
        logger.info("No new files to process. Skipping triggers.")
        return
    
    for file_path in new_files:
        file_name = os.path.basename(file_path)
        try:
            # Trigger the main DAG with the file path as a configuration
            logger.info(f"Triggering DAG run for file: {file_name}")
            return {
                "target_dag_id": "file_workflow_dag",
                "conf": {"file_path": file_path}
            }
        except Exception as e:
            logger.error(f"Error triggering DAG for file {file_name}: {str(e)}")
            logger.error(traceback.format_exc())
    
    return None

def get_file_from_trigger(**kwargs):
    """
    Gets the file path from the DAG run configuration.
    This allows the main DAG to process the specific file that triggered it.
    """
    # Get the DAG run configuration
    dag_run = kwargs.get('dag_run')
    if dag_run and dag_run.conf and 'file_path' in dag_run.conf:
        file_path = dag_run.conf['file_path']
        logger.info(f"Processing file from trigger: {file_path}")
        return file_path
    
    # Fall back to finding a new file if no specific file was provided
    logger.info("No specific file provided in trigger. Falling back to finding any new file.")
    return find_new_files(**kwargs)

# Define tasks
setup_task = PythonOperator(
    task_id='setup_folders',
    python_callable=setup_folders,
    provide_context=True,
    dag=dag,
)

find_files_task = PythonOperator(
    task_id='find_new_files',
    python_callable=find_new_files,
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
    task_id='compress_file',
    python_callable=compress_file,
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

# Set task dependencies
setup_task >> find_files_task >> check_file_exists_task >> compress_task >> get_specs_task >> send_email_task

# Define a function to create the configuration for the TriggerDagRunOperator
def create_workflow_conf(**kwargs):
    """Create a JSON serializable configuration dictionary for the TriggerDagRunOperator."""
    ti = kwargs['ti']
    new_files = ti.xcom_pull(task_ids='detect_file_events')
    
    if new_files and len(new_files) > 0:
        return {"file_path": new_files[0]}
    else:
        return {"file_path": ""}

# Define sensor DAG tasks - these detect new files and trigger the processing DAG
file_event_detection = PythonOperator(
    task_id='detect_file_events',
    python_callable=detect_file_events,
    provide_context=True,
    dag=sensor_dag,
)

# Fix the trigger mechanism to ensure it runs only when new files are found
check_files_branch = ShortCircuitOperator(
    task_id='check_files_branch',
    python_callable=lambda ti: ti.xcom_pull(task_ids='detect_file_events') is not None,
    provide_context=True,
    dag=sensor_dag,
)

trigger_processing_dag = TriggerDagRunOperator(
    task_id='trigger_processing_dag',
    trigger_dag_id='file_workflow_dag',
    conf=create_workflow_conf,  # Using a function instead of lambda
    wait_for_completion=False,
    dag=sensor_dag,
)

# Set sensor DAG task dependencies with the branch operator
file_event_detection >> check_files_branch >> trigger_processing_dag