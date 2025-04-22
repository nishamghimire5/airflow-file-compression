from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import os
import boto3
from botocore.client import Config

# Minio credentials and configuration
MINIO_ENDPOINT = 'http://minio:9000'
MINIO_ACCESS_KEY = 'minioadmin'
MINIO_SECRET_KEY = 'minioadmin'
BUCKET_NAME = 'airflow-data'
EMAIL = 'your_email@email.com'  # Using your email for tagging

# Functions for our tasks
def create_bucket_if_not_exists(**kwargs):
    """Create an S3 bucket in Minio if it doesn't exist"""
    # Initialize S3 client with Minio settings
    s3_client = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'  # Default region
    )
    
    # Check if the bucket exists
    try:
        s3_client.head_bucket(Bucket=BUCKET_NAME)
        print(f"Bucket {BUCKET_NAME} already exists")
    except:
        # Create the bucket
        s3_client.create_bucket(Bucket=BUCKET_NAME)
        print(f"Bucket {BUCKET_NAME} created successfully")
        
        # Add a tag with your email
        s3_client.put_bucket_tagging(
            Bucket=BUCKET_NAME,
            Tagging={
                'TagSet': [
                    {
                        'Key': 'Owner',
                        'Value': EMAIL
                    },
                ]
            }
        )
        print(f"Added owner tag with email: {EMAIL}")
    
    return True

def generate_sample_file(**kwargs):
    """Generate a sample file with timestamp"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"sample_file_{timestamp}.txt"
    file_path = f"/tmp/{filename}"
    
    # Create a sample file
    with open(file_path, 'w') as f:
        f.write(f"This is a sample file created at {timestamp}\n")
        f.write(f"Created by Airflow for {EMAIL}\n")
        f.write("This file demonstrates Minio integration with Airflow\n")
    
    print(f"Generated sample file: {file_path}")
    
    # Pass the file information to the next task
    kwargs['ti'].xcom_push(key='filename', value=filename)
    kwargs['ti'].xcom_push(key='file_path', value=file_path)
    
    return filename

def upload_file_to_minio(**kwargs):
    """Upload the generated file to Minio"""
    # Get file information from previous task
    ti = kwargs['ti']
    filename = ti.xcom_pull(task_ids='generate_sample_file', key='filename')
    file_path = ti.xcom_pull(task_ids='generate_sample_file', key='file_path')
    
    # Initialize S3 client with Minio settings
    s3_client = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'  # Default region
    )
    
    # Upload the file
    s3_client.upload_file(
        Filename=file_path,
        Bucket=BUCKET_NAME,
        Key=filename,
        ExtraArgs={'Metadata': {'owner': EMAIL}}
    )
    
    print(f"Uploaded {filename} to bucket {BUCKET_NAME}")
    return filename

def list_files_in_bucket(**kwargs):
    """List all files in the bucket"""
    # Initialize S3 client with Minio settings
    s3_client = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'  # Default region
    )
    
    # List objects in the bucket
    response = s3_client.list_objects_v2(Bucket=BUCKET_NAME)
    
    if 'Contents' in response:
        print(f"Files in bucket {BUCKET_NAME}:")
        for obj in response['Contents']:
            print(f" - {obj['Key']} (Size: {obj['Size']} bytes, Last modified: {obj['LastModified']})")
    else:
        print(f"No files found in bucket {BUCKET_NAME}")
    
    return True

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': [EMAIL],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'minio_workflow',
    default_args=default_args,
    description='A simple DAG to demonstrate Minio integration',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 4, 22),
    catchup=False,
    tags=['minio', 'storage'],
) as dag:
    
    # Task 1: Create bucket if it doesn't exist
    create_bucket = PythonOperator(
        task_id='create_bucket_if_not_exists',
        python_callable=create_bucket_if_not_exists,
        provide_context=True,
    )
    
    # Task 2: Generate a sample file
    generate_file = PythonOperator(
        task_id='generate_sample_file',
        python_callable=generate_sample_file,
        provide_context=True,
    )
    
    # Task 3: Upload file to Minio
    upload_file = PythonOperator(
        task_id='upload_file_to_minio',
        python_callable=upload_file_to_minio,
        provide_context=True,
    )
    
    # Task 4: List files in bucket
    list_files = PythonOperator(
        task_id='list_files_in_bucket',
        python_callable=list_files_in_bucket,
        provide_context=True,
    )
    
    # Define the order of tasks
    create_bucket >> generate_file >> upload_file >> list_files