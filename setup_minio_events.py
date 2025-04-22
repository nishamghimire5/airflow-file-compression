#!/usr/bin/env python3
"""
Setup script for MinIO event-driven workflow.
This script:
1. Creates required buckets in MinIO
2. Provides instructions for setting up event notifications
"""

import argparse
import os
import sys
import boto3
from botocore.client import Config
import subprocess
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('setup-minio-events')

# MinIO Configuration
MINIO_ENDPOINT = 'http://localhost:9000'  # Change if MinIO is running elsewhere
MINIO_ACCESS_KEY = 'minioadmin'
MINIO_SECRET_KEY = 'minioadmin'
AIRFLOW_WEBSERVER = 'http://localhost:8080'  # Change to your Airflow webserver URL

# Bucket names
SOURCE_BUCKET = 'source-files'
PROCESSED_BUCKET = 'processed-files'
COMPRESSED_BUCKET = 'compressed-files'


def get_s3_client(endpoint=MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY):
    """Create and return a boto3 S3 client configured for MinIO."""
    return boto3.client(
        's3',
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'  # Default region
    )


def create_buckets(endpoint, access_key, secret_key):
    """Create the required buckets in MinIO if they don't exist."""
    try:
        s3_client = get_s3_client(endpoint, access_key, secret_key)
        
        # List of buckets to create
        buckets = [SOURCE_BUCKET, PROCESSED_BUCKET, COMPRESSED_BUCKET]
        
        # Get existing buckets
        existing_buckets = []
        try:
            response = s3_client.list_buckets()
            existing_buckets = [bucket['Name'] for bucket in response.get('Buckets', [])]
        except Exception as e:
            logger.error(f"Failed to list buckets: {str(e)}")
            logger.error("Check if MinIO is running and credentials are correct")
            return False
        
        # Create buckets if they don't exist
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
                                'Value': 'airflow-minio-events'
                            },
                        ]
                    }
                )
            else:
                logger.info(f"Bucket already exists: {bucket}")
        
        return True
    except Exception as e:
        logger.error(f"Error creating buckets: {str(e)}")
        return False


def print_mc_instructions(endpoint, access_key, secret_key, webserver):
    """Print instructions for setting up MinIO event notifications using mc."""
    webhook_url = f"{webserver}/api/v1/minio-events/"
    
    print("\n" + "="*80)
    print("MINIO EVENT NOTIFICATION SETUP INSTRUCTIONS")
    print("="*80)
    print("\nTo enable MinIO to trigger your Airflow DAG when files are uploaded, follow these steps:")
    print("\n1. Install the MinIO client (mc) if not already installed:")
    print("   - Download from: https://min.io/docs/minio/linux/reference/minio-mc.html")
    print("   - Or with Homebrew: brew install minio/stable/mc")
    print("   - Or with apt: apt-get install mc")
    print("\n2. Configure mc to connect to your MinIO server:")
    print(f"   mc config host add myminio {endpoint} {access_key} {secret_key}")
    print("\n3. Set up webhook notifications for the source bucket:")
    print(f"   mc event add myminio/{SOURCE_BUCKET} arn:minio:sqs::1:webhook --event put --suffix .txt,.pdf,.csv,.json")
    print("\n4. Configure the webhook endpoint to point to your Airflow webserver:")
    print(f"   mc admin config set myminio notify_webhook:1 endpoint={webhook_url}")
    print("   mc admin service restart myminio")
    print("\n5. Test the setup by uploading a file to MinIO:")
    print(f"   mc cp ./test_file.txt myminio/{SOURCE_BUCKET}/")
    print("\n" + "="*80)
    print("NOTE: Make sure your Airflow webserver is accessible from the MinIO server!")
    print("      You may need to adjust network settings, firewalls, or use container networking.")
    print("="*80 + "\n")


def main():
    """Main function to setup MinIO for event-driven workflows."""
    parser = argparse.ArgumentParser(description='Set up MinIO for event-driven Airflow workflows')
    parser.add_argument('--endpoint', default=MINIO_ENDPOINT, help='MinIO endpoint URL')
    parser.add_argument('--access-key', default=MINIO_ACCESS_KEY, help='MinIO access key')
    parser.add_argument('--secret-key', default=MINIO_SECRET_KEY, help='MinIO secret key')
    parser.add_argument('--airflow-webserver', default=AIRFLOW_WEBSERVER, help='Airflow webserver URL')
    
    args = parser.parse_args()
    
    endpoint = args.endpoint
    access_key = args.access_key
    secret_key = args.secret_key
    webserver = args.airflow_webserver
    
    # Create the buckets
    logger.info("Setting up MinIO buckets...")
    if create_buckets(endpoint, access_key, secret_key):
        logger.info("✓ MinIO buckets setup complete")
    else:
        logger.error("✗ Failed to set up MinIO buckets")
        return 1
    
    # Print instructions for event notifications
    print_mc_instructions(endpoint, access_key, secret_key, webserver)
    
    logger.info("Setup complete! Follow the instructions above to configure event notifications.")
    return 0


if __name__ == "__main__":
    sys.exit(main())