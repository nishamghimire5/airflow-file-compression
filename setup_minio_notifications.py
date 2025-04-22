#!/usr/bin/env python3
"""
Script to configure MinIO event notifications using boto3.
This does not require the MinIO client (mc) tool.
"""

import json
import requests
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('minio-notifications')

# Configuration
MINIO_ENDPOINT = "http://minio:9000"
MINIO_API_ENDPOINT = "http://minio:9000"
MINIO_ADMIN_USER = "minioadmin"
MINIO_ADMIN_PASSWORD = "minioadmin"
SOURCE_BUCKET = "source-files"
AIRFLOW_WEBHOOK_URL = "http://webserver:8080/api/v1/minio-events/"

def setup_webhook_notification():
    """
    Set up MinIO bucket notifications using the MinIO Admin API.
    """
    try:
        # Create a session to authenticate requests
        session = requests.Session()
        session.auth = (MINIO_ADMIN_USER, MINIO_ADMIN_PASSWORD)

        # Step 1: Configure the webhook notification target in MinIO
        webhook_config = {
            "webhook": {
                "1": {
                    "enable": True,
                    "endpoint": AIRFLOW_WEBHOOK_URL,
                    "auth_token": "",
                    "queue_dir": "",
                    "queue_limit": 0,
                    "client_cert": "",
                    "client_key": ""
                }
            }
        }
        
        # Convert config to format needed for MinIO API
        config_str = json.dumps(webhook_config)
        
        # Apply the webhook configuration to MinIO
        logger.info("Configuring webhook notification target in MinIO...")
        try:
            config_url = f"{MINIO_API_ENDPOINT}/minio/admin/v3/set-config?subsys=notify_webhook"
            response = session.put(
                config_url,
                data=config_str,
                headers={"Content-Type": "application/json"}
            )
            
            if response.status_code == 200:
                logger.info("✓ Successfully configured webhook notification target")
            else:
                logger.error(f"Failed to configure webhook: {response.status_code} - {response.text}")
                return False
        except Exception as e:
            logger.error(f"Error configuring webhook: {str(e)}")
            return False

        # Step 2: Configure bucket to send events to the webhook target
        bucket_notification = {
            "EventTypes": ["s3:ObjectCreated:*"],
            "Filter": {
                "Key": {
                    "FilterRules": [
                        {
                            "Name": "suffix",
                            "Value": ".txt"
                        }
                    ]
                }
            },
            "Queue": "arn:minio:sqs::1:webhook"
        }
        
        notification_config = {"QueueConfigurations": [bucket_notification]}
        
        # Apply the notification configuration to the bucket
        logger.info(f"Setting up bucket notifications for {SOURCE_BUCKET}...")
        try:
            bucket_config_url = f"{MINIO_API_ENDPOINT}/{SOURCE_BUCKET}/?notification"
            response = session.put(
                bucket_config_url,
                data=json.dumps(notification_config),
                headers={"Content-Type": "application/json"}
            )
            
            if response.status_code == 200:
                logger.info(f"✓ Successfully configured bucket notifications for {SOURCE_BUCKET}")
            else:
                logger.error(f"Failed to configure bucket notifications: {response.status_code} - {response.text}")
                return False
        except Exception as e:
            logger.error(f"Error configuring bucket notifications: {str(e)}")
            return False

        # Step 3: Restart MinIO to apply changes (Admin API doesn't have a direct restart command)
        logger.info("Note: You may need to restart the MinIO service for changes to take effect.")
        logger.info("You can restart MinIO using: docker restart airflow-minio")
        
        return True
    except Exception as e:
        logger.error(f"Error setting up webhook notification: {str(e)}")
        return False

def main():
    """Set up MinIO event notifications."""
    logger.info("Setting up MinIO event notifications for the source bucket...")
    
    if setup_webhook_notification():
        logger.info("MinIO event notification setup initiated successfully.")
        print("\n" + "="*80)
        print("TESTING INSTRUCTIONS")
        print("="*80)
        print("\nTo test the MinIO event notifications:")
        print("\n1. Restart the MinIO server to ensure changes take effect:")
        print("   docker restart airflow-minio")
        print("\n2. Copy the test file to the source bucket:")
        print("   docker cp test_file.txt airflow-minio:/tmp/")
        print("   docker exec airflow-minio curl -X PUT -T /tmp/test_file.txt http://localhost:9000/source-files/test_file.txt -u minioadmin:minioadmin")
        print("\n3. Check the Airflow logs to see if the webhook was triggered:")
        print("   docker logs airflow-webserver | grep minio_event")
        print("\n" + "="*80)
    else:
        logger.error("Failed to set up MinIO event notifications.")
        return 1
    
    return 0

if __name__ == "__main__":
    main()