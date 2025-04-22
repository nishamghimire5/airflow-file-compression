"""
MinIO Event Handler Plugin: Receives webhook notifications from MinIO and triggers DAG runs.
"""

from flask import Blueprint, request, Response
from airflow.models import DagBag
from airflow.api.common.experimental.trigger_dag import trigger_dag
import json
import logging
import time
import traceback

# Create a Flask Blueprint for the webhook
minio_event_blueprint = Blueprint('minio_event', __name__, url_prefix='/api/v1/minio-events')
logger = logging.getLogger(__name__)

@minio_event_blueprint.route('/', methods=['POST'])
def handle_minio_event():
    """
    Handle MinIO webhook events and trigger the appropriate DAG.
    """
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
            # Update DAG ID to use our unified DAG
            if 'unified_minio_processing_dag' in dagbag.dags:
                logger.info(f"Triggering unified DAG for file: {object_key}")
                run_id = f"minio_event_{bucket}_{object_key.replace('/', '_')}_{int(time.time())}"
                trigger_dag(
                    dag_id='unified_minio_processing_dag',
                    run_id=run_id,
                    conf={'file_key': object_key, 'bucket': bucket, 'size': size}
                )
                logger.info(f"Unified DAG triggered with run_id: {run_id}")
            else:
                logger.error(f"DAG 'unified_minio_processing_dag' not found in DAG bag")
                return Response("DAG not found", status=404)
        
        return Response("Event processed successfully", status=200)
        
    except Exception as e:
        logger.error(f"Error processing MinIO event: {str(e)}")
        logger.error(traceback.format_exc())
        return Response(f"Error: {str(e)}", status=500)

# This is the plugin class that Airflow will use to register the blueprint
class MinioEventPlugin:
    """
    Airflow plugin class to register the MinIO webhook blueprint.
    """
    name = "minio_event_plugin"
    
    def __init__(self):
        self.flask_blueprints = [minio_event_blueprint]