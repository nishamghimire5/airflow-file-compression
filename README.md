# Event-Driven File Processing with Airflow and MinIO

This project implements event-driven Airflow DAGs that process files using two methods:

1. Local filesystem monitoring with real-time file detection
2. MinIO object storage with event-driven processing

## Features

### Local File Processing

- **Real-time Monitoring**: Checks for new files every minute
- **Automatic Compression**: Compresses newly uploaded files using ZIP format
- **Email Notifications**: Sends detailed reports with file specifications
- **File Organization**: Maintains processed and compressed file directories

### MinIO File Processing (Event-Driven)

- **Object Storage Integration**: Works with MinIO S3-compatible storage
- **Event-Driven Processing**: Near real-time file detection using high-frequency polling
- **Automated File Compression**: Compresses files uploaded to MinIO
- **Bucket Organization**: Separate buckets for source, processed, and compressed files
- **Email Reports**: Detailed email notifications with file statistics

### System Design

- **Docker-Based**: Runs entirely in Docker containers
- **Error Handling**: Robust error handling and logging
- **Configurable**: Easily customizable for different environments

## Prerequisites

- Docker and Docker Compose
- SMTP server access for sending emails (optional)

## Project Components

### DAGs

- **file_workflow_dag.py**: Monitors local filesystem for new files
- **minio_event_workflow_dag.py**: Processes files uploaded to MinIO buckets
- **minio_frequent_checker_dag.py**: Checks MinIO for new files every 10 seconds

### Helper Components

- **plugins/minio_event_handler.py**: Webhook handler for MinIO events
- **setup_minio_events.py**: Creates required MinIO buckets
- **setup_minio_notifications.py**: Configures MinIO event notifications

## Getting Started

### 1. Configure Email Settings

Edit the DAG files to set your email address:

```python
EMAIL_RECIPIENT = "your_email@example.com"  # Replace with your email
```

### 2. Start the Docker Environment

```bash
# Start all services
docker-compose up -d
```

This will start:

- Airflow Webserver
- Airflow Scheduler
- PostgreSQL (for Airflow metadata)
- MinIO (S3-compatible object storage)

### 3. Access the Services

- **Airflow Web UI**: http://localhost:8080
  - Login with default credentials (username: airflow, password: airflow)
- **MinIO Console**: http://localhost:9001
  - Login with default credentials (username: minioadmin, password: minioadmin)

## Running Your DAGs in the Correct Order

The project includes two main sets of DAGs:

### File System Processing DAGs:

- **file_sensor_dag** - Detects changes in the local filesystem
- **file_workflow_dag** - Processes detected files (compresses and sends notifications)

### MinIO Processing DAGs:

- **minio_frequent_checker_dag** - Checks for new files in MinIO every 10 seconds
- **minio_event_workflow_dag** - Processes files from MinIO (compresses and sends notifications)

### Running Order

#### For Local File Processing:

1. **First Run**: `file_sensor_dag`

   - This DAG monitors your local filesystem for new files
   - It runs every 30 seconds to check for file events
   - When it detects a new file, it automatically triggers the `file_workflow_dag`

2. **Automatically Triggered**: `file_workflow_dag`
   - This DAG is triggered by the sensor DAG when files are detected
   - You should NOT need to manually run this DAG
   - It compresses files and sends email notifications

#### For MinIO Processing:

1. **First Run**: `minio_frequent_checker_dag`

   - This DAG checks MinIO every 10 seconds for new files
   - When it detects a file upload in MinIO, it triggers the processing DAG
   - This should be enabled if you want near real-time processing of MinIO uploads

2. **Automatically Triggered**: `minio_event_workflow_dag`
   - This DAG is triggered by the checker DAG when files are uploaded to MinIO
   - You should NOT need to manually run this DAG
   - It compresses the files in MinIO and sends email notifications

### How to Enable the DAGs

In the Airflow UI (http://localhost:8080), locate the DAGs list.

Enable the "sensor" DAGs first:

- Turn on the toggle switch for `file_sensor_dag` (for local file processing)
- Turn on the toggle switch for `minio_frequent_checker_dag` (for MinIO processing)

The processor DAGs (`file_workflow_dag` and `minio_event_workflow_dag`) will be triggered automatically, so they should be enabled but will only run when triggered.

## Using Local File Processing

To use the local file processing workflow:

1. Create files in the shared folder mounted to the Docker container:

   ```bash
   # Copy a file to the shared folder
   cp test_file.txt ./shared_folder/
   ```

2. The DAG will detect the new file, compress it, and send an email notification within a minute.

3. Check the processed and compressed folders to see the results.

## Using MinIO Event-Driven Processing

To use the MinIO event-driven workflow:

1. Access the MinIO console at http://localhost:9001

2. Navigate to the "source-files" bucket

3. Upload files through the MinIO web interface
4. The system will automatically:
   - Detect the new file (within 10 seconds)
   - Compress it and store it in the "compressed-files" bucket
   - Move the original to the "processed-files" bucket
   - Send an email notification with file statistics

## Testing the Setup

### To test local file processing:

1. Make sure `file_sensor_dag` is enabled
2. Copy a test file to your shared folder: `/opt/airflow/shared_folder/`
3. The sensor will detect it within 30 seconds and trigger processing

### To test MinIO processing:

1. Make sure `minio_frequent_checker_dag` is enabled
2. Upload a file to the `source-files` bucket through the MinIO console (http://localhost:9001)
3. The checker will detect it within 10 seconds and trigger processing

## How the Event-Driven System Works

1. The `minio_frequent_checker_dag` runs every 10 seconds to check for new files in the MinIO "source-files" bucket

2. When new files are detected, it triggers the main processing DAG (`minio_event_workflow_dag`)

3. The processing DAG:

   - Downloads the file from MinIO
   - Compresses it
   - Uploads the compressed version to the "compressed-files" bucket
   - Copies the original file to the "processed-files" bucket
   - Sends an email notification with file details

4. This architecture provides near real-time processing with minimal resource overhead

## Advanced: True Event-Driven Setup

For a true event-driven setup using MinIO webhooks:

1. Configure MinIO to send events to Airflow:

   ```bash
   # Install MinIO client
   mc config host add myminio http://localhost:9000 minioadmin minioadmin

   # Configure webhooks
   mc event add myminio/source-files arn:minio:sqs::1:webhook --event put --suffix .txt,.pdf,.csv,.json
   mc admin config set myminio notify_webhook:1 endpoint=http://airflow-webserver:8080/api/v1/minio-events/
   mc admin service restart myminio
   ```

2. With this setup, MinIO will directly notify Airflow when files are uploaded, eliminating any polling delay.

## Troubleshooting

1. **Permission issues**:

   ```bash
   docker exec airflow-webserver ls -la /shared_folder
   ```

2. **Email sending failures**:

   - Verify SMTP settings in the DAG files
   - For Gmail, you might need to create an App Password

3. **MinIO connectivity issues**:

   ```bash
   docker exec airflow-webserver curl -v http://minio:9000
   ```

4. **DAG not running**:

   ```bash
   docker logs airflow-scheduler
   docker logs airflow-webserver
   ```

5. **MinIO not detecting files**:
   - Check buckets in the MinIO console
   - Ensure the `minio_frequent_checker_dag` is running

Remember that the "sensor" DAGs are your entry points - they detect events and trigger the actual processing DAGs. You should see both types of DAGs in the Airflow UI, but you'll primarily enable and interact with the sensor DAGs.

## Customization

You can customize the project by:

- Changing email settings
- Modifying checking frequencies
- Adding support for additional file formats
- Implementing custom processing logic
- Adjusting bucket names and folder paths

## Advanced Use Cases

- **Multi-step Processing**: Add additional steps for file validation, transformation, or analysis
- **Machine Learning**: Integrate with ML pipelines for automated document processing
- **Data Ingestion**: Use as a data ingestion platform for ETL processes
- **File Backup System**: Implement versioning for a complete file backup solution
