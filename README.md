# Event-Driven File Processing with Airflow and MinIO

## Quick Start Guide

Just cloned this repo? Follow these steps to get the project running:

### Step 1: Prerequisites

Ensure these are installed on your system:

- Docker and Docker Compose
- Git (to clone the repository)

### Step 2: Clone the Repository (if you haven't already)

```bash
git clone <repository-url>
cd airflow-file-compression
```

### Step 3: Configure Email Settings

Before starting the services, update the email and password settings in the following files:

#### a) In `dags/unified_minio_processing_dag.py`:

```python
# Replace these lines with your own email and app password
EMAIL_RECIPIENT = "your_email@gmail.com"  # Replace with your email
SMTP_USER = conf.get('smtp', 'smtp_user', fallback='your_email@gmail.com')  # Replace with your email
SMTP_PASSWORD = conf.get('smtp', 'smtp_password', fallback='your_app_password')  # Replace with your app password
```

#### b) In `dags/file_workflow_dag.py`:

```python
# Replace these lines with your own email and app password
EMAIL_RECIPIENT = "your_email@gmail.com"  # Replace with your email
SMTP_USER = conf.get('smtp', 'smtp_user', fallback='your_email@gmail.com')  # Replace with your email
SMTP_PASSWORD = conf.get('smtp', 'smtp_password', fallback='your_app_password')  # Replace with your app password
```

#### c) In `docker-compose.yaml`:

```yaml
# Find these environment variables in the webserver and scheduler services and update them
- AIRFLOW__SMTP__SMTP_MAIL_FROM=your_email@gmail.com # Replace with your email
- AIRFLOW__SMTP__SMTP_USER=your_email@gmail.com # Replace with your email
- AIRFLOW__SMTP__SMTP_PASSWORD=your_app_password # Replace with your app password
```

Note: For Gmail, you'll need to create an App Password:

1. Go to your Google Account
2. Select Security
3. Under "Signing in to Google," select App Passwords
4. Generate a new app password for "Mail" application

### Step 4: Start the Docker Environment

```bash
# Create a Docker network first (helps avoid network conflicts)
docker network create airflow-compression-network

# Start all services
docker-compose up -d
```

### Step 5: Wait for Services to Initialize (about 1-2 minutes)

The first startup takes a bit longer as Docker downloads required images and initializes the databases.

### Step 6: Access the Airflow Web UI

- Open http://localhost:8080 in your browser
- Login with these credentials:
  - Username: `admin`
  - Password: `admin`

### Step 7: Access the MinIO Console

- Open http://localhost:9001 in your browser
- Login with these credentials:
  - Username: `minioadmin`
  - Password: `minioadmin`

### Step 8: Set up MinIO Buckets

The system needs three buckets: `source-files`, `processed-files`, and `compressed-files`. These will be automatically created when you start the unified_minio_processing_dag.

### Step 9: Enable the DAG

1. In the Airflow Web UI, find `unified_minio_processing_dag` in the DAG list
2. Click the toggle switch on the left to enable it
3. The DAG will now run every minute, checking for new files to process

### Step 10: Test the Workflow

1. Go to the MinIO console (http://localhost:9001)
2. Navigate to the `source-files` bucket (create it if it doesn't exist)
3. Upload a file (any text file, PDF, etc.)
4. Within a minute, the system will:
   - Detect the new file
   - Compress it and store it in `compressed-files` bucket
   - Move the original to `processed-files` bucket
   - Send an email notification (if email is configured correctly)

### Step 11: Check Results

1. In Airflow UI, check the DAG runs to see if processing completed successfully
2. In MinIO, verify that your file appears in both `compressed-files` (as a .zip) and `processed-files`

### Common Issues

- **Network Problems**: If containers can't communicate, try restarting Docker and recreating the network
- **Email Errors**: Verify SMTP settings in `unified_minio_processing_dag.py` if you're not receiving emails
- **Permissions**: Ensure Docker has proper permissions to mount volumes

### Next Steps

Once you've confirmed the basic setup is working, explore the detailed documentation below to understand how the system works and how to customize it.

---

This project implements event-driven Airflow DAGs that process files using two methods:

1. Local filesystem monitoring with real-time file detection
2. MinIO object storage with scheduled processing

## Features

### Local File Processing

- **Real-time Monitoring**: Checks for new files every 30 seconds
- **Automatic Compression**: Compresses newly uploaded files using ZIP format
- **Email Notifications**: Sends detailed reports with file specifications
- **File Organization**: Maintains processed and compressed file directories

### MinIO File Processing

- **Object Storage Integration**: Works with MinIO S3-compatible storage
- **Scheduled Processing**: Checks for new files every minute
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

- **file_workflow_dag.py**: Main DAG for processing local files (triggered by file_sensor_dag)
- **unified_minio_processing_dag.py**: Single DAG that handles all MinIO operations (checking for new files and processing them)

### Helper Components

- **plugins/minio_event_handler.py**: Optional webhook handler for MinIO events (if event-based triggering is preferred)
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

- **file_sensor_dag** - Detects changes in the local filesystem every 30 seconds
- **file_workflow_dag** - Processes detected files (compresses and sends notifications)

### MinIO Processing DAG:

- **unified_minio_processing_dag** - Comprehensive DAG that both checks for new files in MinIO every minute and processes them

### Running Order

#### For Local File Processing:

1. **First Enable**: `file_sensor_dag`

   - This DAG monitors your local filesystem for new files
   - It runs every 30 seconds to check for file events
   - When it detects a new file, it automatically triggers the `file_workflow_dag`

2. **Also Enable**: `file_workflow_dag`
   - This DAG is triggered by the sensor DAG when files are detected
   - It compresses files and sends email notifications

#### For MinIO Processing:

1. **Enable**: `unified_minio_processing_dag`
   - This DAG checks MinIO every minute for new files
   - When it finds unprocessed files, it automatically processes them
   - It compresses the files in MinIO and sends email notifications
   - All MinIO processing is handled by this single DAG

### How to Enable the DAGs

In the Airflow UI (http://localhost:8080), locate the DAGs list.

To enable local file processing:

- Turn on the toggle switch for `file_sensor_dag`
- Turn on the toggle switch for `file_workflow_dag`

To enable MinIO file processing:

- Turn on the toggle switch for `unified_minio_processing_dag`

## Using Local File Processing

To use the local file processing workflow:

1. Create files in the shared folder mounted to the Docker container:

   ```bash
   # Copy a file to the shared folder
   cp test_file.txt ./shared_folder/
   ```

2. The DAG will detect the new file, compress it, and send an email notification within a minute.

3. Check the processed and compressed folders to see the results.

## Using MinIO Scheduled Processing

To use the MinIO workflow:

1. Access the MinIO console at http://localhost:9001

2. Navigate to the "source-files" bucket

3. Upload files through the MinIO web interface

4. The system will automatically:
   - Detect the new file (within a minute)
   - Compress it and store it in the "compressed-files" bucket
   - Move the original to the "processed-files" bucket
   - Send an email notification with file statistics

## Testing the Setup

### To test local file processing:

1. Make sure `file_sensor_dag` and `file_workflow_dag` are enabled
2. Copy a test file to your shared folder: `/opt/airflow/shared_folder/`
3. The sensor will detect it within 30 seconds and trigger processing

### To test MinIO processing:

1. Make sure `unified_minio_processing_dag` is enabled
2. Upload a file to the `source-files` bucket through the MinIO console (http://localhost:9001)
3. The DAG will detect it within a minute and process it

## How the System Works

### Local File Processing:

1. The `file_sensor_dag` runs every 30 seconds to check for new files in the shared folder
2. When new files are detected, it triggers the main processing DAG (`file_workflow_dag`)
3. The processing DAG compresses files and sends notifications

### MinIO Processing:

1. The `unified_minio_processing_dag` runs every minute
2. It checks for files in the "source-files" bucket that haven't been processed yet
3. For each new file, it:
   - Downloads the file from MinIO
   - Compresses it
   - Uploads the compressed version to the "compressed-files" bucket
   - Copies the original file to the "processed-files" bucket
   - Sends an email notification with file details

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

3. Make sure the `minio_event_handler.py` plugin is properly installed in your Airflow plugins directory.

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
   - Ensure all the buckets exist: source-files, processed-files, and compressed-files
   - Check that the `unified_minio_processing_dag` is running on schedule

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
