# Event-Driven File Processing with Airflow

This project implements an event-driven Airflow DAG that monitors a shared folder for new file uploads, compresses them, and sends email notifications with file specifications.

## Features

- **Real-time Monitoring**: Checks for new files every minute
- **Automatic Compression**: Compresses newly uploaded files using ZIP format
- **Email Notifications**: Sends detailed reports with file specifications
- **File Organization**: Maintains processed and compressed file directories
- **Error Handling**: Robust error handling and logging
- **Docker-Friendly**: Designed to run inside a Docker container

## Prerequisites

- Docker
- A shared folder (like Google Drive or a local folder) mounted to the Docker container
- SMTP server access for sending emails

## Getting Started

### 1. Configure Email Settings

Edit the `file_workflow_dag.py` file and set your email address:

```python
EMAIL_RECIPIENT = "your_email@example.com"  # Replace with your email
```

### 2. Set up your shared folder

Mount your shared folder (e.g., Google Drive) to the Docker container. You have two options:

#### Option A: Using Local Directory (Simple)

Mount a local directory to the Docker container:

```bash
mkdir -p ~/shared_folder
```

#### Option B: Using Google Drive (Advanced)

For Google Drive integration, you can use `google-drive-ocamlfuse`:

```bash
# Install on host system (Ubuntu example)
sudo add-apt-repository ppa:alessandro-strada/ppa
sudo apt-get update
sudo apt-get install google-drive-ocamlfuse

# Mount Google Drive to a local directory
mkdir -p ~/google-drive
google-drive-ocamlfuse ~/google-drive

# This local directory will be mounted into the Docker container
```

### 3. Build and Run with Docker

```bash
# Build the Docker image
docker build -t airflow-file-monitor .

# Run the Docker container
docker run -d \
  --name airflow-container \
  -p 8080:8080 \
  -v ~/shared_folder:/shared_folder \
  -e AIRFLOW__SMTP__SMTP_HOST=your_smtp_server \
  -e AIRFLOW__SMTP__SMTP_PORT=587 \
  -e AIRFLOW__SMTP__SMTP_USER=your_email@example.com \
  -e AIRFLOW__SMTP__SMTP_PASSWORD=your_password \
  -e AIRFLOW__SMTP__SMTP_MAIL_FROM=your_email@example.com \
  airflow-file-monitor
```

For Gmail, use:

- SMTP_HOST: smtp.gmail.com
- SMTP_PORT: 587

### 4. Access the Airflow Web UI

Open a web browser and navigate to:

```
http://localhost:8080
```

Login with default credentials (username: admin, password: admin)

## How It Works

1. The DAG checks for new files in the shared folder every minute
2. When a new file is detected, it's compressed using ZIP format
3. The original file is copied to a processed folder
4. An email is sent with specifications of both the original and compressed files
5. The workflow continues monitoring for new files

## DAG Structure

- **setup_folders**: Creates necessary directory structure
- **find_new_files**: Searches for new files in the shared folder
- **check_file_exists**: Determines if workflow should continue
- **compress_file**: Compresses the detected file
- **get_file_specs**: Gathers file specifications
- **send_email_notification**: Sends notification email

## Customization

You can customize the DAG by editing `file_workflow_dag.py`:

- Change `WATCH_FOLDER` to monitor a different directory
- Modify `schedule_interval` for different monitoring frequency
- Adjust email templates in `send_email_notification` function
- Enable/disable original file deletion after processing by uncommenting the line: `# os.remove(file_path)`

## Troubleshooting

1. **Permission issues**: Ensure the shared folder has appropriate permissions

   ```bash
   sudo chmod -R 777 ~/shared_folder
   ```

2. **Email sending failures**: Verify SMTP settings and credentials

   - For Gmail, you might need to create an App Password

3. **Docker volume mount issues**: Check Docker volume mount

   ```bash
   docker exec airflow-container ls -la /shared_folder
   ```

4. **DAG not running**: Check Airflow logs
   ```bash
   docker logs airflow-container
   ```

## Testing

To test your setup, simply copy a file to your shared folder:

```bash
# If using local directory
cp test_file.txt ~/shared_folder/

# If using Google Drive
cp test_file.txt ~/google-drive/
```

The DAG should detect the new file, compress it, and send an email notification within a minute.
