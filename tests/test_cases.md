# Test Cases: Event-Driven File Processing with Airflow and MinIO

## 1. Local File Processing Tests

### TC-L01: Basic File Detection

**Objective:** Verify that the `file_sensor_dag` detects new files in the shared folder.  
**Preconditions:** Airflow and DAGs are running; shared folder exists.  
**Test Steps:**

1. Enable `file_sensor_dag` and `file_workflow_dag` in Airflow UI.
2. Copy a sample text file to the shared folder.
3. Wait for up to 30 seconds (sensor interval).
4. Check Airflow UI for DAG run triggers.
   **Expected Result:** `file_sensor_dag` detects the new file and triggers `file_workflow_dag`.

### TC-L02: File Compression

**Objective:** Verify that detected files are properly compressed.  
**Preconditions:** TC-L01 has passed.  
**Test Steps:**

1. Execute TC-L01 steps.
2. Wait for the workflow DAG to complete.
3. Check the `shared_folder/compressed` directory.
   **Expected Result:** A compressed ZIP file with the original filename + .zip extension exists in the compressed directory.

### TC-L03: File Organization

**Objective:** Verify correct file organization after processing.  
**Preconditions:** TC-L02 has passed.  
**Test Steps:**

1. Execute TC-L02 steps.
2. Check the `shared_folder/processed` directory.
   **Expected Result:** The original file has been copied to the processed directory while remaining in the source directory.

### TC-L04: Email Notification

**Objective:** Verify email notifications are sent with file specifications.  
**Preconditions:** SMTP settings are configured, TC-L03 has passed.  
**Test Steps:**

1. Execute TC-L03 steps.
2. Check email at the configured EMAIL_RECIPIENT address.
   **Expected Result:** Email received with correct file details including name, size, compression ratio, and processing time.

### TC-L05: Multiple File Types

**Objective:** Verify processing of various file types.  
**Preconditions:** Airflow DAGs are running.  
**Test Steps:**

1. Copy multiple file types (txt, pdf, csv, json) to the shared folder.
2. Wait for processing to complete.
3. Check processed and compressed directories.
   **Expected Result:** All file types are correctly processed and compressed.

## 2. MinIO Processing Tests

### TC-M01: MinIO Connection

**Objective:** Verify Airflow can connect to MinIO.  
**Preconditions:** Airflow and MinIO services are running.  
**Test Steps:**

1. Enable `unified_minio_processing_dag`.
2. Wait for a DAG run to complete.
3. Check task logs for setup_minio_infrastructure.
   **Expected Result:** Task completes successfully with bucket creation confirmation.

### TC-M02: MinIO File Detection

**Objective:** Verify detection of new files in MinIO source-files bucket.  
**Preconditions:** TC-M01 has passed.  
**Test Steps:**

1. Upload a file to the `source-files` bucket via MinIO console.
2. Wait for 1-2 minutes for the scheduled DAG run.
3. Check Airflow UI for task execution.
   **Expected Result:** The file is detected and processing begins.

### TC-M03: MinIO File Compression

**Objective:** Verify MinIO file compression functionality.  
**Preconditions:** TC-M02 has passed.  
**Test Steps:**

1. Execute TC-M02 steps.
2. Wait for processing to complete.
3. Check `compressed-files` bucket in MinIO.
   **Expected Result:** A compressed ZIP file appears in the compressed-files bucket.

### TC-M04: MinIO File Organization

**Objective:** Verify MinIO file organization after processing.  
**Preconditions:** TC-M03 has passed.  
**Test Steps:**

1. Execute TC-M03 steps.
2. Check `processed-files` bucket in MinIO.
   **Expected Result:** Original file is copied to the processed-files bucket.

### TC-M05: MinIO Email Notification

**Objective:** Verify email notifications for MinIO processing.  
**Preconditions:** SMTP settings are configured, TC-M04 has passed.  
**Test Steps:**

1. Execute TC-M04 steps.
2. Check email at the configured EMAIL_RECIPIENT address.
   **Expected Result:** Email received with MinIO-specific file details including bucket information.

## 3. Error Handling Tests

### TC-E01: Invalid File Handling

**Objective:** Verify system behavior with corrupted/empty files.  
**Test Steps:**

1. Create a zero-byte file in the shared folder.
2. Wait for processing to complete.
3. Check logs for error handling.
   **Expected Result:** System processes the file without crashing, logs appropriate warnings.

### TC-E02: Duplicate File Handling

**Objective:** Verify system behavior when processing a duplicate file.  
**Test Steps:**

1. Process a file normally.
2. Copy the same file to the shared folder again.
3. Wait for processing to complete.
   **Expected Result:** System detects the file has already been processed and handles accordingly.

### TC-E03: Large File Handling

**Objective:** Verify system behavior with large files.  
**Test Steps:**

1. Create a large file (>100MB) in the shared folder.
2. Wait for processing to complete.
3. Check compressed output and logs.
   **Expected Result:** File is processed successfully despite size, with appropriate compression.

### TC-E04: MinIO Connection Loss

**Objective:** Verify system behavior during MinIO unavailability.  
**Test Steps:**

1. Start processing a file via MinIO.
2. During processing, stop the MinIO service.
3. Restart MinIO service after failure.
4. Check error handling and recovery.
   **Expected Result:** System logs appropriate errors and retries on the next scheduled run after service restoration.

## 4. Performance Tests

### TC-P01: Concurrent File Processing

**Objective:** Verify system behavior with multiple concurrent files.  
**Test Steps:**

1. Copy 10 files simultaneously to the shared folder.
2. Monitor system resource usage and processing time.
3. Check all results for completeness.
   **Expected Result:** All files are processed correctly, with resource usage within acceptable limits.

### TC-P02: MinIO Throughput

**Objective:** Measure MinIO processing throughput.  
**Test Steps:**

1. Upload 20 files of varying sizes to MinIO source bucket.
2. Measure time taken to process all files.
3. Calculate average throughput.
   **Expected Result:** Processing throughput meets defined performance requirements.

## 5. Integration Tests

### TC-I01: End-to-End Local Workflow

**Objective:** Verify complete local processing workflow.  
**Test Steps:**

1. Start with an empty processed/compressed folder.
2. Copy multiple files of various types to shared folder.
3. Monitor entire workflow from detection to email notification.
   **Expected Result:** All steps complete successfully for all files.

### TC-I02: End-to-End MinIO Workflow

**Objective:** Verify complete MinIO processing workflow.  
**Test Steps:**

1. Start with empty processed/compressed buckets.
2. Upload multiple files of various types to source bucket.
3. Monitor entire workflow from detection to email notification.
   **Expected Result:** All steps complete successfully for all files.

## 6. Regression Test Cases

### TC-R01: System Restart

**Objective:** Verify system recovers correctly after restart.  
**Test Steps:**

1. Start processing files.
2. Restart Docker containers during processing.
3. Check if system resumes processing correctly.
   **Expected Result:** System recovers and completes processing of pending files.

### TC-R02: Configuration Change

**Objective:** Verify system adapts to configuration changes.  
**Test Steps:**

1. Change email settings in DAG files.
2. Restart Airflow scheduler.
3. Process a test file.
   **Expected Result:** System uses new configuration successfully.

## 7. Defect Report Template

### Defect ID: DEF-XXX

**Description:** Brief description of the issue  
**Steps:**

1. Step-by-step reproduction steps
2. Include specific inputs and actions
3. Note expected vs. actual results

**Severity:** Critical/High/Medium/Low  
**Status:** New/In Progress/Fixed/Verified  
**Assigned To:** Team member name  
**Environment:** Docker container versions, OS, etc.  
**Attachments:** Screenshots, logs, etc.

## 8. Test Results Summary

| Test Case ID | Description          | Status | Date | Comments |
| ------------ | -------------------- | ------ | ---- | -------- |
| TC-L01       | Basic File Detection |        |      |          |
| TC-L02       | File Compression     |        |      |          |
| ...          | ...                  |        |      |          |

_Status can be: Pass, Fail, Blocked, Not Run_
