# Sample Defect Reports

This document contains sample defect reports for the Event-Driven File Processing system. Use these as templates when reporting actual issues found during testing.

## Defect ID: DEF-001

**Description:** Email notification fails when SMTP settings are incorrect  
**Steps:**

1. Configure incorrect SMTP credentials in the DAG files
2. Process a file through the local file workflow
3. Check logs for error messages
4. Expected: System should log error but continue processing
5. Actual: Task fails and entire DAG run fails, preventing further file processing

**Severity:** Medium  
**Status:** New  
**Assigned To:** [Developer Name]  
**Environment:** Docker containers, Airflow 2.6.1  
**Attachments:** [Error log screenshot]

## Defect ID: DEF-002

**Description:** MinIO file detection doesn't handle filenames with special characters  
**Steps:**

1. Upload a file with special characters (e.g., "test#file%.txt") to MinIO source-files bucket
2. Wait for processing DAG to run
3. Expected: File should be processed normally
4. Actual: Processing fails with object key encoding error

**Severity:** High  
**Status:** New  
**Assigned To:** [Developer Name]  
**Environment:** MinIO latest, Airflow 2.6.1  
**Attachments:** [Error log]

## Defect ID: DEF-003

**Description:** Large files (>500MB) cause memory errors during compression  
**Steps:**

1. Place a 600MB file in the shared folder
2. Wait for the file_workflow_dag to process it
3. Expected: File should be compressed successfully
4. Actual: Python process runs out of memory during compression

**Severity:** Medium  
**Status:** New  
**Assigned To:** [Developer Name]  
**Environment:** Docker containers with default memory allocation  
**Attachments:** [Memory error log]

## Defect ID: DEF-004

**Description:** System doesn't handle file deletion while processing  
**Steps:**

1. Place a file in the shared folder
2. When file_sensor_dag detects it but before compression completes, delete the file
3. Expected: Task should fail gracefully with appropriate error message
4. Actual: DAG crashes with unhandled exception

**Severity:** Low  
**Status:** New  
**Assigned To:** [Developer Name]  
**Environment:** Local filesystem, Airflow 2.6.1  
**Attachments:** [Error screenshot]

## Defect ID: DEF-005

**Description:** Duplicate emails sent when file has same name as previously processed file  
**Steps:**

1. Process a file normally (e.g., "test.txt")
2. Delete the file from processed and compressed folders
3. Place a different file with the same name ("test.txt") in the shared folder
4. Wait for processing
5. Expected: Normal single email notification
6. Actual: Multiple email notifications sent for the same file

**Severity:** Low  
**Status:** New  
**Assigned To:** [Developer Name]  
**Environment:** Local filesystem, SMTP configured  
**Attachments:** [Screenshot of duplicate emails]
