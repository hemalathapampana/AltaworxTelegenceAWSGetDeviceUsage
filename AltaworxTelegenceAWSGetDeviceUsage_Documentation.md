# AltaworxTelegenceAWSGetDeviceUsage Lambda Function Documentation

## Overview
The `AltaworxTelegenceAWSGetDeviceUsage` Lambda function is responsible for processing AT&T Telegence device usage reports by downloading files from FTP/SFTP servers, processing the data, and storing it in the database. It handles three types of reports: Premier, MUBU (voice and data), and Final usage reports.

## 1. Usage Processing Triggers

### SQS Event Triggers
The Lambda function is primarily triggered by **SQS messages** containing processing instructions:

- **Primary Queue**: `TelegenceDeviceUsageQueueURL` (Environment Variable)
- **Notification Queue**: `TelegenceDeviceNotificationQueueURL` (Environment Variable)

#### SQS Message Attributes:
- `InitializeProcessing`: Boolean - Triggers initialization of all AT&T Telegence Providers
- `ServiceProviderId`: Integer - Specific service provider to process
- `FAN`: String - Foundation Account Number for Premier reports
- `ReportType`: String - Type of report ("Premier", "MUBU", "Final")
- `IsFromCloudwatchEvent`: Boolean - Indicates if triggered by CloudWatch scheduled event
- `IsDownLoadFileAgain`: Boolean - Retry failed downloads
- `IsDownloadNextInstance`: Boolean - Continue batch processing
- `FileNamesNextDownload`: String - Comma-separated file names for next download
- `WriteTimesNextDownload`: String - Comma-separated write times
- `DownloadFailedIds`: String - Comma-separated IDs of failed downloads
- `TelegenceSyncDataStep`: Integer - Processing step enumeration

### CloudWatch Scheduled Events
- **Daily Sync Trigger**: Identified by `IsFromCloudwatchEvent = true`
- **Purpose**: Initiates daily processing for Premier/MUBU reports
- **Behavior**: When triggered from CloudWatch, the function processes all configured service providers

#### CloudWatch Event Processing Logic:
```csharp
if (IsFromCloudwatchEvent)
{
    // Process daily usage for all providers
    await StartDailyDeviceUsageProcessingAsync(context, true);
}
```

## 2. FTP/SFTP Configuration Details

### Connection Settings
All FTP/SFTP connections are configured per service provider in the `TelegenceSettings`:

- **Server**: `settings.TelegenceFtpServer`
- **Username**: `settings.TelegenceFtpUsername`
- **Password**: `settings.TelegenceFtpPassword` (Base64 encoded)
- **Authentication**: Password-based authentication using SSH.NET library

### Path Configuration by Report Type

#### Premier Reports
- **Path**: `settings.TelegenceFtpPath`
- **File Pattern**: `{FAN}_*.zip`
- **Description**: Foundation Account Number specific usage files

#### MUBU Reports
- **Path**: `settings.TelegenceFtpMubuPath`
- **Voice Files**: `*_voice.zip`
- **Data Files**: `*_data.zip`
- **Description**: Mobile Unified Billing Usage reports

#### Final Usage Reports
- **Path**: `settings.TelegenceFtpFinalUsagePath`
- **File Pattern**: `CUS_FANALL_*.zip`
- **Description**: Final consolidated usage reports

### File Formats and Structure

#### Supported File Formats:
- **ZIP Archives**: All reports are delivered as ZIP files
- **Internal Formats**: 
  - CSV files (standard)
  - XLS/XLSX files (Excel format detection)
- **Compression**: Standard ZIP compression

#### File Naming Conventions:
- **Premier**: `{FAN}_{timestamp}.zip`
- **MUBU Voice**: `{provider}_{timestamp}_voice.zip`
- **MUBU Data**: `{provider}_{timestamp}_data.zip`
- **Final Usage**: `CUS_FANALL_{YYYYMM}.zip`

## 3. Size Limits and Processing Constraints

### File Size Limits
- **MUBU Row Count Limit**: `MUBURowsCountLimit` (Environment Variable)
  - Default: `200,000` rows per file
  - Configurable via environment variable
- **Batch Processing**: `LimitAmountFilePerRunTimes` (Environment Variable)
  - Limits number of files processed per execution

### Processing Limits:
```csharp
private long MUBURowsCountLimit = (long)Convert.ToDouble(Environment.GetEnvironmentVariable("MUBURowsCountLimit"));
private const long DefaultMUBURowsCountLimit = 200000;
private int LimitAmountFilePerRunTimes = Convert.ToInt32(Environment.GetEnvironmentVariable("LimitAmountFilePerRunTimes"));
```

### Large File Handling:
- **Chunked Processing**: Files exceeding row limits are processed in batches
- **Continuation Messages**: SQS messages sent for remaining data
- **Memory Management**: Streaming file processing to avoid memory issues

## 4. Retry and Continuation Logic

### Download Failure Handling

#### Retry Mechanism:
1. **Failed downloads** are tracked in `TelegenceSFTPFileDownloadStatus` table
2. **Retry Queue**: Failed downloads are re-queued with `IsDownLoadFileAgain = true`
3. **Maximum Attempts**: Controlled by application logic (not explicitly limited)

#### Continuation Logic for Large Files:
```csharp
// When file exceeds row limit, queue next batch
if (records.Count >= MUBURowsCountLimit)
{
    await SendMessageToQueueDownloadAsync(context, serviceProviderId, 
        IsFromCloudwatchEvent, REPORT_TYPE_MUBU, file, DefaultDelaySQS);
}
```

#### File Processing States:
- **Pending**: Not yet processed
- **In Progress**: Currently being downloaded/processed
- **Success**: Successfully processed and marked complete
- **Failed**: Added to retry queue with error details

### Batch Processing Logic:
- **Next Instance Processing**: `IsDownloadNextInstance = true`
- **File Queue Management**: Maintains list of files to process
- **Write Time Tracking**: Preserves file timestamps across batches

## 5. Thresholds for Blank/Stale File Notifications

### Blank File Detection
Files are considered blank when they contain no data records after processing:

#### Blank File Thresholds:
- **Detection**: Files with zero data rows after CSV/Excel parsing
- **Notification Trigger**: Immediate upon detection
- **Database Tracking**: `IsFileBlank = true` in `TelegenceMubuFtpFile` table

#### Blank File Notification Logic:
```csharp
private void MarkMubuFileAsBlank(KeySysLambdaContext context, int mubuFtpFileId)
{
    // Mark file as blank and set notification flag
    SET [IsFileBlank] = true, [HasNotificationBeenSent] = false
}
```

### Stale File Detection
Files are considered stale when they haven't been updated within the threshold period:

#### Stale File Thresholds:
- **FTP Report Notification Threshold**: `FtpReportNotificationThresholdDays` (Environment Variable)
- **Check Files Missed Threshold**: `CheckFilesMissedThresholdDays` (Environment Variable)
- **Default Behavior**: Notification sent when files are older than threshold

#### Stale File Detection Logic:
```csharp
private bool IsRecentFile(UsageFile usageFile)
{
    return usageFile.WriteTime >= DateTime.Now.AddDays(FtpReportNotificationThresholdDays * -1);
}
```

### Notification Configuration:
- **Email Recipients**: `DeviceSyncToEmailAddresses` (General Provider Settings)
- **Email Sender**: `DeviceSyncFromEmailAddress` (General Provider Settings)
- **Notification Content**: HTML formatted with file details and timestamps

## 6. Polly Retry Configuration

### SQL Operations Retry Policy
The function uses Polly for resilient SQL operations:

#### SQL Retry Configuration:
```csharp
private static RetryPolicy GetSqlRetryPolicy(KeySysLambdaContext context)
{
    var sqlTransientRetryPolicy = Policy
        .Handle<SqlException>(SqlServerTransientExceptionDetector.ShouldRetryOn)
        .Or<TimeoutException>()
        .WaitAndRetry(MaxRetries,
            retryAttempt => TimeSpan.FromSeconds(RetryDelaySeconds),
            (exception, timeSpan, retryCount, context) =>
                LogInfo(context, "SQL_RETRY", 
                    $"Encountered transient SQL error - delaying for {timeSpan.TotalMilliseconds}ms, " +
                    $"then making retry {retryCount}. Exception: {exception?.Message}"));
    
    return sqlTransientRetryPolicy;
}
```

#### Retry Parameters:
- **Maximum Retries**: `MaxRetries = 3`
- **Retry Delay**: `RetryDelaySeconds = 5` seconds
- **Backoff Strategy**: Fixed delay between retries
- **Exception Handling**: SQL transient exceptions and timeout exceptions

### SFTP Operations Retry
SFTP operations use implicit retry through SQS message reprocessing:

#### SFTP Retry Mechanism:
- **Connection Failures**: Caught and logged, message remains in queue for retry
- **Download Failures**: Added to failed downloads table for explicit retry
- **Authentication Failures**: Logged as warnings, no automatic retry

#### SQS Retry Configuration:
- **Default Delay**: `DefaultDelaySQS = 10` seconds
- **Maximum SQS Delay**: `SQSMaxDelaySeconds = 900` seconds (15 minutes)
- **Cleanup Retries**: `DeviceCleanupMaxRetries` (Environment Variable)

### Error Handling and Logging:
- **Transient Errors**: Automatically retried with Polly
- **Permanent Errors**: Logged and reported via notifications
- **Retry Exhaustion**: Final failure state with comprehensive error logging

## Environment Variables Summary

| Variable Name | Purpose | Default Value |
|---------------|---------|---------------|
| `TelegenceDeviceUsageQueueURL` | Primary SQS queue for processing | Required |
| `TelegenceDeviceNotificationQueueURL` | Notification queue URL | Required |
| `DeviceCleanupMaxRetries` | Maximum cleanup retry attempts | Required |
| `DaysToKeep` | Days to retain processed files | Required |
| `FtpReportNotificationThresholdDays` | Stale file notification threshold | Required |
| `CheckFilesMissedThresholdDays` | Missed files check period | Required |
| `LimitAmountFilePerRunTimes` | Files per execution limit | Required |
| `PremiereReportDelayDays` | Premier report processing delay | Required |
| `IsPremiereReportDelaySimulator` | Enable delay simulation (1/0) | Required |
| `DayEndBillingSimulator` | Billing day simulation | Required |
| `MUBURowsCountLimit` | MUBU file row processing limit | 200,000 |

## Processing Flow Summary

1. **Trigger Reception**: SQS message or CloudWatch event
2. **Configuration Loading**: Environment variables and service provider settings
3. **FTP/SFTP Connection**: Establish secure connection to file server
4. **File Discovery**: List and filter files based on patterns and thresholds
5. **Download Processing**: Stream download with size and format validation
6. **Data Processing**: Parse CSV/Excel data with row limit enforcement
7. **Database Storage**: Bulk insert with transaction management
8. **Retry Management**: Queue failed operations for retry processing
9. **Notification Dispatch**: Send alerts for blank/stale files
10. **Cleanup Operations**: Remove processed files and update status

This comprehensive documentation covers all aspects of the AltaworxTelegenceAWSGetDeviceUsage Lambda function's operation, configuration, and error handling mechanisms.