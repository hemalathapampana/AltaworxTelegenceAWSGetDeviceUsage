# AltaworxTelegenceAWSGetDeviceUsage Lambda Function Documentation

## Overview
The **AltaworxTelegenceAWSGetDeviceUsage** Lambda function is responsible for processing AT&T Telegence device usage reports by downloading files from FTP/SFTP servers, processing the data, and storing it in the database. It handles three types of reports: Premier, MUBU (voice and data), and Final usage reports.

## 1. Usage Processing Triggers

### SQS Event Triggers
The **AltaworxTelegenceAWSGetDeviceUsage** Lambda function is primarily triggered by SQS messages containing processing instructions:

#### Primary Queues:
- **Primary Queue**: `TelegenceDeviceUsageQueueURL` (Environment Variable)
  - Value: `https://sqs.us-east-1.amazonaws.com/130265568833/Telegence_Usage_TEST`
- **Notification Queue**: `TelegenceDeviceNotificationQueueURL` (Environment Variable)
  - Value: `https://sqs.us-east-1.amazonaws.com/130265568833/Telegence_Device_TEST`

#### SQS Message Attributes:
The **AltaworxTelegenceAWSGetDeviceUsage** Lambda function processes the following SQS message attributes:

- **InitializeProcessing**: Boolean - Triggers initialization of all AT&T Telegence Providers
- **ServiceProviderId**: Integer - Specific service provider to process
- **FAN**: String - Foundation Account Number for Premier reports
- **ReportType**: String - Type of report ("Premier", "MUBU", "Final")
- **IsFromCloudwatchEvent**: Boolean - Indicates if triggered by CloudWatch scheduled event
- **IsDownLoadFileAgain**: Boolean - Retry failed downloads
- **IsDownloadNextInstance**: Boolean - Continue batch processing
- **FileNamesNextDownload**: String - Comma-separated file names for next download
- **WriteTimesNextDownload**: String - Comma-separated write times
- **DownloadFailedIds**: String - Comma-separated IDs of failed downloads
- **TelegenceSyncDataStep**: Integer - Processing step enumeration

#### Message Processing Logic:
```csharp
// From AltaworxTelegenceAWSGetDeviceUsage Lambda function
if (message.MessageAttributes.ContainsKey("InitializeProcessing"))
{
    initializeProcessing = Convert.ToBoolean(message.MessageAttributes["InitializeProcessing"].StringValue);
}

if (message.MessageAttributes.ContainsKey("IsFromCloudwatchEvent"))
{
    isFromCloudwatchEvent = Convert.ToBoolean(message.MessageAttributes["IsFromCloudwatchEvent"].StringValue);
    IsFromCloudwatchEvent = isFromCloudwatchEvent;
}
```

### CloudWatch Scheduled Events
**Daily Sync Trigger**: Identified by `IsFromCloudwatchEvent = true` in the **AltaworxTelegenceAWSGetDeviceUsage** Lambda function

#### Purpose: 
Initiates daily processing for Premier/MUBU reports in the **AltaworxTelegenceAWSGetDeviceUsage** Lambda function

#### Behavior: 
When triggered from CloudWatch, the **AltaworxTelegenceAWSGetDeviceUsage** Lambda function processes all configured service providers

#### CloudWatch Event Processing Logic:
```csharp
// From AltaworxTelegenceAWSGetDeviceUsage Lambda function
if (initializeProcessing)
{
    await StartDailyDeviceUsageProcessingAsync(context, true);
}
else if (!initializeProcessing && reportType == REPORT_TYPE_MUBU && IsFromCloudwatchEvent && telegenceSyncDataStep > 0)
{
    // Process specific MUBU sync data steps
}
```

### Trigger Source Analysis
Based on the environment variables provided for the **AltaworxTelegenceAWSGetDeviceUsage** Lambda function:

- **From SQS**: The function is triggered by messages sent to `TelegenceDeviceUsageQueueURL`
- **From CloudWatch**: The function can be triggered by scheduled CloudWatch events when `IsFromCloudwatchEvent = true`
- **Both triggers are supported**: The **AltaworxTelegenceAWSGetDeviceUsage** Lambda function can handle both SQS-based and CloudWatch-based triggers simultaneously

## 2. FTP/SFTP Configuration Details

### Connection Settings
All FTP/SFTP connections in the **AltaworxTelegenceAWSGetDeviceUsage** Lambda function are configured per service provider in the TelegenceSettings:

- **Server**: `settings.TelegenceFtpServer`
  - Value: `ec2-35-169-206-35.compute-1.amazonaws.com`
- **Username**: `settings.TelegenceFtpUsername`
  - Value: `jasper`
- **Password**: `settings.TelegenceFtpPassword` (Base64 encoded)
  - Value: `R2FcLDg+PCQ=`
- **Authentication**: Password-based authentication using SSH.NET library in the **AltaworxTelegenceAWSGetDeviceUsage** Lambda function

### Path Configuration by Report Type

#### Premier Reports
- **Path**: `settings.TelegenceFtpPath`
  - Value: `/Carriers/Altaworx/Telegence`
- **File Pattern**: `{FAN}_*.zip`
- **Description**: Foundation Account Number specific usage files processed by the **AltaworxTelegenceAWSGetDeviceUsage** Lambda function

#### MUBU Reports
- **Path**: `settings.TelegenceFtpMubuPath`
  - Value: `/Carriers/Altaworx/Telegence/MUBU`
- **Voice Files**: `*_voice.zip`
- **Data Files**: `*_data.zip`
- **Description**: Mobile Unified Billing Usage reports processed by the **AltaworxTelegenceAWSGetDeviceUsage** Lambda function

#### Final Usage Reports
- **Path**: `settings.TelegenceFtpFinalUsagePath`
  - Value: (Empty - not configured in current settings)
- **File Pattern**: `CUS_FANALL_*.zip`
- **Description**: Final consolidated usage reports processed by the **AltaworxTelegenceAWSGetDeviceUsage** Lambda function

### File Formats and Structure

#### Supported File Formats in AltaworxTelegenceAWSGetDeviceUsage Lambda function:
- **ZIP Archives**: All reports are delivered as ZIP files
- **Internal Formats**:
  - CSV files (standard)
  - XLS/XLSX files (Excel format detection)
- **Compression**: Standard ZIP compression
- **Processing**: Uses `System.IO.Compression.ZipArchive` for extraction

#### File Naming Conventions:
- **Premier**: `{FAN}_{timestamp}.zip`
- **MUBU Voice**: `{provider}_{timestamp}_voice.zip`
- **MUBU Data**: `{provider}_{timestamp}_data.zip`
- **Final Usage**: `CUS_FANALL_{YYYYMM}.zip`

#### File Processing Logic:
```csharp
// From AltaworxTelegenceAWSGetDeviceUsage Lambda function
using (var archive = new System.IO.Compression.ZipArchive(ms))
{
    using (var reader = new StreamReader(archive.Entries.First().Open(), Encoding.UTF8, true))
    {
        var fileReader = new MubuReportReader(new MubuRecordFactory());
        var records = fileReader.ReadBatchedRecords(serviceProviderId, file.FilePath, savedRecordCount + 1, fileId, reader, MUBURowsCountLimit, fanFilter);
    }
}
```

## 3. Size Limits and Processing Constraints

### File Size Limits in AltaworxTelegenceAWSGetDeviceUsage Lambda function
- **MUBU Row Count Limit**: `MUBURowsCountLimit` (Environment Variable)
  - **Current Value**: Not specified in environment variables
  - **Default**: 200,000 rows per file
  - **Configurable**: Via environment variable `MUBURowsCountLimit`

#### Batch Processing Limits:
- **LimitAmountFilePerRunTimes**: 10 files per execution
- **BatchSize**: 250 records per batch operation

#### Processing Constraints:
```csharp
// From AltaworxTelegenceAWSGetDeviceUsage Lambda function
private long MUBURowsCountLimit = (long)Convert.ToDouble(Environment.GetEnvironmentVariable("MUBURowsCountLimit"));
private const long DefaultMUBURowsCountLimit = 200000;
private int LimitAmountFilePerRunTimes = Convert.ToInt32(Environment.GetEnvironmentVariable("LimitAmountFilePerRunTimes"));

if (MUBURowsCountLimit <= 0)
{
    MUBURowsCountLimit = DefaultMUBURowsCountLimit;
}
```

### Large File Handling in AltaworxTelegenceAWSGetDeviceUsage Lambda function:
- **Chunked Processing**: Files exceeding row limits are processed in batches
- **Continuation Messages**: SQS messages sent for remaining data
- **Memory Management**: Streaming file processing to avoid memory issues

#### Continuation Logic:
```csharp
// From AltaworxTelegenceAWSGetDeviceUsage Lambda function
if (!records.IsEndOfFile)
{
    await SendMessageToQueueDownloadAsync(context, serviceProviderId, IsFromCloudwatchEvent, REPORT_TYPE_MUBU, file, DefaultDelaySQS);
}
```

## 4. Retry and Continuation Logic

### Download Failure Handling in AltaworxTelegenceAWSGetDeviceUsage Lambda function

#### Retry Mechanism:
- **Failed downloads** are tracked in `TelegenceSFTPFileDownloadStatus` table
- **Retry Queue**: Failed downloads are re-queued with `IsDownLoadFileAgain = true`
- **Maximum Attempts**: Controlled by `DeviceCleanupMaxRetries` environment variable (15 retries)

#### File Processing States:
- **Pending**: Not yet processed
- **In Progress**: Currently being downloaded/processed
- **Success**: Successfully processed and marked complete
- **Failed**: Added to retry queue with error details

#### Retry Processing Logic:
```csharp
// From AltaworxTelegenceAWSGetDeviceUsage Lambda function
private TelegenceSFTPFileDownloadStatus GetFileDownloadFailedByFileName(KeySysLambdaContext context, int serviceProviderId, string reportType, int Id)
{
    var queryString = @"SELECT Id, FileName, WriteTime, ServiceProviderId FROM TelegenceSFTPFileDownloadStatus
                       WHERE [Id] = @Id";
    // Database query logic
}

private async Task<DataTable> DownloadAgainMubuFile(KeySysLambdaContext context, int serviceProviderId, string username, string password, string server, string path, int downLoadFailId)
{
    TelegenceSFTPFileDownloadStatus fileFromDb = GetFileDownloadFailedByFileName(context, serviceProviderId, REPORT_TYPE_MUBU, downLoadFailId);
    if (fileFromDb == null || fileFromDb.Id < 1)
    {
        LogInfo(context, "EXCEPTION", $"Not found file name {fileFromDb.FileName}");
        return null;
    }
    // Retry download logic
}
```

### Continuation Logic for Large Files in AltaworxTelegenceAWSGetDeviceUsage Lambda function:

#### Batch Processing Logic:
- **Next Instance Processing**: `IsDownloadNextInstance = true`
- **File Queue Management**: Maintains list of files to process
- **Write Time Tracking**: Preserves file timestamps across batches

#### SQS Message Continuation:
```csharp
// From AltaworxTelegenceAWSGetDeviceUsage Lambda function
private async Task SendMessageToQueueNextDownloadAsync(KeySysLambdaContext context, int serviceProviderId, bool fromCloudwatchEvent, string reportType,
    string fileNamesNextDownload, string writeTimesNextDownload, string downloadFailedIds)
{
    var messageAttributes = new Dictionary<string, MessageAttributeValue>
    {
        {"InitializeProcessing", new MessageAttributeValue {DataType = "String", StringValue = false.ToString()}},
        {"ServiceProviderId", new MessageAttributeValue {DataType = "Number", StringValue = serviceProviderId.ToString()}},
        {"IsFromCloudwatchEvent", new MessageAttributeValue {DataType = "String", StringValue = fromCloudwatchEvent.ToString()}},
        {"ReportType", new MessageAttributeValue {DataType = "String", StringValue = reportType}},
        {"IsDownloadNextInstance", new MessageAttributeValue {DataType = "String", StringValue = true.ToString()}},
        {"FileNamesNextDownload", new MessageAttributeValue {DataType = "String", StringValue = fileNamesNextDownload}},
        {"WriteTimesNextDownload", new MessageAttributeValue {DataType = "String", StringValue = writeTimesNextDownload}},
        {"DownloadFailedIds", new MessageAttributeValue {DataType = "String", StringValue = downloadFailedIds}}
    };
}
```

## 5. Thresholds for Blank/Stale File Notifications

### Blank File Detection in AltaworxTelegenceAWSGetDeviceUsage Lambda function

#### Files are considered blank when they contain no data records after processing:

- **Detection**: Files with zero data rows after CSV/Excel parsing
- **Notification Trigger**: Immediate upon detection
- **Database Tracking**: `IsFileBlank = true` in `TelegenceMubuFtpFile` table

#### Blank File Detection Logic:
```csharp
// From AltaworxTelegenceAWSGetDeviceUsage Lambda function
private void MarkMubuFileAsBlank(KeySysLambdaContext context, int mubuFtpFileId)
{
    LogInfo(context, "SUB", $"MarkMubuFileAsBlank({mubuFtpFileId})");
    
    using (var con = new SqlConnection(context.CentralDbConnectionString))
    {
        using (var cmd = con.CreateCommand())
        {
            cmd.CommandType = CommandType.Text;
            cmd.CommandText = @"UPDATE TelegenceMubuFtpFile 
                               SET [IsFileBlank] = 1, [HasNotificationBeenSent] = 0 
                               WHERE [Id] = @mubuFtpFileId";
            cmd.Parameters.AddWithValue("@mubuFtpFileId", mubuFtpFileId);
            con.Open();
            cmd.ExecuteNonQuery();
        }
    }
}

private bool IsBlankFileFromDB(KeySysLambdaContext context, string fileName)
{
    var queryString = @"SELECT IsFileBlank FROM TelegenceMubuFtpFile WHERE [FtpFileName] = @fileName";
    // Check if file is marked as blank in database
}
```

#### Blank File Processing:
```csharp
// From AltaworxTelegenceAWSGetDeviceUsage Lambda function
if (records.Records.Rows.Count < 1)
{
    MarkMubuFileAsBlank(context, fileId);
}
```

### Stale File Detection in AltaworxTelegenceAWSGetDeviceUsage Lambda function

#### Files are considered stale when they haven't been updated within the threshold period:

#### Stale File Thresholds:
- **FTP Report Notification Threshold**: `FtpReportNotificationThresholdDays` (Environment Variable)
  - **Current Value**: 3 days
- **Check Files Missed Threshold**: `CheckFilesMissedThresholdDays` (Environment Variable)
  - **Current Value**: 7 days
- **Default Behavior**: Notification sent when files are older than threshold

#### Stale File Detection Logic:
```csharp
// From AltaworxTelegenceAWSGetDeviceUsage Lambda function
private bool IsRecentFile(UsageFile usageFile)
{
    return usageFile.WriteTime >= DateTime.Now.AddDays(FtpReportNotificationThresholdDays * -1);
}

// File processing with threshold check
var today = DateTime.Now;
var startDate = today.AddDays(-CheckFilesMissedThresholdDays);
var endDate = today.AddDays(1);

// Get files downloaded last week in AMOP
var fileNameThresholdDays = GetFilesDownLoaded(context, startDate, endDate);
```

#### Files Downloaded Tracking:
```csharp
// From AltaworxTelegenceAWSGetDeviceUsage Lambda function
private List<string> GetFilesDownLoaded(KeySysLambdaContext context, DateTime startDate, DateTime endDate)
{
    var fileNameList = new List<string>();
    var queryString = @"SELECT FtpFileName FROM TelegenceMubuFtpFile WHERE [CreatedDate] BETWEEN @startDate AND @endDate";
    using (var con = new SqlConnection(context.CentralDbConnectionString))
    {
        using (var cmd = con.CreateCommand())
        {
            cmd.CommandType = CommandType.Text;
            cmd.CommandText = queryString;
            cmd.Parameters.AddWithValue("@startDate", startDate);
            cmd.Parameters.AddWithValue("@endDate", endDate);
            // Query execution logic
        }
    }
    return fileNameList;
}
```

### Notification Configuration for AltaworxTelegenceAWSGetDeviceUsage Lambda function:
- **Email Recipients**: `DeviceSyncToEmailAddresses` (General Provider Settings)
- **Email Sender**: `DeviceSyncFromEmailAddress` (General Provider Settings)
- **Notification Content**: HTML formatted with file details and timestamps

#### Notification Logic:
```csharp
// From AltaworxTelegenceAWSGetDeviceUsage Lambda function
private async Task BuildNotifyDownLoadFileBlank(KeySysLambdaContext context, int serviceProviderId)
{
    LogInfo(context, "SUB", $"BuildNotifyDownLoadFileBlank({serviceProviderId})");
    // Email notification logic for blank/stale files
}
```

## 6. Polly Retry Configuration

### SQL Operations Retry Policy in AltaworxTelegenceAWSGetDeviceUsage Lambda function

The **AltaworxTelegenceAWSGetDeviceUsage** Lambda function uses Polly for resilient SQL operations:

#### SQL Retry Configuration:
```csharp
// From AltaworxTelegenceAWSGetDeviceUsage Lambda function
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

#### SQL Retry Usage:
```csharp
// From AltaworxTelegenceAWSGetDeviceUsage Lambda function
var sqlRetryPolicy = GetSqlRetryPolicy(context);

// Usage examples:
sqlRetryPolicy.Execute(() => UpdateTelegenceFinalUsageFromStaging(context, serviceProviderId, billingPeriodYear, billingPeriodMonth));
sqlRetryPolicy.Execute(() => UpdateTelegenceMubuUsageFromStaging(context, serviceProviderId));
sqlRetryPolicy.Execute(() => UpdateMobilityMubuUsageFromTelegence(context, serviceProviderId));
sqlRetryPolicy.Execute(() => UpdateLateMubuUsageFromTelegence(context, serviceProviderId));
sqlRetryPolicy.Execute(() => UpdateTelegenceKafkaUsage(context, serviceProviderId));
```

### SFTP Operations Retry in AltaworxTelegenceAWSGetDeviceUsage Lambda function

SFTP operations use implicit retry through SQS message reprocessing:

#### SFTP Retry Mechanism:
- **Connection Failures**: Caught and logged, message remains in queue for retry
- **Download Failures**: Added to failed downloads table for explicit retry
- **Authentication Failures**: Logged as warnings, no automatic retry

#### SQS Retry Configuration:
- **Default Delay**: `DefaultDelaySQS = 10` seconds
- **Maximum SQS Delay**: `SQSMaxDelaySeconds = 900` seconds (15 minutes)
- **Cleanup Retries**: `DeviceCleanupMaxRetries` (Environment Variable - 15 retries)

#### Error Handling and Logging:
```csharp
// From AltaworxTelegenceAWSGetDeviceUsage Lambda function
catch (Exception ex)
{
    transactionScope.Dispose();
    LogInfo(context, "WARN", $"Error download again file name {file.FilePath}. Error detail: {ex.Message} - {ex.StackTrace}");
    if (shouldRetrySync && retryDataTable != null)
    {
        AddToDataRow(retryDataTable, file, ex.Message, serviceProviderId, REPORT_TYPE_MUBU);
    }
    return usage;
}
```

## Environment Variables Summary for AltaworxTelegenceAWSGetDeviceUsage Lambda function

| Variable Name | Purpose | Current Value |
|---------------|---------|---------------|
| `TelegenceDeviceUsageQueueURL` | Primary SQS queue for processing | `https://sqs.us-east-1.amazonaws.com/130265568833/Telegence_Usage_TEST` |
| `TelegenceDeviceNotificationQueueURL` | Notification queue URL | `https://sqs.us-east-1.amazonaws.com/130265568833/Telegence_Device_TEST` |
| `DeviceCleanupMaxRetries` | Maximum cleanup retry attempts | 15 |
| `DaysToKeep` | Days to retain processed files | 90 |
| `FtpReportNotificationThresholdDays` | Stale file notification threshold | 3 |
| `CheckFilesMissedThresholdDays` | Missed files check period | 7 |
| `LimitAmountFilePerRunTimes` | Files per execution limit | 10 |
| `PremiereReportDelayDays` | Premier report processing delay | 5 |
| `IsPremiereReportDelaySimulator` | Enable delay simulation (1/0) | 0 |
| `DayEndBillingSimulator` | Billing day simulation | 0 |
| `MUBURowsCountLimit` | MUBU file row processing limit | 200,000 (default) |
| `BatchSize` | Database batch size | 250 |
| `ConnectionString` | Central database connection | Configured |
| `BaseMultiTenantConnectionString` | Multi-tenant database connection | Configured |
| `EnvName` | Environment name | Test |
| `VerboseLogging` | Enable verbose logging | false |

## Processing Flow Summary for AltaworxTelegenceAWSGetDeviceUsage Lambda function

1. **Trigger Reception**: SQS message or CloudWatch event received by **AltaworxTelegenceAWSGetDeviceUsage** Lambda function
2. **Configuration Loading**: Environment variables and service provider settings loaded in **AltaworxTelegenceAWSGetDeviceUsage** Lambda function
3. **FTP/SFTP Connection**: Establish secure connection to file server from **AltaworxTelegenceAWSGetDeviceUsage** Lambda function
4. **File Discovery**: List and filter files based on patterns and thresholds in **AltaworxTelegenceAWSGetDeviceUsage** Lambda function
5. **Download Processing**: Stream download with size and format validation in **AltaworxTelegenceAWSGetDeviceUsage** Lambda function
6. **Data Processing**: Parse CSV/Excel data with row limit enforcement in **AltaworxTelegenceAWSGetDeviceUsage** Lambda function
7. **Database Storage**: Bulk insert with transaction management by **AltaworxTelegenceAWSGetDeviceUsage** Lambda function
8. **Retry Management**: Queue failed operations for retry processing in **AltaworxTelegenceAWSGetDeviceUsage** Lambda function
9. **Notification Dispatch**: Send alerts for blank/stale files from **AltaworxTelegenceAWSGetDeviceUsage** Lambda function
10. **Cleanup Operations**: Remove processed files and update status in **AltaworxTelegenceAWSGetDeviceUsage** Lambda function

## Database Tables Used by AltaworxTelegenceAWSGetDeviceUsage Lambda function

### Primary Tables:
- **TelegenceAllUsageStaging**: Staging table for Premier usage data
- **TelegenceDeviceUsageMubuStaging**: Staging table for MUBU voice and data usage
- **TelegenceDeviceFinalUsageStaging**: Staging table for final usage reports
- **TelegenceMubuFtpFile**: Tracks processed MUBU files and their status
- **TelegenceSFTPFileDownloadStatus**: Tracks failed downloads for retry processing
- **ServiceProviderSetting**: Configuration settings per service provider

### Key Database Operations:
- **Bulk Copy Operations**: High-performance data insertion using `SqlBulkCopy`
- **Transaction Management**: Uses `TransactionScope` for data consistency
- **Retry Logic**: Polly retry policies for transient SQL errors
- **Status Tracking**: File processing status and notification flags

This comprehensive documentation covers all aspects of the **AltaworxTelegenceAWSGetDeviceUsage** Lambda function's operation, configuration, and error handling mechanisms.