# Lambda Usage Documentation - Missing Components

This document addresses the missing documentation components for the Altaworx Telegence AWS Lambda function usage patterns and mechanisms.

## 1. Continuation Mechanics: Multi-Run Attributes

### IsDownloadNextInstance Attribute

The `IsDownloadNextInstance` attribute is a critical continuation mechanism that enables multi-run processing for large file downloads and processing operations.

#### Purpose
- **Multi-Run Continuation**: Enables the Lambda function to continue processing across multiple invocations when file processing exceeds single execution limits
- **State Preservation**: Maintains processing state between Lambda invocations to ensure data integrity
- **Resource Management**: Prevents Lambda timeout issues by breaking large operations into manageable chunks

#### Implementation Details

```csharp
// Message attribute extraction
var isDownloadNextInstance = false;
if (message.MessageAttributes.ContainsKey("IsDownloadNextInstance"))
{
    isDownloadNextInstance = Convert.ToBoolean(message.MessageAttributes["IsDownloadNextInstance"].StringValue);
    LogInfo(context, "IsDownloadNextInstance", isDownloadNextInstance);
}
```

#### Associated Attributes for Continuation

1. **WriteTimesNextDownload**: List of file write timestamps for next download batch
   ```csharp
   var writeTimesNextDownload = new List<string>();
   if (message.MessageAttributes.ContainsKey("WriteTimesNextDownload"))
   {
       var writeTimesNextDownloadString = message.MessageAttributes["WriteTimesNextDownload"].StringValue;
       writeTimesNextDownload = writeTimesNextDownloadString.Split(',').ToList();
   }
   ```

2. **FileNamesNextDownload**: List of file names to process in next iteration
   ```csharp
   var fileNamesNextDownload = new List<string>();
   if (message.MessageAttributes.ContainsKey("FileNamesNextDownload"))
   {
       var writeTimesNextDownloadString = message.MessageAttributes["FileNamesNextDownload"].StringValue;
       fileNamesNextDownload = writeTimesNextDownloadString.Split(',').ToList();
   }
   ```

3. **DownloadFailedIds**: List of failed download IDs for retry processing
   ```csharp
   var downloadFailedIds = new List<string>();
   if (message.MessageAttributes.ContainsKey("DownloadFailedIds"))
   {
       var downLoadFailedIdsString = message.MessageAttributes["DownloadFailedIds"].StringValue;
       downloadFailedIds = downLoadFailedIdsString.Split(',').ToList();
   }
   ```

#### Processing Flow

```csharp
if (isDownloadNextInstance && fileNamesNextDownload.Count > 0 && writeTimesNextDownload.Count > 0)
{
    await ProcessDownloadFileNextInstance(context, serviceProviderId, fan, reportType, 
        settings.TelegenceFtpUsername, password, settings.TelegenceFtpServer, 
        settings.TelegenceFtpPath, settings.TelegenceFtpMubuPath,
        settings.TelegenceFtpFinalUsagePath, isFromCloudwatchEvent, 
        fileNamesNextDownload, writeTimesNextDownload, downloadFailedIds);
}
```

#### Message Queue Continuation

When continuation is needed, the Lambda function sends a message to itself with continuation attributes:

```csharp
var messageAttributes = new Dictionary<string, MessageAttributeValue>
{
    {"IsDownloadNextInstance", new MessageAttributeValue {DataType = "String", StringValue = true.ToString() }},
    {"WriteTimesNextDownload", new MessageAttributeValue {DataType = "String", StringValue = string.Join(",", writeTimesNextDownload)}},
    {"FileNamesNextDownload", new MessageAttributeValue {DataType = "String", StringValue = string.Join(",", fileNamesNextDownload)}},
    {"DownloadFailedIds", new MessageAttributeValue {DataType = "String", StringValue = string.Join(",", downloadFailedIds)}}
};
```

---

## 2. MUBU Multi-Step Sync: TelegenceSyncDataStep Chaining

### Overview

The MUBU (Mobile Usage Billing Unit) multi-step synchronization process uses the `TelegenceSyncDataStep` enumeration to chain sequential processing steps across multiple Lambda invocations.

### TelegenceSyncDataStep Enumeration

The system uses the following step sequence:

1. **None** - Initial state
2. **UpdateTelegenceMubuUsageFromStaging** - First step: Update Telegence MUBU usage from staging tables
3. **UpdateMobilityMubuUsageFromTelegence** - Second step: Update Mobility MUBU usage from Telegence data
4. **UpdateLateMubuUsageFromTelegence** - Final step: Update late MUBU usage from Telegence data

### Step Processing Logic

```csharp
var telegenceSyncDataStep = (int)TelegenceSyncDataStepEnum.None;
if (message.MessageAttributes.ContainsKey(SQSMessageKeyConstant.TELEGENCE_SYNC_DATA_STEP))
{
    telegenceSyncDataStep = int.Parse(message.MessageAttributes[SQSMessageKeyConstant.TELEGENCE_SYNC_DATA_STEP].StringValue);
    LogInfo(context, SQSMessageKeyConstant.TELEGENCE_SYNC_DATA_STEP, 
        Enum.GetName(typeof(TelegenceSyncDataStepEnum), telegenceSyncDataStep));
}
```

### Chaining Mechanism

#### Step 1: UpdateTelegenceMubuUsageFromStaging
```csharp
if (telegenceSyncDataStep == (int)TelegenceSyncDataStepEnum.UpdateTelegenceMubuUsageFromStaging)
{
    UpdateTelegenceMubuUsageFromStaging(context, serviceProviderId);
    await SendProcessMessageToQueueAsync(context, serviceProviderId, string.Empty, REPORT_TYPE_MUBU, true, 
        (int)TelegenceSyncDataStepEnum.UpdateMobilityMubuUsageFromTelegence);
}
```

#### Step 2: UpdateMobilityMubuUsageFromTelegence
```csharp
else if (telegenceSyncDataStep == (int)TelegenceSyncDataStepEnum.UpdateMobilityMubuUsageFromTelegence)
{
    UpdateMobilityMubuUsageFromTelegence(context, serviceProviderId);
    UpdateTelegenceKafkaUsage(context, serviceProviderId);
    await SendProcessMessageToQueueAsync(context, serviceProviderId, string.Empty, REPORT_TYPE_MUBU, true, 
        (int)TelegenceSyncDataStepEnum.UpdateLateMubuUsageFromTelegence);
}
```

#### Step 3: UpdateLateMubuUsageFromTelegence
```csharp
else if (telegenceSyncDataStep == (int)TelegenceSyncDataStepEnum.UpdateLateMubuUsageFromTelegence)
{
    UpdateLateMubuUsageFromTelegence(context, serviceProviderId);
    // Final step - no further chaining
}
```

### Message Queue Chaining

Each step sends a message to the next step:

```csharp
private async Task SendProcessMessageToQueueAsync(KeySysLambdaContext context, int serviceProviderId, 
    string fan, string reportType, bool fromCloudwatchEvent, 
    int telegenceSyncDataStep = (int)TelegenceSyncDataStepEnum.None, int delay = DefaultDelaySQS)
{
    var messageAttributes = new Dictionary<string, MessageAttributeValue>
    {
        {"TelegenceSyncDataStep", new MessageAttributeValue {DataType = "String", StringValue = telegenceSyncDataStep.ToString()}},
        {"ServiceProviderId", new MessageAttributeValue {DataType = "String", StringValue = serviceProviderId.ToString()}},
        {"ReportType", new MessageAttributeValue {DataType = "String", StringValue = reportType}},
        {"IsFromCloudwatchEvent", new MessageAttributeValue {DataType = "String", StringValue = fromCloudwatchEvent.ToString()}}
    };
}
```

---

## 3. Blank Files: Special Notification Handling

### Overview

The system implements comprehensive blank file detection and notification mechanisms to alert administrators when empty or corrupted files are encountered during processing.

### Blank File Detection

#### Database Tracking
```csharp
private bool IsBlankFileFromDB(KeySysLambdaContext context, string fileName)
{
    var isBlankFile = false;
    using (var Conn = new SqlConnection(context.CentralDbConnectionString))
    {
        using (var Cmd = new SqlCommand(@"SELECT Id FROM [TelegenceMubuFtpFile] 
            WHERE [FtpFileName] = @fileName 
            AND [IsFileBlank] = @isBlankFile
            AND [HasNotificationBeenSent] = @hasNotificationBeenSent", Conn))
        {
            Cmd.Parameters.AddWithValue("@fileName", fileName);
            Cmd.Parameters.AddWithValue("@isBlankFile", true);
            Cmd.Parameters.AddWithValue("@hasNotificationBeenSent", true);
            // ... execution logic
        }
    }
    return isBlankFile;
}
```

#### File Processing Logic
```csharp
var isBlankFile = IsBlankFileFromDB(context, file.FilePath);
if (isBlankFile)
{
    LogInfo(context, LogTypeConstant.Info, $"The file name {file.FileName} is the blank file.");
    continue; // Skip processing blank files
}
```

### Blank File Marking

When a blank file is detected during processing:

```csharp
private void MarkMubuFileAsBlank(KeySysLambdaContext context, int mubuFtpFileId)
{
    using (var Conn = new SqlConnection(context.CentralDbConnectionString))
    {
        using (var Cmd = new SqlCommand(@"UPDATE [TelegenceMubuFtpFile] 
            SET [IsFileBlank] = @isFileBlank,
                [HasNotificationBeenSent] = @hasNotificationBeenSent
            WHERE Id = @id AND [HasNotificationBeenSent] IS NULL", Conn))
        {
            Cmd.Parameters.AddWithValue("@id", mubuFtpFileId);
            Cmd.Parameters.AddWithValue("@isFileBlank", true);
            Cmd.Parameters.AddWithValue("@hasNotificationBeenSent", false);
            // ... execution
        }
    }
}
```

### Notification Generation

#### Blank File Notification Body
```csharp
private BodyBuilder BuildDownloadMUBUFileEmptyNotificationBody(List<UsageFile> mubuFileList, string title)
{
    var bodyBuilder = new BodyBuilder();
    var htmlBody = new StringBuilder();
    
    htmlBody.Append($"<h3>{title}</h3>");
    htmlBody.Append("<div>The following MUBU files were found to be empty:</div>");
    htmlBody.Append("<ul>");
    
    foreach (var file in mubuFileList)
    {
        htmlBody.Append($"<li>{file.FileName} - {file.WriteTime}</li>");
    }
    
    htmlBody.Append("</ul>");
    bodyBuilder.HtmlBody = htmlBody.ToString();
    return bodyBuilder;
}
```

#### Notification Completion Update
```csharp
private void UpdateBlankFileNotifyComplete(KeySysLambdaContext context)
{
    using (var Conn = new SqlConnection(context.CentralDbConnectionString))
    {
        using (var Cmd = new SqlCommand(@"UPDATE [TelegenceMubuFtpFile] 
            SET [IsFileBlank] = @isFileBlank,
                [HasNotificationBeenSent] = @hasNotificationBeenSent
            WHERE [HasNotificationBeenSent] = 0", Conn))
        {
            Cmd.Parameters.AddWithValue("@isFileBlank", true);
            Cmd.Parameters.AddWithValue("@hasNotificationBeenSent", true);
            // ... execution
        }
    }
}
```

---

## 4. Initialization Cleanup: Queue Entry Clearing

### Overview

The Lambda function implements comprehensive initialization cleanup procedures to ensure clean processing states and prevent stale queue entries from interfering with current operations.

### Queue Clearing Mechanism

#### Primary Queue Cleanup
```csharp
private static void ClearQueue(KeySysLambdaContext context, string dbConnectionString)
{
    LogInfo(context, "SUB", "ClearQueue");
    try
    {
        using (var conn = new SqlConnection(dbConnectionString))
        {
            using (var cmd = new SqlCommand("TRUNCATE TABLE [dbo].[TelegenceDeviceUsageIdsToProcess]", conn))
            {
                cmd.CommandType = CommandType.Text;
                cmd.CommandTimeout = 800;
                conn.Open();
                cmd.ExecuteNonQuery();
            }
        }
    }
    catch (Exception ex)
    {
        LogInfo(context, "EXCEPTION", $"ClearQueue() - {ex.Message}");
    }
}
```

### Initialization Process

#### Daily Processing Initialization
```csharp
private async Task StartDailyDeviceUsageProcessingAsync(KeySysLambdaContext context, bool isFromCloudwatchEvent = false)
{
    LogInfo(context, "SUB", "StartDailyDeviceUsageProcessingAsync");
    
    // Clear existing queue entries before starting new processing
    ClearQueue(context, context.CentralDbConnectionString);
    
    // Continue with provider processing...
    var currentServiceProviderId = 0;
    do
    {
        currentServiceProviderId = ServiceProviderCommon.GetNextServiceProviderId(
            context.CentralDbConnectionString, IntegrationType.Telegence, currentServiceProviderId);
            
        if (currentServiceProviderId > 0)
        {
            await AddProviderToQueueAsync(context, context.CentralDbConnectionString, 
                currentServiceProviderId, DeviceNotificationQueueURL, isFromCloudwatchEvent);
        }
    } while (currentServiceProviderId > 0);
}
```

### FTP Cleanup Operations

#### FTP Directory Cleanup
```csharp
private void CleanUpFtp(KeySysLambdaContext context, string username, string password, string server, string path)
{
    LogInfo(context, "SUB", "CleanUpFtp");
    
    try
    {
        using (var client = new SftpClient(server, username, password))
        {
            client.Connect();
            
            if (client.IsConnected)
            {
                var files = client.ListDirectory(path);
                var filesToDelete = files.Where(f => f.IsRegularFile && 
                    f.LastWriteTime < DateTime.Now.AddDays(-Convert.ToInt32(DaysToKeep)));
                
                foreach (var file in filesToDelete)
                {
                    try
                    {
                        client.DeleteFile(file.FullName);
                        LogInfo(context, "INFO", $"Deleted old file: {file.Name}");
                    }
                    catch (Exception ex)
                    {
                        LogInfo(context, "WARNING", $"Failed to delete file {file.Name}: {ex.Message}");
                    }
                }
            }
            
            client.Disconnect();
        }
    }
    catch (Exception ex)
    {
        LogInfo(context, "EXCEPTION", $"CleanUpFtp failed: {ex.Message}");
    }
}
```

### Cleanup Integration

The cleanup process is integrated into the main processing flow:

```csharp
public async Task FunctionHandler(SQSEvent sqsEvent, ILambdaContext context)
{
    KeySysLambdaContext keysysContext = null;
    try
    {
        keysysContext = BaseFunctionHandler(context);
        await ProcessEventAsync(keysysContext, sqsEvent);
    }
    catch (Exception ex)
    {
        LogInfo(keysysContext, "EXCEPTION", ex.Message + " " + ex.StackTrace);
    }
    
    // Ensure cleanup always occurs
    CleanUp(keysysContext);
}
```

---

## 5. Notifications: Stale/Blank File Alerts and Threshold-Based Notifications

### Overview

The system implements comprehensive notification mechanisms for detecting and alerting on stale files, blank files, and threshold-based conditions that may indicate system issues.

### Threshold-Based Configuration

#### Environment Variables
```csharp
private int FtpReportNotificationThresholdDays = Convert.ToInt32(Environment.GetEnvironmentVariable("FtpReportNotificationThresholdDays"));
private int CheckFilesMissedThresholdDays = Convert.ToInt32(Environment.GetEnvironmentVariable("CheckFilesMissedThresholdDays"));
```

### Stale File Detection

#### File Age Validation
```csharp
private bool IsFileWithinThreshold(UsageFile usageFile)
{
    return usageFile.WriteTime >= DateTime.Now.AddDays(FtpReportNotificationThresholdDays * -1);
}
```

#### Stale Usage Detection Logic
```csharp
// Check for stale voice usage
if (latestMubuVoice != null && !IsFileWithinThreshold(latestMubuVoice))
{
    context.logger.LogInfo("WARNING", $"Stale voice usage for MUBU - last write time: {latestMubuVoice.WriteTime}");
    
    var subject = $"Stale MUBU Voice Sync - {serviceProviderName}";
    var body = BuildStaleMubuVoiceSyncNotificationBody(serviceProviderName, latestMubuVoice);
    
    // Send notification logic here
}

// Check for stale usage data
if (latestMubuUsage != null && !IsFileWithinThreshold(latestMubuUsage))
{
    context.logger.LogInfo("WARNING", $"Stale usage for MUBU - last write time: {latestMubuUsage.WriteTime}");
    
    var subject = $"Stale MUBU Usage Sync - {serviceProviderName}";
    var body = BuildStaleMubuUsageSyncNotificationBody(serviceProviderName, latestMubuUsage);
    
    // Send notification logic here
}
```

### Stale File Notification Bodies

#### Stale Voice Notification
```csharp
private BodyBuilder BuildStaleMubuVoiceSyncNotificationBody(string serviceProvider, UsageFile mostRecentFile)
{
    var bodyBuilder = new BodyBuilder();
    bodyBuilder.HtmlBody = string.Format(
        "<h3>Stale MUBU Voice Sync Alert - {0}</h3>" +
        "<div>{0} MUBU voice report has not been delivered to FTP since {1}. " +
        "AMOP voice metrics may be stale until FTP delivery resumes.</div>",
        serviceProvider, mostRecentFile.WriteTime);
    
    return bodyBuilder;
}
```

#### Stale Usage Notification
```csharp
private BodyBuilder BuildStaleMubuUsageSyncNotificationBody(string serviceProvider, UsageFile mostRecentFile)
{
    var bodyBuilder = new BodyBuilder();
    bodyBuilder.HtmlBody = string.Format(
        "<h3>Stale MUBU Usage Sync Alert - {0}</h3>" +
        "<div>{0} MUBU usage report has not been delivered to FTP since {1}. " +
        "AMOP usage metrics may be stale until FTP delivery resumes.</div>",
        serviceProvider, mostRecentFile.WriteTime);
    
    return bodyBuilder;
}
```

### Notification Queue Integration

#### Device Notification Queue
```csharp
private async Task SendNotificationMessageToQueueAsync(KeySysLambdaContext context, int serviceProviderId)
{
    LogInfo(context, "SUB", $"SendNotificationMessageToQueueAsync({serviceProviderId})");
    LogInfo(context, "DeviceNotificationQueueURL", DeviceNotificationQueueURL);
    
    if (string.IsNullOrEmpty(DeviceNotificationQueueURL))
    {
        LogInfo(context, "WARN", "DeviceNotificationQueueURL is not configured");
        return;
    }
    
    var messageAttributes = new Dictionary<string, MessageAttributeValue>
    {
        {"ServiceProviderId", new MessageAttributeValue {DataType = "String", StringValue = serviceProviderId.ToString()}},
        {"MaxRetries", new MessageAttributeValue {DataType = "Number", StringValue = DeviceCleanupMaxRetries}}
    };
    
    var sendMessageRequest = new SendMessageRequest
    {
        MessageAttributes = messageAttributes,
        MessageBody = "Sending device sync cleanup/notification message",
        QueueUrl = DeviceNotificationQueueURL
    };
    
    // Send message to notification queue
    using (var sqsClient = new AmazonSQSClient(AwsCredentials(context)))
    {
        await sqsClient.SendMessageAsync(sendMessageRequest);
    }
}
```

### Notification Processing Integration

The notification system is integrated into the main processing workflow:

```csharp
private async Task AddProviderToQueueAsync(KeySysLambdaContext context, string dbConnectionString, 
    int serviceProviderId, string deviceNotificationQueueURL, bool fromCloudwatchEvent)
{
    // ... processing logic ...
    
    // Check for stale files and send notifications
    if (latestUsageFile != null && !IsFileWithinThreshold(latestUsageFile))
    {
        context.logger.LogInfo("WARNING", $"Stale usage for FAN {fan} - last write time: {latestUsageFile.WriteTime}");
        // Trigger notification process
    }
    
    // Send notification message for cleanup/monitoring
    await SendNotificationMessageToQueueAsync(context, serviceProviderId);
}
```

---

## Summary

This documentation covers the four critical missing components of the Lambda usage documentation:

1. **Continuation Mechanics**: Detailed explanation of `IsDownloadNextInstance` and related attributes for multi-run processing
2. **MUBU Multi-Step Sync**: Complete description of `TelegenceSyncDataStep` chaining mechanism
3. **Blank File Handling**: Comprehensive coverage of blank file detection and notification systems
4. **Initialization Cleanup**: Thorough documentation of queue clearing and cleanup procedures
5. **Notification Systems**: Complete coverage of stale file alerts and threshold-based notifications

Each section provides implementation details, code examples, and integration patterns to ensure complete understanding of these critical Lambda function capabilities.