# Lambda Usage Documentation

## Overview
This document provides comprehensive responses to queries about the Altaworx Telegence AWS Lambda function for device usage processing, covering continuation mechanics, MUBU multi-step synchronization, blank file handling, initialization cleanup, and notification systems.

## 1. Lambda Usage

### Function Structure
The main Lambda function (`AltaworxTelegenceAWSGetDeviceUsage`) extends `AwsFunctionBase` and processes SQS events for device usage data synchronization from Telegence FTP servers.

**Key Components:**
- **Entry Point**: `FunctionHandler(SQSEvent sqsEvent, ILambdaContext context)`
- **Base Class**: Inherits from `AwsFunctionBase` providing common functionality
- **Processing Types**: Handles Premier, MUBU, and Final report types
- **Event Sources**: SQS messages and CloudWatch events

**Environment Variables:**
```csharp
private string ExportDeviceUsageQueueURL = Environment.GetEnvironmentVariable("TelegenceDeviceUsageQueueURL");
private string DeviceNotificationQueueURL = Environment.GetEnvironmentVariable("TelegenceDeviceNotificationQueueURL");
private string DeviceCleanupMaxRetries = Environment.GetEnvironmentVariable("DeviceCleanupMaxRetries");
private int FtpReportNotificationThresholdDays = Convert.ToInt32(Environment.GetEnvironmentVariable("FtpReportNotificationThresholdDays"));
private long MUBURowsCountLimit = (long)Convert.ToDouble(Environment.GetEnvironmentVariable("MUBURowsCountLimit"));
```

## 2. Continuation Mechanics: Multi-Run Attributes

### IsDownloadNextInstance Attribute

The `IsDownloadNextInstance` attribute enables multi-run continuation for processing large datasets across multiple Lambda invocations.

**Implementation Details:**
```csharp
var isDownloadNextInstance = false;
if (message.MessageAttributes.ContainsKey("IsDownloadNextInstance"))
{
    isDownloadNextInstance = Convert.ToBoolean(message.MessageAttributes["IsDownloadNextInstance"].StringValue);
    LogInfo(context, "IsDownloadNextInstance", isDownloadNextInstance);
}
```

**Usage Scenarios:**
1. **File Processing Continuation**: When processing large files that exceed Lambda execution time limits
2. **Batch Processing**: Breaking down large datasets into manageable chunks
3. **Resource Management**: Preventing timeouts and memory issues

**Associated Attributes:**
- `FileNamesNextDownload`: List of files to process in next iteration
- `WriteTimesNextDownload`: Corresponding write times for files
- `DownloadFailedIds`: Files that failed processing and need retry

**Flow Control:**
```csharp
else if (isDownloadNextInstance && fileNamesNextDownload.Count > 0 && writeTimesNextDownload.Count > 0)
{
    await ProcessDownloadFileNextInstance(context, serviceProviderId, fan, reportType, 
        settings.TelegenceFtpUsername, password, settings.TelegenceFtpServer, 
        settings.TelegenceFtpPath, settings.TelegenceFtpMubuPath,
        settings.TelegenceFtpFinalUsagePath, isFromCloudwatchEvent, 
        fileNamesNextDownload, writeTimesNextDownload, downloadFailedIds);
}
```

### Message Queuing for Continuation
When continuation is needed, the system queues the next instance:
```csharp
await SendMessageToQueueNextDownloadAsync(context, serviceProviderId, isFromCloudwatchEvent, 
    REPORT_TYPE_MUBU, fileNamesNextDownloadString, writeTimesNextDownloadString, 
    downloadFailedIdsString, DefaultDelaySQS);
```

**SQS Message Attributes for Continuation:**
```csharp
{"IsDownloadNextInstance", new MessageAttributeValue {DataType = "String", StringValue = true.ToString() }},
{"FileNamesNextDownload", new MessageAttributeValue {DataType = "String", StringValue = fileNamesNextDownloadString}},
{"WriteTimesNextDownload", new MessageAttributeValue {DataType = "String", StringValue = writeTimesNextDownloadString}},
{"DownloadFailedIds", new MessageAttributeValue {DataType = "String", StringValue = downloadFailedIdsString}}
```

## 3. MUBU Multi-Step Sync: TelegenceSyncDataStep Chaining

### TelegenceSyncDataStep Enumeration
The MUBU synchronization process uses a step-based approach with `TelegenceSyncDataStepEnum`:

**Step Processing Logic:**
```csharp
var telegenceSyncDataStep = (int)TelegenceSyncDataStepEnum.None;
if (message.MessageAttributes.ContainsKey(Amop.Core.Constants.SQSMessageKeyConstant.TELEGENCE_SYNC_DATA_STEP))
{
    telegenceSyncDataStep = int.Parse(message.MessageAttributes[Amop.Core.Constants.SQSMessageKeyConstant.TELEGENCE_SYNC_DATA_STEP].StringValue);
    LogInfo(context, Amop.Core.Constants.SQSMessageKeyConstant.TELEGENCE_SYNC_DATA_STEP, 
        Enum.GetName(typeof(TelegenceSyncDataStepEnum), telegenceSyncDataStep));
}
```

### Multi-Step Processing Chain

**Step 1: UpdateTelegenceMubuUsageFromStaging**
```csharp
if (telegenceSyncDataStep == (int)TelegenceSyncDataStepEnum.UpdateTelegenceMubuUsageFromStaging)
{
    sqlRetryPolicy.Execute(() => UpdateTelegenceMubuUsageFromStaging(context, serviceProviderId));
    await SendProcessMessageToQueueAsync(context, serviceProviderId, string.Empty, REPORT_TYPE_MUBU, 
        true, (int)TelegenceSyncDataStepEnum.UpdateMobilityMubuUsageFromTelegence);
}
```

**Step 2: UpdateMobilityMubuUsageFromTelegence**
```csharp
else if (telegenceSyncDataStep == (int)TelegenceSyncDataStepEnum.UpdateMobilityMubuUsageFromTelegence)
{
    sqlRetryPolicy.Execute(() => UpdateMobilityMubuUsageFromTelegence(context, serviceProviderId));
    TruncateTableByTableName(context, Amop.Core.Constants.DatabaseTableNames.TELEGENCE_DEVICE_USAGE_MUBU_STAGING);
    await SendProcessMessageToQueueAsync(context, serviceProviderId, string.Empty, REPORT_TYPE_MUBU, 
        true, (int)TelegenceSyncDataStepEnum.UpdateLateMubuUsageFromTelegence);
}
```

**Step 3: UpdateLateMubuUsageFromTelegence**
```csharp
else if (telegenceSyncDataStep == (int)TelegenceSyncDataStepEnum.UpdateLateMubuUsageFromTelegence)
{
    sqlRetryPolicy.Execute(() => UpdateLateMubuUsageFromTelegence(context, serviceProviderId));
    TruncateTableByTableName(context, Amop.Core.Constants.DatabaseTableNames.TELEGENCE_DEVICE_USAGE_MUBU_STAGING);
}
```

### Chaining Mechanism
Each step automatically triggers the next step by sending a message to the queue:
```csharp
private async Task SendProcessMessageToQueueAsync(KeySysLambdaContext context, int serviceProviderId, 
    string fan, string reportType, bool isFromCloudwatchEvent, int telegenceSyncDataStep)
{
    var messageAttributes = new Dictionary<string, MessageAttributeValue>
    {
        {"ServiceProviderId", new MessageAttributeValue {DataType = "Number", StringValue = serviceProviderId.ToString()}},
        {"FAN", new MessageAttributeValue {DataType = "String", StringValue = fan}},
        {"ReportType", new MessageAttributeValue {DataType = "String", StringValue = reportType}},
        {"IsFromCloudwatchEvent", new MessageAttributeValue {DataType = "String", StringValue = isFromCloudwatchEvent.ToString()}},
        {Amop.Core.Constants.SQSMessageKeyConstant.TELEGENCE_SYNC_DATA_STEP, 
         new MessageAttributeValue {DataType = "Number", StringValue = telegenceSyncDataStep.ToString()}}
    };
}
```

## 4. Blank Files: Special Notification Handling

### Blank File Detection
The system identifies blank files during processing and marks them appropriately:

```csharp
var isBlankFile = IsBlankFileFromDB(context, file.FilePath);
if (isBlankFile)
{
    LogInfo(context, LogTypeConstant.Info, $"The file name {file.FileName} is the blank file.");
    // Skip processing blank files
}
```

### Database Tracking
Blank files are tracked in the `TelegenceMubuFtpFile` table:
```csharp
private void MarkMubuFileAsBlank(KeySysLambdaContext context, int mubuFtpFileId)
{
    using (var Conn = new SqlConnection(context.CentralDbConnectionString))
    {
        using (var Cmd = new SqlCommand(@"
            UPDATE [TelegenceMubuFtpFile] 
            SET [IsFileBlank] = @isFileBlank,
                [HasNotificationBeenSent] = @hasNotificationBeenSent
            WHERE Id = @id AND [HasNotificationBeenSent] IS NULL", Conn))
        {
            Cmd.Parameters.AddWithValue("@id", mubuFtpFileId);
            Cmd.Parameters.AddWithValue("@isFileBlank", true);
            Cmd.Parameters.AddWithValue("@hasNotificationBeenSent", false);
        }
    }
}
```

### Notification System for Blank Files

**Notification Trigger:**
```csharp
var usageFileList = GetBlankFileList(context);
if (usageFileList.Count > 0)
{
    var serviceProviderName = GetServiceProviderName(context, serviceProviderId);
    var tenantName = GetTenantName(context, serviceProviderId);
    var subject = $"{serviceProviderName} Blank MUBU Report(s) Received ({tenantName})";
    
    var body = BuildDownloadMUBUFileEmptyNotificationBody(usageFileList, subject);
    await SendEmailNotificationAsync(context, subject, body);
    UpdateBlankFileNotifyComplete(context);
}
```

**Notification Body Builder:**
```csharp
private BodyBuilder BuildDownloadMUBUFileEmptyNotificationBody(List<UsageFile> mubuFileList, string title)
{
    var bodyBuilder = new BodyBuilder();
    var htmlBody = new StringBuilder();
    
    htmlBody.AppendLine($"<h2>{title}</h2>");
    htmlBody.AppendLine("<p>The following MUBU files were found to be blank:</p>");
    htmlBody.AppendLine("<table border='1'>");
    htmlBody.AppendLine("<tr><th>File Name</th><th>Created Date</th><th>Write Time UTC</th></tr>");
    
    foreach (var file in mubuFileList)
    {
        htmlBody.AppendLine($"<tr><td>{file.FileName}</td><td>{file.CreatedDate}</td><td>{file.WriteTime}</td></tr>");
    }
    
    htmlBody.AppendLine("</table>");
    bodyBuilder.HtmlBody = htmlBody.ToString();
    return bodyBuilder;
}
```

**Notification Status Update:**
```csharp
private void UpdateBlankFileNotifyComplete(KeySysLambdaContext context)
{
    using (var Conn = new SqlConnection(context.CentralDbConnectionString))
    {
        using (var Cmd = new SqlCommand(@"
            UPDATE [TelegenceMubuFtpFile] 
            SET [IsFileBlank] = @isFileBlank,
                [HasNotificationBeenSent] = @hasNotificationBeenSent
            WHERE [HasNotificationBeenSent] = 0", Conn))
        {
            Cmd.Parameters.AddWithValue("@isFileBlank", true);
            Cmd.Parameters.AddWithValue("@hasNotificationBeenSent", true);
        }
    }
}
```

## 5. Initialization Cleanup: Old Queue Entries

### Cleanup Process
The Lambda function includes comprehensive cleanup mechanisms for old queue entries and temporary data:

**FTP Cleanup:**
```csharp
private void CleanUpFtp(KeySysLambdaContext context, string username, string password, string server, string path)
{
    LogInfo(context, "SUB", "CleanUpFtp");
    
    try
    {
        using (var client = new SftpClient(server, username, password))
        {
            client.Connect();
            var files = client.ListDirectory(path);
            var daysToKeep = Convert.ToInt32(DaysToKeep);
            var cutoffDate = DateTime.Now.AddDays(-daysToKeep);
            
            foreach (var file in files.Where(f => f.LastWriteTime < cutoffDate))
            {
                try
                {
                    client.DeleteFile(file.FullName);
                    LogInfo(context, "CLEANUP", $"Deleted old file: {file.Name}");
                }
                catch (Exception ex)
                {
                    LogInfo(context, "CLEANUP_ERROR", $"Failed to delete {file.Name}: {ex.Message}");
                }
            }
            client.Disconnect();
        }
    }
    catch (Exception ex)
    {
        LogInfo(context, "CLEANUP_EXCEPTION", ex.Message);
    }
}
```

**Database Cleanup:**
```csharp
// Cleanup is performed after each processing cycle
CleanUpFtp(context, settings.TelegenceFtpUsername, password, settings.TelegenceFtpServer, settings.TelegenceFtpPath);
CleanUpFtp(context, settings.TelegenceFtpUsername, password, settings.TelegenceFtpServer, settings.TelegenceFtpMubuPath);
```

**Staging Table Cleanup:**
```csharp
// After processing MUBU data, staging tables are truncated
TruncateTableByTableName(context, Amop.Core.Constants.DatabaseTableNames.TELEGENCE_DEVICE_USAGE_MUBU_STAGING);
```

**Queue Record Cleanup:**
```csharp
// Delete processed queue records
Dequeue(context, serviceProviderId, fan);
```

### Context Cleanup
The base function ensures proper resource cleanup:
```csharp
public virtual void CleanUp(KeySysLambdaContext context)
{
    context.CleanUp();
}

// Called at the end of function execution
CleanUp(keysysContext);
```

## 6. Notifications: Stale/Blank File Alerts and Threshold-Based Notifications

### Threshold Configuration
```csharp
private int FtpReportNotificationThresholdDays = Convert.ToInt32(Environment.GetEnvironmentVariable("FtpReportNotificationThresholdDays"));
private int CheckFilesMissedThresholdDays = Convert.ToInt32(Environment.GetEnvironmentVariable("CheckFilesMissedThresholdDays"));
```

### Stale File Detection
```csharp
private bool IsFileWithinThreshold(UsageFile usageFile)
{
    return usageFile.WriteTime >= DateTime.Now.AddDays(FtpReportNotificationThresholdDays * -1);
}
```

### Stale MUBU Voice Notification
```csharp
private BodyBuilder BuildStaleMubuVoiceSyncNotificationBody(string serviceProvider, UsageFile mostRecentFile)
{
    var bodyBuilder = new BodyBuilder();
    var htmlBody = new StringBuilder();
    
    htmlBody.AppendLine($"<h2>Stale MUBU Voice Report Alert - {serviceProvider}</h2>");
    htmlBody.AppendLine("<p>The most recent MUBU Voice file is older than the threshold:</p>");
    htmlBody.AppendLine("<table border='1'>");
    htmlBody.AppendLine("<tr><th>File Name</th><th>Write Time UTC</th><th>Days Old</th></tr>");
    
    var daysOld = (DateTime.Now - mostRecentFile.WriteTime).Days;
    htmlBody.AppendLine($"<tr><td>{mostRecentFile.FileName}</td><td>{mostRecentFile.WriteTime}</td><td>{daysOld}</td></tr>");
    
    htmlBody.AppendLine("</table>");
    htmlBody.AppendLine($"<p>Threshold: {FtpReportNotificationThresholdDays} days</p>");
    
    bodyBuilder.HtmlBody = htmlBody.ToString();
    return bodyBuilder;
}
```

### Stale MUBU Usage Notification
```csharp
private BodyBuilder BuildStaleMubuUsageSyncNotificationBody(string serviceProvider, UsageFile mostRecentFile)
{
    var bodyBuilder = new BodyBuilder();
    var htmlBody = new StringBuilder();
    
    htmlBody.AppendLine($"<h2>Stale MUBU Usage Report Alert - {serviceProvider}</h2>");
    htmlBody.AppendLine("<p>The most recent MUBU Usage file is older than the threshold:</p>");
    htmlBody.AppendLine("<table border='1'>");
    htmlBody.AppendLine("<tr><th>File Name</th><th>Write Time UTC</th><th>Days Old</th></tr>");
    
    var daysOld = (DateTime.Now - mostRecentFile.WriteTime).Days;
    htmlBody.AppendLine($"<tr><td>{mostRecentFile.FileName}</td><td>{mostRecentFile.WriteTime}</td><td>{daysOld}</td></tr>");
    
    htmlBody.AppendLine("</table>");
    htmlBody.AppendLine($"<p>Threshold: {FtpReportNotificationThresholdDays} days</p>");
    
    bodyBuilder.HtmlBody = htmlBody.ToString();
    return bodyBuilder;
}
```

### Notification Queue Management
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
        {"ServiceProviderId", new MessageAttributeValue {DataType = "Number", StringValue = serviceProviderId.ToString()}},
        {"MaxRetries", new MessageAttributeValue {DataType = "Number", StringValue = DeviceCleanupMaxRetries}}
    };
    
    var sendMessageRequest = new SendMessageRequest
    {
        MessageAttributes = messageAttributes,
        MessageBody = "Sending device sync cleanup/notification message",
        QueueUrl = DeviceNotificationQueueURL
    };
    
    using (var sqsClient = new AmazonSQSClient(AwsCredentials(context)))
    {
        await sqsClient.SendMessageAsync(sendMessageRequest);
    }
}
```

### Notification Database Tracking
```csharp
private static void InsertTelegenceUsageQueueRecord(string fan, int serviceProviderId, string deviceNotificationQueueURL, SqlConnection conn)
{
    using (var cmd = new SqlCommand(
        "INSERT INTO [dbo].[TelegenceDeviceUsageIdsToProcess](DeviceNotificationQueueURL,ServiceProviderId,RetryCount,FoundationAccountNumber) VALUES(@DeviceNotificationQueueURL, @ServiceProviderId, 0, @FoundationAccountNumber)",
        conn))
    {
        cmd.Parameters.AddWithValue("@DeviceNotificationQueueURL", deviceNotificationQueueURL);
        cmd.Parameters.AddWithValue("@ServiceProviderId", serviceProviderId);
        cmd.Parameters.AddWithValue("@FoundationAccountNumber", fan);
        cmd.ExecuteNonQuery();
    }
}
```

## Summary

This Lambda function implements a comprehensive device usage processing system with:

1. **Robust Continuation Mechanics** using `IsDownloadNextInstance` and related attributes for multi-run processing
2. **Sophisticated MUBU Multi-Step Sync** with `TelegenceSyncDataStep` chaining for complex data processing workflows
3. **Intelligent Blank File Handling** with automatic detection, database tracking, and notification systems
4. **Thorough Initialization Cleanup** including FTP cleanup, staging table truncation, and queue record management
5. **Advanced Notification System** with threshold-based alerts for stale files, blank file notifications, and comprehensive email reporting

The system ensures reliable, scalable processing of large datasets while maintaining data integrity and providing comprehensive monitoring and alerting capabilities.