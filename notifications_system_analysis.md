# Notifications System: Stale/Blank File Alerts and Threshold-Based Notifications

## Overview
The notification system provides comprehensive monitoring and alerting capabilities for detecting stale files, blank files, and threshold-based conditions that may indicate system issues in the MUBU (Mobile Usage Billing Unit) synchronization process.

## 1. Threshold-Based Configuration

### Environment Variables Setup
The system reads configuration values from environment variables to determine notification thresholds:

**Flow:**
1. The system retrieves the `FtpReportNotificationThresholdDays` environment variable and converts it to an integer
2. The system retrieves the `CheckFilesMissedThresholdDays` environment variable and converts it to an integer
3. These values establish the time boundaries for determining when files are considered "stale"

```csharp
private int FtpReportNotificationThresholdDays = Convert.ToInt32(Environment.GetEnvironmentVariable("FtpReportNotificationThresholdDays"));
private int CheckFilesMissedThresholdDays = Convert.ToInt32(Environment.GetEnvironmentVariable("CheckFilesMissedThresholdDays"));
```

## 2. Stale File Detection

### File Age Validation Logic
The system implements a threshold-based validation mechanism to determine if files are within acceptable age limits:

**Flow:**
1. The `IsFileWithinThreshold` method receives a `UsageFile` object as input
2. It compares the file's `WriteTime` against the current date minus the configured threshold days
3. If the file's write time is greater than or equal to the calculated threshold date, the file is considered "fresh"
4. If the file's write time is older than the threshold, the file is flagged as "stale"
5. The method returns a boolean indicating whether the file passes the freshness test

```csharp
private bool IsFileWithinThreshold(UsageFile usageFile)
{
    return usageFile.WriteTime >= DateTime.Now.AddDays(FtpReportNotificationThresholdDays * -1);
}
```

### Stale Usage Detection Implementation
The system performs comprehensive checks for both voice and usage data staleness:

**Voice Usage Stale Detection Flow:**
1. The system checks if the latest MUBU voice file exists (`latestMubuVoice != null`)
2. It validates the file's freshness using the `IsFileWithinThreshold` method
3. If the file fails the threshold check (is stale), the system logs a warning message with the file's last write time
4. The system constructs a notification subject line indicating "Stale MUBU Voice Sync" with the service provider name
5. It builds a detailed notification body using the `BuildStaleMubuVoiceSyncNotificationBody` method
6. The notification is prepared for sending (actual sending logic would be implemented separately)

**Usage Data Stale Detection Flow:**
1. The system checks if the latest MUBU usage file exists (`latestMubuUsage != null`)
2. It validates the file's freshness using the `IsFileWithinThreshold` method
3. If the file fails the threshold check (is stale), the system logs a warning message with the file's last write time
4. The system constructs a notification subject line indicating "Stale MUBU Usage Sync" with the service provider name
5. It builds a detailed notification body using the `BuildStaleMubuUsageSyncNotificationBody` method
6. The notification is prepared for sending (actual sending logic would be implemented separately)

## 3. Stale File Notification Bodies

### Stale Voice Notification Construction
The system creates HTML-formatted notification messages for stale voice synchronization alerts:

**Flow:**
1. The `BuildStaleMubuVoiceSyncNotificationBody` method receives the service provider name and the most recent file information
2. It creates a new `BodyBuilder` instance for constructing the email body
3. The method formats an HTML message with a header indicating "Stale MUBU Voice Sync Alert" and the service provider name
4. The body content explains that the MUBU voice report has not been delivered to FTP since the specified date
5. It warns that AMOP voice metrics may be stale until FTP delivery resumes
6. The completed `BodyBuilder` object is returned for use in email notifications

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

### Stale Usage Notification Construction
The system creates HTML-formatted notification messages for stale usage synchronization alerts:

**Flow:**
1. The `BuildStaleMubuUsageSyncNotificationBody` method receives the service provider name and the most recent file information
2. It creates a new `BodyBuilder` instance for constructing the email body
3. The method formats an HTML message with a header indicating "Stale MUBU Usage Sync Alert" and the service provider name
4. The body content explains that the MUBU usage report has not been delivered to FTP since the specified date
5. It warns that AMOP usage metrics may be stale until FTP delivery resumes
6. The completed `BodyBuilder` object is returned for use in email notifications

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

## 4. Notification Queue Integration

### Device Notification Queue Processing
The system integrates with AWS SQS (Simple Queue Service) to handle notification message distribution:

**Flow:**
1. The `SendNotificationMessageToQueueAsync` method is called with a context and service provider ID
2. The system logs the method entry and the configured Device Notification Queue URL
3. It performs a validation check to ensure the `DeviceNotificationQueueURL` is configured and not empty
4. If the queue URL is missing, the system logs a warning and exits the method early
5. The system constructs message attributes including the Service Provider ID and maximum retry count
6. It creates a `SendMessageRequest` object with the message attributes, body text, and queue URL
7. The system establishes a connection to AWS SQS using the configured credentials
8. The message is sent asynchronously to the notification queue for processing
9. The SQS client connection is properly disposed of after the operation completes

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

## Key Features Summary

1. **Configurable Thresholds**: The system uses environment variables to set flexible notification thresholds
2. **Dual Monitoring**: Separate monitoring for voice and usage data synchronization
3. **Comprehensive Logging**: Detailed logging for troubleshooting and audit purposes
4. **HTML Notifications**: Rich HTML-formatted email notifications with clear messaging
5. **Queue Integration**: Asynchronous message processing through AWS SQS
6. **Error Handling**: Graceful handling of missing configurations and connection issues
7. **Scalable Architecture**: Support for multiple service providers and retry mechanisms

## Business Impact

- **Proactive Monitoring**: Early detection of synchronization issues before they impact business operations
- **Automated Alerting**: Reduces manual monitoring overhead and ensures timely response to issues
- **Data Quality Assurance**: Helps maintain the integrity of AMOP voice and usage metrics
- **Operational Visibility**: Provides clear insights into FTP delivery status and potential bottlenecks