# AltaworxTelegenceAWSGetDeviceUsage Lambda Function - Detailed Analysis

## Overview

The `AltaworxTelegenceAWSGetDeviceUsage` Lambda function is responsible for processing AT&T Telegence device usage data through various report types (Premier, MUBU, Final) and managing device synchronization with the Telegence API.

---

## 1. Trigger Mechanisms

### SQS Event Sources

The Lambda function is triggered by **SQS events** with the following sources:

```csharp
public async Task FunctionHandler(SQSEvent sqsEvent, ILambdaContext context)
```

**Code Evidence**: Lines 65-105 in `AltaworxTelegenceAWSGetDeviceUsage.cs`

### Trigger Types:

1. **CloudWatch Events** (Scheduled/Manual):
   - When `sqsEvent?.Records` is null, it triggers daily processing
   - **Code**: Lines 117-121
   ```csharp
   else
   {
       // queue all AT&T Telegence Providers
       await StartDailyDeviceUsageProcessingAsync(context, true);
   }
   ```

2. **SQS Message Processing**:
   - When SQS records are present, processes individual messages
   - **Code**: Lines 110-116
   ```csharp
   if (sqsEvent?.Records != null)
   {
       foreach (var sqsEventRecord in sqsEvent.Records)
       {
           await ProcessEventRecordAsync(context, sqsEventRecord);
       }
   }
   ```

3. **Message Attributes Trigger Logic**:
   - `InitializeProcessing`: Triggers provider queuing
   - `IsFromCloudwatchEvent`: Indicates scheduled execution
   - `TelegenceSyncDataStep`: Controls MUBU processing steps

**Code Evidence**: Lines 127-223 in message attribute parsing

---

## 2. Retry Initialization - SQL Retry Implementation

### Why SQL Retry is First Step

SQL retry is implemented as the very first step to protect against:
- **Transient SQL connection failures**
- **Database timeout issues**
- **Network connectivity problems**
- **SQL Server temporary unavailability**

**Code Evidence**: Lines 571-581
```csharp
private static RetryPolicy GetSqlRetryPolicy(KeySysLambdaContext context)
{
    var sqlTransientRetryPolicy = Policy
        .Handle<SqlException>(SqlServerTransientExceptionDetector.ShouldRetryOn)
        .Or<TimeoutException>()
        .WaitAndRetry(MaxRetries,
            retryAttempt => TimeSpan.FromSeconds(RetryDelaySeconds),
            (exception, timeSpan, retryCount, sqlContext) => LogInfo(context, "STATUS",
                $"Encountered transient SQL error - delaying for {timeSpan.TotalMilliseconds}ms, then making retry {retryCount}. Exception: {exception?.Message}"));
    return sqlTransientRetryPolicy;
}
```

### Retry Configuration:
- **Max Retries**: 3 attempts
- **Delay Strategy**: Fixed 5-second delay between retries
- **Exception Types**: `SqlException` and `TimeoutException`

**Code Evidence**: Lines 51-52
```csharp
private const int MaxRetries = 3;
private const int RetryDelaySeconds = 5;
```

---

## 3. Staging Table Clearing

### Automatic Clearing at Start

**YES**, staging tables are cleared at the start of each run through the `InitializeSync` method:

**Code Evidence**: Lines 1578-1600
```csharp
private static void InitializeSync(KeySysLambdaContext context, string dbConnectionString)
{
    LogInfo(context, "SUB", "InitializeSync");
    try
    {
        using (var Conn = new SqlConnection(context.CentralDbConnectionString))
        {
            using (var Cmd = new SqlCommand("usp_Telegence_Truncate_UsageStaging", Conn))
            {
                Cmd.CommandType = CommandType.StoredProcedure;
                Cmd.CommandTimeout = 800;
                Conn.Open();
                Cmd.ExecuteNonQuery();
                Conn.Close();
            }
        }
        // Additional initialization code...
    }
}
```

### Tables Cleared:
- **Primary**: Via `usp_Telegence_Truncate_UsageStaging` stored procedure
- **MUBU Staging**: `TelegenceDeviceUsageMubuStaging` (cleared after processing)
- **Device Staging**: Various device staging tables

### Why Not Auto-Cleared from Previous Run:
The staging tables are **not automatically cleared** when the previous day's run completes because:
1. **Data integrity** - Ensures clean start for new processing cycle
2. **Error recovery** - Previous incomplete runs might leave data
3. **Audit trail** - Staging data might be needed for debugging

**Code Evidence**: Lines 460, 466 showing explicit truncation
```csharp
TruncateTableByTableName(context, Amop.Core.Constants.DatabaseTableNames.TELEGENCE_DEVICE_USAGE_MUBU_STAGING);
```

---

## 4. BAN/FAN Status Storage

### Storage Tables

BAN, FAN, and Number statuses from Telegence API are **NOT** stored in `BillingAccountNumberStatusStaging`. Instead, they are:

1. **Retrieved in real-time** via API calls during processing
2. **Used immediately** for validation and processing
3. **Not persisted** in staging tables

### BAN Status Retrieval Process:

**Code Evidence**: Lines 472-494 in `TelegenceCommon.cs`
```csharp
public static async Task<string> GetBanStatusAsync(KeySysLambdaContext context, TelegenceAuthentication telegenceAuth, string proxyUrl, string ban, string telegenceBanDetailGetURL)
{
    AwsFunctionBase.LogInfo(context, LogTypeConstant.Info, string.Format(LogCommonStrings.GET_BAN_STATUS_BY_BAN_NUMBER, ban));
    var status = string.Empty;
    string banDetailUrl = telegenceBanDetailGetURL.Replace("{ban}", ban);
    // API call logic...
    return status;
}
```

### FAN List Source:
FAN lists are retrieved from the local database, not from Telegence staging tables:

**Code Evidence**: Lines 1923-1961
```csharp
private static List<string> GetFoundationAccountList(KeySysLambdaContext context, string dbConnectionString, int serviceProviderId)
{
    // Retrieves from TelegenceDevice table joined with MobilityDevice
    using (var cmd = new SqlCommand(
        @"SELECT DISTINCT td.FoundationAccountNumber
          FROM TelegenceDevice td
          LEFT JOIN MobilityDevice md on md.MSISDN = td.SubscriberNumber AND md.ServiceProviderId = td.ServiceProviderId
          WHERE td.ServiceProviderId = @ServiceProviderId
          AND td.IsDeleted = 0
          AND (md.IsDeleted = 0 OR md.IsDeleted IS NULL)", conn))
}
```

---

## 5. BAN List Retrieval in Normal Flow

In the "Normal Flow," BAN list statuses are retrieved **directly from the local database**, not from `BillingAccountNumberStatusStaging`.

### Retrieval Source:
**Code Evidence**: Lines 69-103 in `TelegenceCommon.cs`
```csharp
public static List<TelegenceBillingAccount> GetTelegenceBillingAccounts(string connectionString, int serviceProviderId)
{
    using (var Cmd = new SqlCommand("usp_Telegence_Get_BillingAccountsByProviderId", Conn))
    {
        Cmd.CommandType = CommandType.StoredProcedure;
        Cmd.Parameters.AddWithValue("@providerId", serviceProviderId);
        // Returns BillingAccountNumber and FoundationAccountNumber
    }
}
```

### Process Flow:
1. **Database Query** → Local `TelegenceDevice` and `MobilityDevice` tables
2. **API Validation** → Real-time status check via Telegence API
3. **Processing** → Immediate use without staging storage

---

## 6. API Details - Device Fetch

### Telegence API Endpoint

**Exact Endpoint**: The specific endpoint is determined dynamically but follows this pattern:
- **Base URL**: Retrieved from `TelegenceAuthentication` (Production/Sandbox)
- **Device Endpoint**: Passed as `deviceDetailEndpoint` parameter
- **Subscriber Endpoint**: `{endpoint}{subscriberNo}` format

**Code Evidence**: Lines 438-470 in `TelegenceCommon.cs`
```csharp
public static async Task<TelegenceGetDevicesSyncState> GetTelegenceDevicesAsync(KeySysLambdaContext context, TelegenceGetDevicesSyncState syncState, string proxyUrl,
   List<TelegenceDeviceResponse> telegenceDeviceList, string deviceDetailEndpoint, int pageSize)
{
    var deviceDetailRequestUrl = $"{baseUrl.AbsoluteUri.TrimEnd('/')}{deviceDetailEndpoint}";
    // API call implementation
}
```

### Page Size Configuration

**Page Size**: Configurable parameter passed to the method
- **Default**: Not specified in constants, passed as parameter
- **Header**: Added to HTTP request as `PAGE_SIZE`

**Code Evidence**: Lines 598-599
```csharp
client.DefaultRequestHeaders.Add(CommonConstants.CURRENT_PAGE, syncState.CurrentPage.ToString());
client.DefaultRequestHeaders.Add(CommonConstants.PAGE_SIZE, pageSize.ToString());
```

### Page Completion Detection

The system determines all pages are processed through:

**Code Evidence**: Lines 607-612
```csharp
if (int.TryParse(responseMessage.Headers.GetValues(CommonConstants.PAGE_TOTAL).FirstOrDefault(), out int pageTotal))
{
    syncState.HasMoreData = syncState.CurrentPage < pageTotal;
}
syncState.IsLastCycle = !syncState.HasMoreData;
```

**Detection Method**:
1. **API Response Header**: `PAGE_TOTAL` indicates total pages
2. **Comparison**: `CurrentPage < PageTotal`
3. **State Management**: `IsLastCycle` flag controls loop termination

---

## 7. Missing Devices Handling

### Subscriber-Level Validation API

**API Details**: `GetTelegenceDeviceBySubscriberNumber`

**Code Evidence**: Lines 358-380 in `TelegenceCommon.cs`
```csharp
public static async Task<string> GetTelegenceDeviceBySubscriberNumber(KeySysLambdaContext context, TelegenceAuthentication telegenceAuthentication,
    bool isProduction, string subscriberNo, string endpoint, string proxyUrl)
{
    var deviceDetailEndpoint = $"{endpoint}{subscriberNo}";
    // Validation logic
}
```

**Parameters Used**:
- **subscriberNo**: Phone number/MSISDN
- **endpoint**: Device detail API endpoint
- **proxyUrl**: Optional proxy configuration
- **Authentication**: Client ID and Secret in headers

### Failed Validation Handling

**What Happens to Failed Devices**:
1. **Logging**: Failed validations are logged with detailed error messages
2. **Return Empty**: Method returns empty string for failures
3. **Continue Processing**: System continues with next device
4. **No Retry**: Individual device failures don't trigger retries

**Code Evidence**: Lines 517-518, 539-540
```csharp
AwsFunctionBase.LogInfo(context, LogTypeConstant.Error, string.Format(LogCommonStrings.REQUEST_FAILED_RESPONSE_AT_FINAL_RETRIES, deviceDetailUrl, responseBody));
return string.Empty;
```

---

## 8. Error Handling / Retry Configuration

### Exponential Backoff using Polly

**Retry Configuration**: Uses Polly library for HTTP requests

**Code Evidence**: Lines 502, 524, 552, 592, 632, 654
```csharp
var responseMessage = await Amop.Core.Helpers.RetryPolicyHelper.PollyRetryForProxyRequestAsync(context.logger, CommonConstants.NUMBER_OF_TELEGENCE_RETRIES).ExecuteAsync(async () =>
```

**Configuration Details**:
- **Library**: Polly retry policy
- **Retry Count**: `CommonConstants.NUMBER_OF_TELEGENCE_RETRIES`
- **Strategy**: Configured in `RetryPolicyHelper` (exponential backoff)
- **Scope**: Applied to all Telegence API calls

### Re-enqueuing Messages

**Technical Implementation**: New SQS messages are created for incomplete/timed-out processing

**Code Evidence**: Lines 1963-2016 (SendProcessMessageToQueueAsync)
```csharp
private async Task SendProcessMessageToQueueAsync(KeySysLambdaContext context, int serviceProviderId, string fan, string reportType, bool fromCloudwatchEvent, int telegenceSyncDataStep = (int)TelegenceSyncDataStepEnum.None, int delay = DefaultDelaySQS)
{
    var request = new SendMessageRequest
    {
        DelaySeconds = (int)TimeSpan.FromSeconds(delay).TotalSeconds,
        MessageAttributes = new Dictionary<string, MessageAttributeValue>
        {
            {"InitializeProcessing", new MessageAttributeValue {DataType = "String", StringValue = false.ToString()}},
            {"ServiceProviderId", new MessageAttributeValue {DataType = "String", StringValue = serviceProviderId.ToString()}},
            // Additional attributes...
        },
        QueueUrl = ExportDeviceUsageQueueURL
    };
}
```

**Re-enqueuing Scenarios**:
1. **File Download Failures**: `SendMessageToQueueDownloadAgainAsync`
2. **Next Instance Downloads**: `SendMessageToQueueNextDownloadAsync`
3. **Processing Continuation**: `SendProcessMessageToQueueAsync`

---

## 9. Business Rules Details

### Premier Report Delay Logic

**Business Rule**: Skip Premier report downloads in first N days of billing cycle

**Code Evidence**: Lines 1800-1804
```csharp
if (now.Day >= billPeriodEndDay && now.Day <= billPeriodEndDay + (PremiereReportDelayDays - 1))
{
    context.logger.LogInfo("INFO", $"Skipping Premiere Report Download in First {PremiereReportDelayDays} Days of the Billing Cycle");
}
```

**Configuration**:
- **Environment Variable**: `PremiereReportDelayDays`
- **Purpose**: Avoid processing incomplete billing data

### MUBU Row Count Limits

**Business Rule**: Limit MUBU processing based on row count

**Code Evidence**: Lines 54-55
```csharp
private long MUBURowsCountLimit = (long)Convert.ToDouble(Environment.GetEnvironmentVariable("MUBURowsCountLimit"));
private const long DefaultMUBURowsCountLimit = 200000;
```

### File Processing Limits

**Business Rule**: Limit number of files processed per run

**Code Evidence**: Lines 46, 300
```csharp
private int LimitAmountFilePerRunTimes = Convert.ToInt32(Environment.GetEnvironmentVariable("LimitAmountFilePerRunTimes"));
var limitFilesDownloadNumber = LimitAmountFilePerRunTimes - fileNameNeedDownloadFailed.Count;
```

---

## 10. Stored Procedures Function

### Key Stored Procedures:

1. **`usp_Telegence_Truncate_UsageStaging`**
   - **Function**: Clears all usage staging tables
   - **When**: At initialization of daily processing
   - **Code**: Lines 1585-1592

2. **`usp_Telegence_Update_DeviceFinalUsage_FromStaging`**
   - **Function**: Moves final usage data from staging to production tables
   - **Parameters**: ServiceProviderId, BillingPeriodYear, BillingPeriodMonth
   - **Code**: Lines 1640-1650

3. **`usp_Telegence_Update_DeviceMubuUsage_FromStaging`**
   - **Function**: Processes MUBU usage data from staging
   - **When**: After MUBU file download completion
   - **Code**: Lines 1660-1670

4. **`usp_Telegence_DeviceSync`**
   - **Function**: Synchronizes device data between Telegence and Mobility tables
   - **When**: During MUBU processing workflow
   - **Code**: Lines 1695-1705

5. **`usp_MobilityDeviceUsage_UpdateLateRecords`**
   - **Function**: Updates late-arriving usage records
   - **When**: Final step of MUBU processing
   - **Code**: Lines 1713-1720

6. **`usp_Telegence_Get_AuthenticationByProviderId`**
   - **Function**: Retrieves API authentication details
   - **Returns**: ClientId, ClientSecret, URLs, BillPeriodEndDay
   - **Code**: Lines 32-58 in `TelegenceCommon.cs`

7. **`usp_Telegence_Get_BillingAccountsByProviderId`**
   - **Function**: Gets billing account mappings
   - **Returns**: FoundationAccountNumber, BillingAccountNumber pairs
   - **Code**: Lines 76-94 in `TelegenceCommon.cs`

---

## 11. Summary Logs Details

### Log Categories and Information:

1. **Initialization Logs**:
   ```
   SUB: StartDailyDeviceUsageProcessingAsync
   SUB: InitializeSync
   SUB: ProcessEventAsync
   ```

2. **Processing Status Logs**:
   ```
   STATUS: Usage SQL Bulk Copy Start
   STATUS: MUBU Voice SQL Bulk Copy Start  
   STATUS: Final Usage SQL Bulk Copy Start
   ```

3. **API Interaction Logs**:
   ```
   INFO: GetTelegenceDevicesAsync: {proxyUrl}, {currentPage}
   INFO: REQUEST_GET_DEVICE_DETAIL: {deviceDetailUrl}
   INFO: REQUEST_URL_SUCCESS: {url}
   ```

4. **Error and Warning Logs**:
   ```
   WARNING: Stale usage for FAN {fan} - last write time: {writeTime}
   WARNING: MUBU report will not be loaded. No MUBU Path specified
   EXCEPTION: {exceptionMessage} {stackTrace}
   ```

5. **Configuration Logs**:
   ```
   InitializeProcessing: {value}
   ServiceProviderId: {id}
   ReportType: {type}
   TelegenceSyncDataStep: {stepName}
   ```

6. **Queue Management Logs**:
   ```
   Sending message for: {requestMsgBody} to DeviceUsage queue: {queueUrl}
   RESPONSE STATUS: {httpStatusCode}
   MessageBody: {messageBody}
   ```

---

## 12. Reference Items Usage

### Functions:
- **`GetTelegenceDevicesAsync`**: Paginated device retrieval from Telegence API
- **`GetTelegenceDeviceBySubscriberNumber`**: Individual device validation
- **`GetBanStatusAsync`**: Real-time BAN status checking
- **`UpdateTelegenceDeviceStatus`**: Device status updates to Telegence

### Queues:
- **`TelegenceDeviceUsageQueueURL`**: Main processing queue for usage reports
- **`TelegenceDeviceNotificationQueueURL`**: Cleanup and notification queue

### Stored Procedures:
- **Staging Management**: `usp_Telegence_Truncate_UsageStaging`
- **Data Processing**: `usp_Telegence_Update_*` series
- **Authentication**: `usp_Telegence_Get_AuthenticationByProviderId`
- **Account Mapping**: `usp_Telegence_Get_BillingAccountsByProviderId`

### Tables:
- **Staging Tables**: `TelegenceAllUsageStaging`, `TelegenceDeviceUsageMubuStaging`, `TelegenceDeviceFinalUsageStaging`
- **Production Tables**: `TelegenceDevice`, `MobilityDevice`
- **Queue Management**: `TelegenceDeviceUsageIdsToProcess`
- **File Tracking**: `TelegenceSFTPFileDownloadStatus`

---

## Architecture Flow Summary

1. **Trigger** → SQS Event or CloudWatch Schedule
2. **Initialize** → Clear staging tables, setup SQL retry policies
3. **Provider Loop** → Process each AT&T Telegence service provider
4. **Report Processing** → Handle Premier, MUBU, and Final reports
5. **API Integration** → Real-time device and BAN status validation
6. **Data Movement** → Staging → Production tables via stored procedures
7. **Queue Management** → Re-enqueue for continuation and cleanup
8. **Notification** → Email alerts for stale data and processing issues

This Lambda function serves as the central orchestrator for AT&T Telegence data processing, handling complex workflows with robust error handling and retry mechanisms.