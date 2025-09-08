# Altaworx Telegence AWS Get Device Usage Lambda Flow Documentation

## Overview
This document provides a comprehensive analysis of the AltaworxTelegenceAWSGetDeviceUsage.cs Lambda function and its supporting classes, detailing the complete flow from initialization to completion.

## High-Level Sequential Flow

### 1. Lambda Entry Point
**Function**: `FunctionHandler(SQSEvent sqsEvent, ILambdaContext context)`

**Flow:**
1. Initialize KeySysLambdaContext using `BaseFunctionHandler()`
2. Load environment variables and configuration
3. Set default values for missing configurations
4. Call `ProcessEventAsync()` to handle the main processing logic
5. Handle exceptions and perform cleanup via `CleanUp()`

### 2. Event Processing
**Function**: `ProcessEventAsync(KeySysLambdaContext context, SQSEvent sqsEvent)`

**Decision Flow:**
- **If SQS Records exist**: Process each record via `ProcessEventRecordAsync()`
- **If No SQS Records**: Initialize daily processing via `StartDailyDeviceUsageProcessingAsync()`

### 3. Record Processing
**Function**: `ProcessEventRecordAsync(KeySysLambdaContext context, SQSEvent.SQSMessage message)`

**Message Attribute Extraction:**
- InitializeProcessing (boolean)
- ServiceProviderId (integer)
- FAN (Foundation Account Number)
- ReportType (Premier/MUBU/Final)
- IsFromCloudwatchEvent (boolean)
- IsDownLoadFileAgain (boolean)
- IsDownloadNextInstance (boolean)
- WriteTimesNextDownload (comma-separated list)
- FileNamesNextDownload (comma-separated list)
- DownloadFailedIds (comma-separated list)
- TelegenceSyncDataStep (enumeration)

**Processing Logic:**
- **If InitializeProcessing = true**: Call `StartDailyDeviceUsageProcessingAsync()`
- **If MUBU report with sync step**: Call `ProcessTelegenceMubuUsageDataSync()`
- **Otherwise**: Call `ProcessDailyUsage()` with appropriate parameters

## Detailed Method Flows

### A. Daily Usage Processing Flow

#### `ProcessDailyUsage(KeySysLambdaContext context, int serviceProviderId, string fan, string reportType, ...)`

**1. Settings Retrieval:**
- Call `context.SettingsRepo.GetTelegenceDeviceSettings(serviceProviderId)`
- Extract FTP credentials and decode password using `context.Base64Service.Base64Decode()`

**2. Processing Branch Selection:**
- **Download Failed Files**: `ProcessDownloadFileAgain()`
- **Download Next Instance**: `ProcessDownloadFileNextInstance()`
- **Normal Processing**: Main `ProcessDailyUsage()` method

**3. FTP Cleanup:**
- Call `CleanUpFtp()` for main path and MUBU path
- Call `Dequeue()` to remove queue record

#### `ProcessDailyUsage(KeySysLambdaContext context, int serviceProviderId, string fan, string reportType, string username, string password, string server, string path, string mubuPath, string finalUsagePath, bool isFromCloudwatchEvent)`

**Report Type Processing:**

**Premier Reports (`reportType == REPORT_TYPE_PREMIER` or empty):**
1. Call `GetLatestUsage()` to download and process Premier usage files
2. Perform `SqlBulkCopy()` to load data into `TelegenceAllUsageStaging` table

**MUBU Reports (`reportType == REPORT_TYPE_MUBU`):**
1. Initialize file tracking data structures
2. Get failed download files via `GetFileNamesDownloadFailed()`
3. Get previously downloaded files via `GetFilesDownLoaded()`
4. Establish SFTP connection
5. Process Voice Files:
   - Call `GetLatestMubuVoiceFileList()` to identify new files
   - Call `GetLatestMubuVoice()` for first file
   - Perform `SqlBulkCopy()` to `TelegenceDeviceUsageMubuStaging`
6. Process Data Files:
   - Call `GetLatestMubuUsageFileList()` to identify new files
   - Call `GetLatestMubuUsage()` for first file
   - Perform `SqlBulkCopy()` to `TelegenceDeviceUsageMubuStaging`
7. Handle file download failures via `SqlBulkCopy()` to `TelegenceSFTPFileDownloadStatus`
8. Queue management:
   - Send next download messages via `SendMessageToQueueNextDownloadAsync()`
   - Send retry messages via `SendMessageToQueueDownloadAgainAsync()`
   - Send processing messages via `SendProcessMessageToQueueAsync()`
9. Handle blank file notifications via `BuildNotifyDownLoadFileBlank()`

**Final Usage Reports (`reportType == REPORT_TYPE_FINAL`):**
1. Call `GetLatestFinalUsage()` to download final usage files
2. Extract billing period information from filename
3. Perform `SqlBulkCopy()` to `TelegenceDeviceFinalUsageStaging`
4. Call `UpdateTelegenceFinalUsageFromStaging()` to process staged data

### B. MUBU Data Synchronization Flow

#### `ProcessTelegenceMubuUsageDataSync(KeySysLambdaContext context, int serviceProviderId, int telegenceSyncDataStep)`

**Step-by-Step Processing:**

**Step 1: UpdateTelegenceMubuUsageFromStaging**
1. Execute `UpdateTelegenceMubuUsageFromStaging()`
2. Send message for next step via `SendProcessMessageToQueueAsync()`

**Step 2: UpdateMobilityMubuUsageFromTelegence**
1. Execute `UpdateMobilityMubuUsageFromTelegence()`
2. Truncate staging table via `TruncateTableByTableName()`
3. Send message for next step

**Step 3: UpdateLateMubuUsageFromTelegence**
1. Execute `UpdateLateMubuUsageFromTelegence()`
2. Truncate staging table

### C. Daily Processing Initialization Flow

#### `StartDailyDeviceUsageProcessingAsync(KeySysLambdaContext context, bool fromCloudwatchEvent)`

**Initialization Steps:**
1. Call `InitializeSync()` to truncate staging tables and initialize sync tracking
2. Call `ClearQueue()` to clear processing queue
3. Get first service provider via `ServiceProviderCommon.GetNextServiceProviderId()`

**Provider Processing Loop:**
For each service provider:
1. Call `AddProviderToQueueAsync()` to add provider to processing queue
2. If not from CloudWatch: Send notification via `SendNotificationMessageToQueueAsync()`
3. Get next provider and repeat

#### `AddProviderToQueueAsync(KeySysLambdaContext context, string dbConnectionString, int serviceProviderId, string deviceNotificationQueueURL, bool fromCloudwatchEvent)`

**Provider Setup:**
1. Get Telegence authentication via `TelegenceCommon.GetTelegenceAuthenticationInformation()`
2. Get provider settings via `context.SettingsRepo.GetTelegenceDeviceSettings()`
3. Calculate billing period information using `BillingPeriodHelper.GetBillingPeriodForServiceProvider()`

**Usage Zeroing Logic:**
- If current day equals billing period end day + 1: Call `ZeroOutUsage()`
- If in premiere report delay period: Continue zeroing usage

**FAN Processing:**
1. Get Foundation Account Numbers via `GetFoundationAccountList()`
2. Establish SFTP connection
3. For each FAN:
   - Get latest usage file via `GetLatestUsageFile()`
   - Check if recent via `HasRecentUsage()`
   - Insert queue record via `InsertTelegenceUsageQueueRecord()`
   - Send process message via `SendProcessMessageToQueueAsync()`

**MUBU Processing:**
1. Check MUBU voice files via `GetLatestMubuVoiceFile()`
2. Check MUBU usage files via `GetLatestMubuUsageFile()`
3. Send email notifications for stale files via `SendEmailAsync()`
4. Queue MUBU processing if files are current

**Final Usage Processing:**
1. Check final usage files via `GetLatestFinalUsageFile()`
2. Extract billing period from filename
3. Check if billing period is open via `HasOpenBillingPeriod()`
4. Queue final usage processing if applicable

**Kafka Processing:**
- If Kafka settings configured: Execute Kafka usage updates

## Supporting Class Method Details

### AwsFunctionBase.cs Methods

**Logging and Context:**
- `LogInfo()` - Centralized logging with caller information
- `BaseFunctionHandler()` - Initialize KeySysLambdaContext
- `BaseAmopFunctionHandler()` - Initialize AmopLambdaContext
- `CleanUp()` - Context cleanup

**Database Operations:**
- `GetCustomerName()` - Retrieve customer name by ID
- `GetInstance()` - Get optimization instance details
- `GetQueue()` - Get optimization queue details
- `GetSimCardCount()` - Count SIM cards in Jasper database
- `GetBatchedJasperSimCardCountByServiceProviderId()` - Get batched device counts
- `GetCommGroups()` - Get communication groups
- `SqlBulkCopy()` - Bulk copy data to SQL tables

**AWS Operations:**
- `AwsCredentials()` - Get AWS credentials
- `AwsSesCredentials()` - Get AWS SES credentials

**Utility Methods:**
- `GetStringValueFromEnvironmentVariable()` - Environment variable retrieval
- `GetLongValueFromEnvironmentVariable()` - Long value from environment
- `GetBooleanValueFromEnvironmentVariable()` - Boolean value from environment
- `GetIntValueFromEnvironmentVariable()` - Integer value from environment
- `GetFANFilter()` - Get FAN inclusion/exclusion filters

### BillingPeriodHelper.cs Methods

**Billing Period Operations:**
- `GetBillingPeriodForServiceProvider()` - Get billing period with timezone
- `GetBillingPeriodCurrentMonth()` - Get current month billing period
- `GetBillingPeriodForServiceProviderByCurrentDate()` - Get billing period by current date

### ServiceProviderCommon.cs Methods

**Service Provider Operations:**
- `GetNextServiceProviderId()` - Get next provider ID by integration type
- `GetServiceProvider()` - Get service provider by ID
- `GetServiceProviders()` - Get all service providers
- `GetServiceProviderByName()` - Get service provider by name

### SettingsRepository.cs Methods

**Settings Retrieval:**
- `GetGeneralProviderSettings()` - Get general provider configuration
- `GetOptimizationSettings()` - Get optimization settings by tenant
- `GetJasperDeviceSettings()` - Get Jasper-specific settings
- `GetTelegenceDeviceSettings()` - Get Telegence-specific settings
- `GetEbondingDeviceSettings()` - Get eBonding settings
- `GetPondDeviceSettings()` - Get Pond settings

**Internal Helpers:**
- `SettingFromReader()` - Map database reader to setting object
- `OptimizationSettingFromReader()` - Map optimization settings
- `MapToOptimizationSettingsModel()` - Convert settings list to model
- `ReadGetPondDeviceSettings()` - Read Pond device settings

### SqlQueryHelper.cs Methods

**Stored Procedure Execution:**
- `ExecuteStoredProcedureWithListResult<T>()` - Execute SP returning list
- `ExecuteStoredProcedureWithRowCountResult()` - Execute SP returning row count
- `ExecuteStoredProcedureWithIntResult()` - Execute SP returning integer
- `ExecuteStoredProcedureWithSingleValueResult<T>()` - Execute SP returning single value

**Parameter Management:**
- `CloneParameters()` - Clone SQL parameter list

**Validation:**
- `CheckAllExecuteStoredProcedureParameters()` - Validate SP parameters
- `CheckValidStringParameter()` - Validate string parameters
- `CheckValidObjectParameter()` - Validate object parameters

### TelegenceCommon.cs Methods

**Authentication and Account Management:**
- `GetTelegenceAuthenticationInformation()` - Get auth info by provider
- `GetTelegenceBillingAccounts()` - Get billing accounts by provider

**Device Management:**
- `UpdateTelegenceDeviceStatus()` - Update device activation status
- `UpdateTelegenceSubscriber()` - Update subscriber information
- `UpdateTelegenceMobilityConfiguration()` - Update mobility configuration

**Data Retrieval:**
- `GetTelegenceDeviceBySubscriberNumber()` - Get device by subscriber number
- `TelegenceGetDetailDataUsage()` - Get detailed data usage
- `GetTelegenceDevicesAsync()` - Get devices asynchronously with pagination
- `GetBanStatusAsync()` - Get billing account status

**Internal HTTP Operations:**
- `GetTelegenceDeviceBySubscriberNumberByProxy()` - Proxy-based device retrieval
- `GetTelegenceDeviceBySubscriberNumberWithoutProxy()` - Direct device retrieval
- `GetTelegenceDevicesAsyncByProxy()` - Proxy-based devices retrieval
- `GetTelegenceDevicesAsyncWithoutProxy()` - Direct devices retrieval
- `GetBanStatusAsyncByProxy()` - Proxy-based BAN status
- `GetBanStatusAsyncWithoutProxy()` - Direct BAN status

**Utility Methods:**
- `BuildRequestHeaders()` - Build HTTP request headers
- `BuildHeaderContent()` - Build header content for proxy
- `BuildPayloadModel()` - Build payload for proxy requests
- `ConfigHttpClient()` - Configure HTTP client settings
- `MappingProxyResponseContent()` - Map proxy response
- `GetBillingAccountStatus()` - Extract status from response
- `GetTelegenceDeviceList()` - Process device list with timestamps

### TenantRepository.cs Methods

**Tenant Operations:**
- `GetPortalImageByTenantId()` - Get portal image by tenant
- `GetTenantNameByTenantId()` - Get tenant name by ID
- `GetTenantIdByServiceProviderId()` - Get tenant ID by service provider
- `GetCustomObjectById()` - Get custom object by tenant and object ID

**Internal Operations:**
- `ReadCustomObject()` - Read custom object from data reader

## Data Flow Summary

### 1. Initialization Phase
- Lambda receives SQS event or CloudWatch trigger
- Environment variables and settings loaded
- Database connections established

### 2. Provider Discovery Phase
- Service providers with Telegence integration identified
- Provider-specific settings and authentication retrieved
- Billing periods and FAN lists obtained

### 3. File Discovery Phase
- SFTP connections established to Telegence servers
- Latest files identified for each report type (Premier, MUBU, Final)
- File timestamps validated for recency

### 4. File Processing Phase
- Files downloaded and processed based on type
- Data extracted and validated
- Bulk loading to staging tables performed

### 5. Data Synchronization Phase
- Staged data processed through stored procedures
- Main device tables updated
- Late records and mobility data synchronized

### 6. Cleanup and Notification Phase
- Old files cleaned up from SFTP servers
- Processing queues cleared
- Notification emails sent for issues
- Lambda execution completed

## Error Handling and Retry Logic

### SQL Retry Policy
- Transient SQL exceptions handled with retry logic
- Maximum 3 retries with 5-second delays
- Timeout exceptions included in retry scope

### File Download Failures
- Failed downloads tracked in `TelegenceSFTPFileDownloadStatus` table
- Retry messages sent to SQS for failed downloads
- Progressive processing of large files with continuation support

### Email Notifications
- Stale file notifications sent to configured recipients
- Blank file notifications for empty MUBU files
- Error notifications for processing failures

## Configuration Dependencies

### Environment Variables
- TelegenceDeviceUsageQueueURL
- TelegenceDeviceNotificationQueueURL
- DeviceCleanupMaxRetries
- DaysToKeep
- FtpReportNotificationThresholdDays
- CheckFilesMissedThresholdDays
- LimitAmountFilePerRunTimes
- PremiereReportDelayDays
- IsPremiereReportDelaySimulator
- DayEndBillingSimulator
- MUBURowsCountLimit

### Database Settings
- Telegence FTP server credentials and paths
- AWS access keys and SES credentials
- Jasper database connection strings
- Kafka configuration parameters
- Email notification settings

This comprehensive flow documentation covers all major methods and their interactions within the Telegence device usage processing system.