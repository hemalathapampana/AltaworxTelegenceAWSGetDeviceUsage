# AltaworxTelegenceAWSGetDeviceUsage Lambda Function Flow Documentation

## Overview
The AltaworxTelegenceAWSGetDeviceUsage Lambda function is responsible for processing device usage data from Telegence service provider. It handles three types of reports: Premier, MUBU, and Final usage reports through FTP/SFTP downloads and processes them into staging tables for further processing.

## High-Level Architecture Flow

### 1. Entry Point: FunctionHandler
**Main Lambda Entry Point**
- Inherits from `AwsFunctionBase`
- Processes SQS events or CloudWatch events
- Initializes context using `BaseFunctionHandler()`
- Calls `ProcessEventAsync()` for event processing
- Performs cleanup using `CleanUp()`

### 2. Core Processing Flow

```
FunctionHandler → ProcessEventAsync → ProcessEventRecordAsync → ProcessDailyUsage
                                   ↓
                            StartDailyDeviceUsageProcessingAsync (if initialization)
```

## Sequential Function Flow

### Phase 1: Initialization and Context Setup

1. **FunctionHandler** (Entry Point)
   - Creates `KeySysLambdaContext` using `AwsFunctionBase.BaseFunctionHandler()`
   - Loads environment variables
   - Calls `ProcessEventAsync()`

2. **ProcessEventAsync**
   - Checks if SQS event has records
   - If records exist: processes each via `ProcessEventRecordAsync()`
   - If no records: calls `StartDailyDeviceUsageProcessingAsync()` for initialization

### Phase 2: Event Record Processing

3. **ProcessEventRecordAsync**
   - Extracts message attributes (ServiceProviderId, FAN, ReportType, etc.)
   - Routes to appropriate processing:
     - Initialization: `StartDailyDeviceUsageProcessingAsync()`
     - MUBU data sync: `ProcessTelegenceMubuUsageDataSync()`
     - Regular processing: `ProcessDailyUsage()`

### Phase 3: Daily Usage Processing

4. **ProcessDailyUsage** (Main Processing)
   - Gets Telegence device settings using `SettingsRepository.GetTelegenceDeviceSettings()`
   - Decodes FTP password using `Base64Service.Base64Decode()`
   - Routes based on processing type:
     - Download failed files: `ProcessDownloadFileAgain()`
     - Download next instance: `ProcessDownloadFileNextInstance()`
     - Regular processing: `ProcessDailyUsage()` (overloaded method)
   - Performs FTP cleanup using `CleanUpFtp()`
   - Dequeues record using `Dequeue()`

## Detailed Low-Level Flows

### A. Premier Report Processing Flow

**Method Chain:** `ProcessDailyUsage() → GetLatestUsage() → SqlBulkCopy()`

**Steps:**
1. **GetLatestUsage()**
   - Establishes SFTP connection using `SftpClient`
   - Retrieves latest Premier usage files
   - Downloads and parses CSV files using `PremierReportReader`
   - Returns `DataTable` with usage data

2. **Data Loading**
   - Uses `AwsFunctionBase.SqlBulkCopy()` to load data into `TelegenceAllUsageStaging` table
   - Bulk insert operation for performance

### B. MUBU Report Processing Flow

**Method Chain:** `ProcessDailyUsage() → GetLatestMubuVoiceFileList() + GetLatestMubuUsageFileList() → GetLatestMubuVoice() + GetLatestMubuUsage() → SqlBulkCopy()`

**Steps:**
1. **File Discovery**
   - `GetLatestMubuVoiceFileList()`: Gets voice files from SFTP
   - `GetLatestMubuUsageFileList()`: Gets data usage files from SFTP
   - Compares with `GetFilesDownLoaded()` to avoid reprocessing

2. **Voice File Processing**
   - `GetLatestMubuVoice()` → `DownloadMubuFile()` → `InsertMUBURecords()`
   - Downloads voice usage files
   - Parses using `MubuReportReader`
   - Bulk loads into `TelegenceDeviceUsageMubuStaging`

3. **Data File Processing**
   - `GetLatestMubuUsage()` → Similar process for data usage files
   - Uses same staging table with different column mappings

4. **Queue Management**
   - `SendMessageToQueueNextDownloadAsync()`: Queues remaining files
   - `SendMessageToQueueDownloadAgainAsync()`: Retries failed downloads
   - `BuildNotifyDownLoadFileBlank()`: Handles empty file notifications

### C. Final Usage Report Processing Flow

**Method Chain:** `ProcessDailyUsage() → GetLatestFinalUsage() → SqlBulkCopy() → UpdateTelegenceFinalUsageFromStaging()`

**Steps:**
1. **GetLatestFinalUsage()**
   - Downloads final usage reports from SFTP
   - Parses billing period from filename
   - Returns processed `DataTable`

2. **Data Processing**
   - Bulk loads into `TelegenceDeviceFinalUsageStaging`
   - Calls `UpdateTelegenceFinalUsageFromStaging()` stored procedure
   - Moves data from staging to final tables

### D. MUBU Data Synchronization Flow

**Method Chain:** `ProcessTelegenceMubuUsageDataSync() → UpdateTelegenceMubuUsageFromStaging() → UpdateMobilityMubuUsageFromTelegence() → UpdateLateMubuUsageFromTelegence()`

**Steps:**
1. **UpdateTelegenceMubuUsageFromStaging()**
   - Executes `usp_Telegence_Update_DeviceMubuUsage_FromStaging`
   - Moves data from staging to main tables
   - Queues next step: `UpdateMobilityMubuUsageFromTelegence`

2. **UpdateMobilityMubuUsageFromTelegence()**
   - Executes `usp_Telegence_DeviceSync`
   - Synchronizes mobility usage data
   - Truncates staging table
   - Queues next step: `UpdateLateMubuUsageFromTelegence`

3. **UpdateLateMubuUsageFromTelegence()**
   - Handles late-arriving MUBU usage data
   - Final cleanup of staging tables

## Internal Method Dependencies by Class

### AwsFunctionBase.cs Methods Used:
- `BaseFunctionHandler()`: Creates Lambda context
- `LogInfo()`: Logging throughout the application
- `CleanUp()`: Resource cleanup
- `SqlBulkCopy()`: Bulk database operations
- `AwsCredentials()`: AWS credential management
- `GetInstance()`: Gets optimization instance data
- `GetQueue()`: Gets optimization queue data
- `GetCustomerName()`: Retrieves customer information

### BillPeriodHelper.cs Methods Used:
- `GetBillingPeriodForServiceProvider()`: Gets billing period configuration
- `GetBillingPeriodCurrentMonth()`: Gets current month billing period
- `GetBillingPeriodForServiceProviderByCurrentDate()`: Date-based billing period lookup

### ServiceProviderCommon.cs Methods Used:
- `GetServiceProvider()`: Retrieves service provider details
- `GetNextServiceProviderId()`: Gets next provider for processing
- `GetServiceProviders()`: Gets all service providers
- `GetServiceProviderByName()`: Name-based provider lookup

### SettingsRepository.cs Methods Used:
- `GetTelegenceDeviceSettings()`: Gets Telegence-specific FTP settings
- `GetGeneralProviderSettings()`: Gets general AWS and email settings
- `GetOptimizationSettings()`: Gets optimization configuration

### SqlQueryHelper.cs Methods Used:
- `ExecuteStoredProcedureWithListResult()`: Executes SPs returning lists
- `ExecuteStoredProcedureWithRowCountResult()`: Executes SPs returning row counts
- `ExecuteStoredProcedureWithIntResult()`: Executes SPs returning integers

### TelegenceCommon.cs Methods Used:
- `GetTelegenceAuthenticationInformation()`: Gets API authentication
- `GetTelegenceBillingAccounts()`: Gets billing account mappings
- `UpdateTelegenceDeviceStatus()`: Updates device status via API
- `GetTelegenceDevicesAsync()`: Retrieves device lists via API
- `TelegenceGetDetailDataUsage()`: Gets detailed usage data

### TenantRepository.cs Methods Used:
- `GetTenantNameByTenantId()`: Gets tenant name for notifications
- `GetTenantIdByServiceProviderId()`: Maps provider to tenant
- `GetPortalImageByTenantId()`: Gets custom portal branding

## Database Operations Flow

### Staging Tables:
1. `TelegenceAllUsageStaging` - Premier usage data
2. `TelegenceDeviceUsageMubuStaging` - MUBU voice and data usage
3. `TelegenceDeviceFinalUsageStaging` - Final billing usage
4. `TelegenceSFTPFileDownloadStatus` - File download tracking

### Key Stored Procedures:
1. `usp_Telegence_Update_DeviceFinalUsage_FromStaging` - Processes final usage
2. `usp_Telegence_Update_DeviceMubuUsage_FromStaging` - Processes MUBU usage
3. `usp_Telegence_DeviceSync` - Synchronizes device data
4. `usp_Telegence_Zero_Usage_For_New_Billing_Cycle` - Resets usage for new cycle
5. `usp_Telegence_Truncate_UsageStaging` - Cleans staging tables

### Data Flow:
```
SFTP Files → DataTable → Staging Tables → Stored Procedures → Production Tables
```

## Error Handling and Retry Logic

### File Download Retry Mechanism:
1. **Initial Download**: Attempts to download files
2. **Failure Tracking**: Records failed downloads in `TelegenceSFTPFileDownloadStatus`
3. **Retry Queue**: Uses SQS to retry failed downloads
4. **Notification**: Sends email alerts for persistent failures

### SQL Retry Policy:
- Uses Polly retry policy from `PolicyFactory`
- Handles transient SQL connection issues
- Configurable retry count and delay

### SQS Message Processing:
- Handles message attributes for different processing modes
- Supports delayed message processing
- Manages queue cleanup after processing

## Notification System

### Email Notifications:
- **Blank File Alerts**: When MUBU files are empty
- **Download Failures**: When FTP downloads fail
- **Processing Errors**: General error notifications

### Queue Management:
- **TelegenceDeviceUsageQueueURL**: Main processing queue
- **TelegenceDeviceNotificationQueueURL**: Notification queue
- Message attributes control processing flow

## Configuration and Environment Variables

### Key Environment Variables:
- `TelegenceDeviceUsageQueueURL`: Main SQS queue
- `TelegenceDeviceNotificationQueueURL`: Notification queue
- `DeviceCleanupMaxRetries`: Retry limit for cleanup
- `DaysToKeep`: Data retention period
- `FtpReportNotificationThresholdDays`: Notification threshold
- `CheckFilesMissedThresholdDays`: File check threshold
- `LimitAmountFilePerRunTimes`: Files per execution limit
- `PremiereReportDelayDays`: Premier report delay
- `MUBURowsCountLimit`: MUBU processing limit

### Settings from Database:
- FTP/SFTP connection details
- AWS credentials
- Email configuration
- Billing period settings
- Optimization parameters

## Processing Modes

### 1. Initialization Mode
- Triggered by CloudWatch events
- Queues all Telegence service providers
- Sets up daily processing cycle

### 2. Regular Processing Mode
- Processes individual service providers
- Downloads and processes usage files
- Updates staging and production tables

### 3. Retry Mode
- Handles failed file downloads
- Reprocesses specific files
- Manages retry counters

### 4. Synchronization Mode
- Multi-step MUBU data synchronization
- Staged processing with queue management
- Ensures data consistency

This documentation provides a comprehensive overview of the AltaworxTelegenceAWSGetDeviceUsage Lambda function, covering all internal methods, their dependencies, and the complete data flow from FTP file retrieval to database processing.