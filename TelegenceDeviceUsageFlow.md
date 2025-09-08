# Altaworx Telegence AWS Get Device Usage - Function Flow Analysis

## Overview
This document provides a comprehensive analysis of the `AltaworxTelegenceAWSGetDeviceUsage.cs` lambda function and its supporting classes, showing both high-level sequential flow and detailed low-level operations.

## Main Lambda Function: AltaworxTelegenceAWSGetDeviceUsage.cs

### Entry Point
- **FunctionHandler**: Main AWS Lambda entry point that processes SQS events

## Sequential Function Flow (High-Level)

### 1. Lambda Initialization
```
FunctionHandler
├── BaseFunctionHandler (AwsFunctionBase)
├── Environment Variable Setup
└── ProcessEventAsync
```

### 2. Event Processing Flow
```
ProcessEventAsync
├── If SQS Records exist:
│   └── ProcessEventRecordAsync (for each record)
└── If No Records (CloudWatch trigger):
    └── StartDailyDeviceUsageProcessingAsync
```

### 3. Daily Processing Initialization
```
StartDailyDeviceUsageProcessingAsync
├── GetServiceProviders (ServiceProviderCommon)
├── GetTelegenceDeviceSettings (SettingsRepository)
├── AddProviderToQueueAsync (for each provider)
└── SendProcessMessageToQueueAsync
```

### 4. Individual Provider Processing
```
ProcessEventRecordAsync
├── Extract Message Attributes (ServiceProviderId, FAN, ReportType)
├── ProcessDailyUsage
├── ProcessTelegenceMubuUsageDataSync (if MUBU sync)
└── BuildNotifyDownLoadFileBlank (if notification needed)
```

### 5. Usage Data Processing
```
ProcessDailyUsage
├── GetTelegenceDeviceSettings (SettingsRepository)
├── GetTelegenceAuthenticationInformation (TelegenceCommon)
├── GetTelegenceBillingAccounts (TelegenceCommon)
├── FTP File Operations:
│   ├── DownloadMubuFile (MUBU reports)
│   ├── GetLatestMubuUsage (Usage reports)
│   └── GetLatestMubuVoice (Voice reports)
└── Database Operations (SqlQueryHelper)
```

## Detailed Low-Level Flow Analysis

### Section 1: Lambda Function Initialization and Setup

#### 1.1 Function Entry and Context Setup
**Function**: `FunctionHandler`
- Creates KeySysLambdaContext using `BaseFunctionHandler` from `AwsFunctionBase`
- Loads environment variables for queue URLs, retry limits, and processing parameters
- Sets up default values for MUBU row count limits and delays
- Calls `ProcessEventAsync` to handle the main processing logic

#### 1.2 Base Function Initialization
**Class**: `AwsFunctionBase`
**Function**: `BaseFunctionHandler`
- Creates and initializes `KeySysLambdaContext` with AWS Lambda context
- Handles OU-specific logic initialization unless skipped
- Sets up logging and database connection context
- Returns configured context for use throughout the lambda

### Section 2: Event Processing and Message Handling

#### 2.1 Event Processing Logic
**Function**: `ProcessEventAsync`
- Checks if SQS event contains records
- If records exist: processes each SQS message individually via `ProcessEventRecordAsync`
- If no records (CloudWatch trigger): initiates daily processing via `StartDailyDeviceUsageProcessingAsync`
- Handles both queue-driven and scheduled execution patterns

#### 2.2 SQS Message Processing
**Function**: `ProcessEventRecordAsync`
- Extracts message attributes: InitializeProcessing, ServiceProviderId, FAN, ReportType
- Determines processing mode based on message attributes
- Routes to appropriate processing method:
  - `ProcessDailyUsage` for usage data processing
  - `ProcessTelegenceMubuUsageDataSync` for MUBU synchronization
  - `BuildNotifyDownLoadFileBlank` for notification handling

### Section 3: Service Provider and Settings Management

#### 3.1 Service Provider Retrieval
**Class**: `ServiceProviderCommon`
**Functions**:
- `GetServiceProviders`: Retrieves all service providers from database
- `GetServiceProvider`: Gets specific provider by ID with detailed configuration
- `GetNextServiceProviderId`: Gets next provider ID for integration processing
- `GetServiceProviderByName`: Retrieves provider by name

**Database Operations**:
- Queries ServiceProvider table for provider details
- Retrieves integration settings, billing periods, optimization hours
- Handles tenant associations and write permissions

#### 3.2 Settings Repository Operations
**Class**: `SettingsRepository`
**Functions**:
- `GetGeneralProviderSettings`: Retrieves global system settings
- `GetTelegenceDeviceSettings`: Gets Telegence-specific FTP and API settings
- `GetOptimizationSettings`: Retrieves optimization configuration by tenant
- `GetJasperDeviceSettings`: Gets Jasper provider settings
- `GetEbondingDeviceSettings`: Gets eBonding provider settings

**Configuration Management**:
- Loads AWS credentials and connection strings
- Retrieves FTP server details, paths, and authentication
- Manages email settings for notifications
- Handles time zone and billing period configurations

### Section 4: Telegence API and Data Operations

#### 4.1 Telegence Authentication and Account Management
**Class**: `TelegenceCommon`
**Functions**:
- `GetTelegenceAuthenticationInformation`: Retrieves API credentials and endpoints
- `GetTelegenceBillingAccounts`: Gets billing account mappings
- `UpdateTelegenceDeviceStatus`: Updates device status via API
- `UpdateTelegenceSubscriber`: Updates subscriber information
- `UpdateTelegenceMobilityConfiguration`: Updates mobility settings

**Authentication Flow**:
- Retrieves client ID, secret, and endpoint URLs from database
- Supports both production and sandbox environments
- Handles proxy-based and direct API communication
- Manages OAuth bearer tokens and headers

#### 4.2 Device Data Retrieval
**Functions**:
- `GetTelegenceDevicesAsync`: Retrieves paginated device lists
- `GetTelegenceDeviceBySubscriberNumber`: Gets specific device details
- `TelegenceGetDetailDataUsage`: Retrieves detailed usage data
- `GetBanStatusAsync`: Gets billing account status

**Data Processing**:
- Handles pagination with page size and current page tracking
- Processes device responses and maps to internal models
- Manages retry policies for API failures
- Supports both proxy and direct API calls

### Section 5: File Transfer and Data Processing

#### 5.1 FTP Operations
**Functions**:
- `DownloadMubuFile`: Downloads MUBU report files from FTP
- `GetLatestMubuUsage`: Retrieves latest usage data files
- `GetLatestMubuVoice`: Downloads voice usage files
- `DownloadAgainMubuFile`: Handles retry downloads for failed files

**File Processing Flow**:
- Establishes SFTP connection using credentials from settings
- Lists files in specified directories based on report type
- Downloads files based on date criteria and naming patterns
- Processes CSV/text files and converts to DataTable format
- Handles file cleanup and error retry mechanisms

#### 5.2 Usage Data Processing
**Report Types Handled**:
- **Premier Reports**: Primary usage data with delay processing
- **MUBU Reports**: Mobile usage billing data
- **Final Reports**: Consolidated final usage reports

**Processing Logic**:
- Filters files based on date ranges and billing periods
- Processes large files in batches to manage memory
- Validates data integrity and format compliance
- Handles duplicate detection and data deduplication

### Section 6: Database Operations and Data Persistence

#### 6.1 SQL Query Helper Operations
**Class**: `SqlQueryHelper`
**Functions**:
- `ExecuteStoredProcedureWithListResult`: Executes stored procedures returning lists
- `ExecuteStoredProcedureWithRowCountResult`: Executes procedures returning row counts
- `ExecuteStoredProcedureWithIntResult`: Executes procedures returning integer values
- `ExecuteStoredProcedureWithSingleValueResult`: Executes procedures returning single values

**Database Interaction Patterns**:
- Implements retry policies for transient failures
- Handles parameter cloning for reusable commands
- Provides comprehensive error logging and exception handling
- Supports configurable timeout settings

#### 6.2 Billing Period Management
**Class**: `BillingPeriodHelper`
**Functions**:
- `GetBillingPeriodForServiceProvider`: Gets billing period with time zone
- `GetBillingPeriodCurrentMonth`: Retrieves current month billing period
- `GetBillingPeriodForServiceProviderByCurrentDate`: Gets period by current date

**Billing Logic**:
- Calculates billing periods based on provider-specific end days/hours
- Handles time zone conversions for accurate period determination
- Supports custom billing cycles and prorated periods
- Validates date ranges and handles edge cases

### Section 7: Tenant and Multi-Tenancy Support

#### 7.1 Tenant Repository Operations
**Class**: `TenantRepository`
**Functions**:
- `GetTenantNameByTenantId`: Retrieves tenant name by ID
- `GetTenantIdByServiceProviderId`: Gets tenant ID from service provider
- `GetPortalImageByTenantId`: Retrieves custom portal images
- `GetCustomObjectById`: Gets custom tenant-specific objects

**Multi-Tenant Architecture**:
- Supports tenant-specific configurations and branding
- Handles tenant isolation and data segregation
- Manages custom objects and portal customizations
- Implements retry policies for tenant data operations

### Section 8: Queue Management and Message Processing

#### 8.1 SQS Queue Operations
**Functions**:
- `SendProcessMessageToQueueAsync`: Sends processing messages to queue
- `SendMessageToQueueDownloadAgainAsync`: Queues retry download messages
- `SendMessageToQueueDownloadAsync`: Queues download messages
- `SendNotificationMessageToQueueAsync`: Sends notification messages

**Queue Message Flow**:
- Creates SQS messages with appropriate attributes
- Handles message delays for processing coordination
- Manages queue URLs and message routing
- Implements error handling for queue operations

#### 8.2 Notification and Email Processing
**Functions**:
- `SendEmailAsync`: Sends email notifications
- `BuildNotifyDownLoadFileBlank`: Builds download failure notifications

**Notification Logic**:
- Constructs email messages with detailed error information
- Handles MIME message formatting and attachments
- Manages email routing and delivery confirmation
- Supports both HTML and plain text formats

### Section 9: Error Handling and Resilience

#### 9.1 Retry Policies and Error Recovery
**Implementation Patterns**:
- SQL retry policies for database transient failures
- HTTP retry policies for API calls with exponential backoff
- File download retry mechanisms with configurable attempts
- Queue message retry handling with dead letter queues

#### 9.2 Logging and Monitoring
**Logging Strategy**:
- Comprehensive logging at each processing stage
- Structured logging with correlation IDs
- Exception logging with stack traces
- Performance metrics and timing information

## Key Integration Points

### 1. Database Integration
- Central database for configuration and metadata
- Jasper database for device information
- Tenant-specific database connections
- Transaction management for data consistency

### 2. External Service Integration
- Telegence API for device management and usage data
- FTP/SFTP servers for file transfers
- AWS SQS for message queuing
- AWS SES for email notifications

### 3. Configuration Management
- Environment variables for runtime configuration
- Database-stored settings for provider-specific configuration
- Tenant-specific customizations and overrides
- Security credential management with base64 encoding

## Processing Workflows

### Daily Processing Workflow
1. CloudWatch trigger initiates daily processing
2. System retrieves all Telegence service providers
3. Each provider is queued for individual processing
4. Provider-specific settings and authentication are loaded
5. FTP connections are established for file retrieval
6. Files are downloaded, processed, and stored
7. Usage data is synchronized with billing systems
8. Notifications are sent for any processing issues

### On-Demand Processing Workflow
1. SQS message triggers specific provider processing
2. Message attributes determine processing type and scope
3. Targeted data retrieval and processing occurs
4. Results are stored and notifications sent as needed
5. Follow-up processing messages are queued if required

This comprehensive flow analysis provides visibility into the complete processing pipeline from lambda invocation through data persistence and notification delivery.