# Notifications: Stale/Blank File Alerts and Threshold-Based Notifications

## Overview

The system implements a comprehensive notification mechanism designed to detect and alert on stale files, blank files, and threshold-based conditions that may indicate system issues or data synchronization problems.

## Core Functionality

### Threshold-Based Configuration

The system uses environment variables to configure notification thresholds that determine when files are considered stale:

- **FtpReportNotificationThresholdDays**: Defines the number of days after which FTP report files are considered stale
- **CheckFilesMissedThresholdDays**: Sets the threshold for detecting missed file checks

### File Age Validation

The system validates file freshness by comparing file write times against configurable thresholds:

- Files are considered within threshold if their write time is more recent than the current date minus the threshold days
- The `IsFileWithinThreshold` method performs this validation by checking if a usage file's write time falls within the acceptable range

### Stale File Detection Logic

The notification system actively monitors two types of usage files:

#### Voice Usage Monitoring
- Continuously checks the latest MUBU voice usage files for staleness
- Triggers warnings when voice usage files exceed the configured threshold
- Logs detailed information about stale voice usage including the last write time
- Generates specific notification subjects and bodies for stale voice sync alerts

#### General Usage Data Monitoring  
- Monitors MUBU usage data files for staleness indicators
- Detects when usage data has not been updated within the threshold period
- Creates targeted notifications for stale usage sync issues
- Provides detailed logging of stale usage conditions

### Notification Content Generation

The system generates structured HTML notification bodies for different types of stale file conditions:

#### Stale Voice Notifications
- Creates HTML-formatted alerts specifically for stale MUBU voice sync issues
- Includes service provider information and the timestamp of the most recent file
- Warns that AMOP voice metrics may become stale until FTP delivery resumes
- Provides clear identification of the affected service provider

#### Stale Usage Notifications
- Generates HTML notifications for stale MUBU usage sync problems  
- Contains service provider details and last successful file delivery timestamp
- Alerts that AMOP usage metrics may be outdated until synchronization resumes
- Maintains consistent formatting with voice notifications for easy identification

### Queue-Based Notification Delivery

The system integrates with AWS SQS for reliable notification delivery:

#### Device Notification Queue Integration
- Sends notification messages to a configured device notification queue
- Includes service provider ID and retry configuration as message attributes
- Validates queue URL configuration before attempting message delivery
- Provides comprehensive logging of queue operations and potential configuration issues

#### Message Structure and Attributes
- Constructs SQS messages with specific attributes including service provider ID and maximum retry counts
- Uses structured message bodies to convey notification context
- Implements proper error handling for queue communication failures
- Ensures message delivery reliability through AWS SQS infrastructure

### Error Handling and Logging

The notification system includes robust error handling and logging capabilities:

- Logs warning messages when stale files are detected with detailed timestamps
- Provides informational logging for notification queue operations
- Handles missing configuration gracefully with appropriate warning messages
- Maintains audit trails of all notification activities for troubleshooting purposes

### Integration Points

The notification system integrates with several key components:

- **File Monitoring System**: Receives file status updates and write time information
- **Configuration Management**: Retrieves threshold values from environment variables  
- **AWS SQS**: Delivers notifications through reliable message queuing
- **Logging Infrastructure**: Records all notification events and system status
- **Service Provider Management**: Associates notifications with specific service providers

## Key Benefits

- **Proactive Monitoring**: Detects stale file conditions before they impact downstream systems
- **Configurable Thresholds**: Allows adjustment of staleness detection sensitivity through environment variables
- **Reliable Delivery**: Uses AWS SQS for guaranteed notification delivery
- **Detailed Reporting**: Provides comprehensive information about stale file conditions
- **Service Provider Awareness**: Maintains context about which service providers are affected by stale data conditions