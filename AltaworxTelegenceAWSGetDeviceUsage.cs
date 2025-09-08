using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Transactions;
using Altaworx.AWS.Core;
using Altaworx.AWS.Core.Helpers;
using Altaworx.AWS.Core.Helpers.Constants;
using Altaworx.AWS.Core.Models;
using Altaworx.AWS.Core.Services;
using AltaworxTelegenceAWSGetDeviceUsage.Models;
using Amazon.Lambda.Core;
using Amazon.Lambda.SQSEvents;
using Amazon.SQS;
using Amazon.SQS.Model;
using Amop.Core.Helpers;
using Amop.Core.Logger;
using Amop.Core.Resilience;
using Amop.Core.Services.Att;
using Microsoft.Data.SqlClient;
using MimeKit;
using Polly;
using Polly.Retry;
using Renci.SshNet;
using ConnectionInfo = Renci.SshNet.ConnectionInfo;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace AltaworxTelegenceAWSGetDeviceUsage
{
    public class Function : AwsFunctionBase
    {
        private const string REPORT_TYPE_PREMIER = "Premier";
        private const string REPORT_TYPE_MUBU = "MUBU";
        private const string REPORT_TYPE_FINAL = "Final";
        private string ExportDeviceUsageQueueURL = Environment.GetEnvironmentVariable("TelegenceDeviceUsageQueueURL");
        private string DeviceNotificationQueueURL = Environment.GetEnvironmentVariable("TelegenceDeviceNotificationQueueURL");
        private string DeviceCleanupMaxRetries = Environment.GetEnvironmentVariable("DeviceCleanupMaxRetries");
        private string DaysToKeep = Environment.GetEnvironmentVariable("DaysToKeep");
        private int FtpReportNotificationThresholdDays = Convert.ToInt32(Environment.GetEnvironmentVariable("FtpReportNotificationThresholdDays"));
        private int CheckFilesMissedThresholdDays = Convert.ToInt32(Environment.GetEnvironmentVariable("CheckFilesMissedThresholdDays"));
        private int LimitAmountFilePerRunTimes = Convert.ToInt32(Environment.GetEnvironmentVariable("LimitAmountFilePerRunTimes"));
        private int PremiereReportDelayDays = Convert.ToInt32(Environment.GetEnvironmentVariable("PremiereReportDelayDays"));
        private const int SQSMaxDelaySeconds = 900;
        private int IsPremiereReportDelaySimulator = Convert.ToInt32(Environment.GetEnvironmentVariable("IsPremiereReportDelaySimulator")); // 1: true, 0:false
        private int DayEndBillingSimulator = Convert.ToInt32(Environment.GetEnvironmentVariable("DayEndBillingSimulator")); // day
        private const int MaxRetries = 3;
        private const int RetryDelaySeconds = 5;
        private bool IsFromCloudwatchEvent = false;
        private long MUBURowsCountLimit = (long)Convert.ToDouble(Environment.GetEnvironmentVariable("MUBURowsCountLimit"));
        private const long DefaultMUBURowsCountLimit = 200000;
        private const int DefaultDelaySQS = 10;

        /// <summary>
        /// This method is called for every Lambda invocation. This method takes in an SQS event object and can be used 
        /// to respond to SQS messages.
        /// </summary>
        /// <param name="sqsEvent"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public async Task FunctionHandler(SQSEvent sqsEvent, ILambdaContext context)
        {
            KeySysLambdaContext keysysContext = null;
            try
            {
                keysysContext = BaseFunctionHandler(context);
                if (string.IsNullOrEmpty(ExportDeviceUsageQueueURL))
                {
                    ExportDeviceUsageQueueURL = context.ClientContext.Environment["TelegenceDeviceUsageQueueURL"];
                    DeviceNotificationQueueURL = context.ClientContext.Environment["c"];
                    DeviceCleanupMaxRetries = context.ClientContext.Environment["DeviceCleanupMaxRetries"];
                    DaysToKeep = context.ClientContext.Environment["DaysToKeep"];
                    FtpReportNotificationThresholdDays =
                        Convert.ToInt32(context.ClientContext.Environment["FtpReportNotificationThresholdDays"]);
                    PremiereReportDelayDays =
                        Convert.ToInt32(context.ClientContext.Environment["PremiereReportDelayDays"]);
                    IsPremiereReportDelaySimulator =
                        Convert.ToInt32(context.ClientContext.Environment["IsPremiereReportDelaySimulator"]);
                    DayEndBillingSimulator =
                        Convert.ToInt32(context.ClientContext.Environment["DayEndBillingSimulator"]);
                    CheckFilesMissedThresholdDays =
                        Convert.ToInt32(context.ClientContext.Environment["CheckFilesMissedThresholdDays"]);
                    LimitAmountFilePerRunTimes =
                        Convert.ToInt32(context.ClientContext.Environment["LimitAmountFilePerRunTimes"]);
                }

                if (MUBURowsCountLimit <= 0)
                {
                    MUBURowsCountLimit = DefaultMUBURowsCountLimit;
                }

                await ProcessEventAsync(keysysContext, sqsEvent);

            }
            catch (Exception ex)
            {
                LogInfo(keysysContext, "EXCEPTION", ex.Message + " " + ex.StackTrace);
            }

            CleanUp(keysysContext);
        }

        private async Task ProcessEventAsync(KeySysLambdaContext context, SQSEvent sqsEvent)
        {
            LogInfo(context, "SUB", "ProcessEventAsync");
            if (sqsEvent?.Records != null)
            {
                foreach (var sqsEventRecord in sqsEvent.Records)
                {
                    await ProcessEventRecordAsync(context, sqsEventRecord);
                }
            }
            else
            {
                // queue all AT&T Telegence Providers
                await StartDailyDeviceUsageProcessingAsync(context, true);
            }
        }

        private async Task ProcessEventRecordAsync(KeySysLambdaContext context, SQSEvent.SQSMessage message)
        {
            LogInfo(context, "SUB", "ProcessEventRecordAsync");
            var initializeProcessing = false;
            if (message.MessageAttributes.ContainsKey("InitializeProcessing"))
            {
                initializeProcessing = Convert.ToBoolean(message.MessageAttributes["InitializeProcessing"].StringValue);
                LogInfo(context, "InitializeProcessing", initializeProcessing.ToString());
            }

            var serviceProviderId = 0;
            if (message.MessageAttributes.ContainsKey("ServiceProviderId"))
            {
                serviceProviderId = Convert.ToInt32(message.MessageAttributes["ServiceProviderId"].StringValue);
                LogInfo(context, "ServiceProviderId", serviceProviderId);
            }

            string fan = string.Empty;
            if (message.MessageAttributes.ContainsKey("FAN"))
            {
                fan = message.MessageAttributes["FAN"].StringValue;
                LogInfo(context, "FAN", fan);
            }

            string reportType = string.Empty;
            if (message.MessageAttributes.ContainsKey("ReportType"))
            {
                reportType = message.MessageAttributes["ReportType"].StringValue;
                LogInfo(context, "ReportType", reportType);
            }

            //Indicate a Premier/ Mubu report sync that is triggered from daily sync
            var isFromCloudwatchEvent = false;
            if (message.MessageAttributes.ContainsKey("IsFromCloudwatchEvent"))
            {
                isFromCloudwatchEvent = Convert.ToBoolean(message.MessageAttributes["IsFromCloudwatchEvent"].StringValue);
                IsFromCloudwatchEvent = isFromCloudwatchEvent;
                LogInfo(context, "IsFromCloudwatchEvent", isFromCloudwatchEvent);
            }

            var isDownLoadFileAgain = false;
            if (message.MessageAttributes.ContainsKey("IsDownLoadFileAgain"))
            {
                isDownLoadFileAgain = Convert.ToBoolean(message.MessageAttributes["IsDownLoadFileAgain"].StringValue);
                LogInfo(context, "IsDownLoadFileAgain", isDownLoadFileAgain);
            }

            var isDownloadNextInstance = false;
            if (message.MessageAttributes.ContainsKey("IsDownloadNextInstance"))
            {
                isDownloadNextInstance = Convert.ToBoolean(message.MessageAttributes["IsDownloadNextInstance"].StringValue);
                LogInfo(context, "IsDownloadNextInstance", isDownloadNextInstance);
            }

            var writeTimesNextDownload = new List<string>();
            if (message.MessageAttributes.ContainsKey("WriteTimesNextDownload"))
            {
                var writeTimesNextDownloadString = message.MessageAttributes["WriteTimesNextDownload"].StringValue;
                writeTimesNextDownload = writeTimesNextDownloadString.Split(',').ToList();
                LogInfo(context, "WriteTimesNextDownload", writeTimesNextDownload);
            }

            var fileNamesNextDownload = new List<string>();
            if (message.MessageAttributes.ContainsKey("FileNamesNextDownload"))
            {
                var writeTimesNextDownloadString = message.MessageAttributes["FileNamesNextDownload"].StringValue;
                fileNamesNextDownload = writeTimesNextDownloadString.Split(',').ToList();
                LogInfo(context, "FileNamesNextDownload", writeTimesNextDownload);
            }

            var downloadFailedIds = new List<string>();
            if (message.MessageAttributes.ContainsKey("DownloadFailedIds"))
            {
                var downLoadFailedIdsString = message.MessageAttributes["DownloadFailedIds"].StringValue;
                downloadFailedIds = downLoadFailedIdsString.Split(',').ToList();
                LogInfo(context, "DownloadFailedIds", writeTimesNextDownload);
            }

            var telegenceSyncDataStep = (int)TelegenceSyncDataStepEnum.None;
            if (message.MessageAttributes.ContainsKey(Amop.Core.Constants.SQSMessageKeyConstant.TELEGENCE_SYNC_DATA_STEP))
            {
                telegenceSyncDataStep = int.Parse(message.MessageAttributes[Amop.Core.Constants.SQSMessageKeyConstant.TELEGENCE_SYNC_DATA_STEP].StringValue);
                LogInfo(context, Amop.Core.Constants.SQSMessageKeyConstant.TELEGENCE_SYNC_DATA_STEP, Enum.GetName(typeof(TelegenceSyncDataStepEnum), telegenceSyncDataStep));
            }

            if (initializeProcessing)
            {
                // queue all AT&T Telegence Providers
                await StartDailyDeviceUsageProcessingAsync(context);
            }
            else if (!initializeProcessing && reportType == REPORT_TYPE_MUBU && IsFromCloudwatchEvent && telegenceSyncDataStep > 0)
            {
                await ProcessTelegenceMubuUsageDataSync(context, serviceProviderId, telegenceSyncDataStep);
            }
            else
            {
                await ProcessDailyUsage(context, serviceProviderId, fan, reportType, isFromCloudwatchEvent, isDownLoadFileAgain, isDownloadNextInstance,
                        writeTimesNextDownload, fileNamesNextDownload, downloadFailedIds);
            }
        }

        private async Task ProcessDailyUsage(KeySysLambdaContext context, int serviceProviderId, string fan, string reportType, bool isFromCloudwatchEvent, bool isDownLoadFileAgain,
            bool isDownloadNextInstance,
            List<string> writeTimesNextDownload, List<string> fileNamesNextDownload, List<string> downloadFailedIds)
        {
            LogInfo(context, "SUB", $"ProcessDailyUsage(,{serviceProviderId},{fan})");

            var settings = context.SettingsRepo.GetTelegenceDeviceSettings(serviceProviderId);

            if (!string.IsNullOrWhiteSpace(settings.TelegenceFtpUsername) && !string.IsNullOrWhiteSpace(settings.TelegenceFtpPassword))
            {
                var password = context.Base64Service.Base64Decode(settings.TelegenceFtpPassword);

                if (isDownLoadFileAgain && downloadFailedIds.Count > 0)
                {
                    await ProcessDownloadFileAgain(context, serviceProviderId, fan, reportType, settings.TelegenceFtpUsername, password,
                       settings.TelegenceFtpServer, settings.TelegenceFtpPath, settings.TelegenceFtpMubuPath,
                       settings.TelegenceFtpFinalUsagePath, isFromCloudwatchEvent, downloadFailedIds);

                    CleanUpFtp(context, settings.TelegenceFtpUsername, password, settings.TelegenceFtpServer, settings.TelegenceFtpPath);
                    CleanUpFtp(context, settings.TelegenceFtpUsername, password, settings.TelegenceFtpServer, settings.TelegenceFtpMubuPath);
                }
                else if (isDownloadNextInstance && fileNamesNextDownload.Count > 0 && writeTimesNextDownload.Count > 0)
                {
                    await ProcessDownloadFileNextInstance(context, serviceProviderId, fan, reportType, settings.TelegenceFtpUsername, password,
                     settings.TelegenceFtpServer, settings.TelegenceFtpPath, settings.TelegenceFtpMubuPath,
                     settings.TelegenceFtpFinalUsagePath, isFromCloudwatchEvent, fileNamesNextDownload, writeTimesNextDownload, downloadFailedIds);

                    CleanUpFtp(context, settings.TelegenceFtpUsername, password, settings.TelegenceFtpServer, settings.TelegenceFtpPath);
                    CleanUpFtp(context, settings.TelegenceFtpUsername, password, settings.TelegenceFtpServer, settings.TelegenceFtpMubuPath);
                }
                else
                {
                    await ProcessDailyUsage(context, serviceProviderId, fan, reportType, settings.TelegenceFtpUsername, password,
                        settings.TelegenceFtpServer, settings.TelegenceFtpPath, settings.TelegenceFtpMubuPath,
                        settings.TelegenceFtpFinalUsagePath, isFromCloudwatchEvent);

                    CleanUpFtp(context, settings.TelegenceFtpUsername, password, settings.TelegenceFtpServer, settings.TelegenceFtpPath);
                    CleanUpFtp(context, settings.TelegenceFtpUsername, password, settings.TelegenceFtpServer, settings.TelegenceFtpMubuPath);
                }
            }
            else
            {
                LogInfo(context, "WARN", $"No FTP Credentials found for service provider {serviceProviderId}");
            }

            // delete queue record
            Dequeue(context, serviceProviderId, fan);
        }

        public async Task ProcessDailyUsage(KeySysLambdaContext context, int serviceProviderId, string fan, string reportType,
            string username, string password, string server, string path, string mubuPath, string finalUsagePath, bool isFromCloudwatchEvent)
        {
            LogInfo(context, "SUB", $"ProcessDailyUsage({serviceProviderId},{fan},{reportType},{username},,{server},{path},{mubuPath},{finalUsagePath})");
            var sqlRetryPolicy = GetSqlRetryPolicy(context);
            if (string.IsNullOrWhiteSpace(reportType) || reportType == REPORT_TYPE_PREMIER)
            {
                // process premiere daily usage report
                var usage = GetLatestUsage(context, serviceProviderId, fan, username, password, server, path);
                if (usage != null && usage.Rows.Count > 0)
                {
                    // bulk load
                    LogInfo(context, "STATUS", "Usage SQL Bulk Copy Start");
                    SqlBulkCopy(context, context.CentralDbConnectionString, usage, "TelegenceAllUsageStaging");
                }
            }
            else if (reportType == REPORT_TYPE_MUBU)
            {
                if (!string.IsNullOrWhiteSpace(mubuPath))
                {
                    var newestVoiceFileList = new List<UsageFile>();
                    var newestDataFileList = new List<UsageFile>();
                    var fileNameNeedDownloadFailed = GetFileNamesDownloadFailed(context, serviceProviderId, REPORT_TYPE_MUBU);
                    var fileDownloadAgainDt = InitFileNameDownloadFailedDataTable();
                    var fileNamesNextDownload = new List<string>();
                    var writeTimesNextDownload = new List<string>();
                    var limitFilesDownloadNumber = LimitAmountFilePerRunTimes - fileNameNeedDownloadFailed.Count;
                    var today = DateTime.Now;
                    var startDate = today.AddDays(-CheckFilesMissedThresholdDays);
                    var endDate = today.AddDays(1);

                    // get files downloaded last week in AMOP
                    var fileNameThresholdDays = GetFilesDownLoaded(context, startDate, endDate);

                    var connectionInfo = new ConnectionInfo(server, username, new PasswordAuthenticationMethod(username, password));
                    using (SftpClient client = new SftpClient(connectionInfo))
                    {
                        client.Connect();

                        // compare files download and files on SFTP in last week 
                        // if file havn't downloaded -> re-download
                        newestVoiceFileList = GetLatestMubuVoiceFileList(mubuPath, client, limitFilesDownloadNumber, fileNameThresholdDays, startDate, endDate);
                        limitFilesDownloadNumber -= newestVoiceFileList.Count;
                        newestDataFileList = GetLatestMubuUsageFileList(mubuPath, client, limitFilesDownloadNumber, fileNameThresholdDays, startDate, endDate);
                    }

                    LogInfo(context, "INFO", $"The voice file has {newestVoiceFileList.Count} files need download.");
                    LogInfo(context, "INFO", $"The data file has {newestDataFileList.Count} files need download.");

                    // process MUBU
                    if (newestVoiceFileList.Count > 0)
                    {
                        var mubuVoice = await GetLatestMubuVoice(context, fileDownloadAgainDt, serviceProviderId, username, password, server, mubuPath, newestVoiceFileList[0]);
                        if (mubuVoice != null && mubuVoice.Rows.Count > 1)
                        {
                            // bulk load
                            LogInfo(context, "STATUS", "MUBU Voice SQL Bulk Copy Start");
                            SqlBulkCopy(context, context.CentralDbConnectionString, mubuVoice, "TelegenceDeviceUsageMubuStaging", MubuReportReader.GetRecordColumnMapping());
                        }

                        // because the first file downloaded, so just save files have not downloaded to send message
                        if (newestVoiceFileList.Count > 1)
                        {
                            foreach (var fileName in newestVoiceFileList.Select((item, index) => new { item, index }))
                            {
                                if (fileName.index > 0)
                                {
                                    fileNamesNextDownload.Add(fileName.item.FilePath);
                                    writeTimesNextDownload.Add(fileName.item.WriteTime.ToString("yyyy-MM-dd HH:mm:ss"));
                                }
                            }
                        }
                    }

                    if (newestDataFileList.Count > 0)
                    {
                        var mubuUsage = await GetLatestMubuUsage(context, fileDownloadAgainDt, serviceProviderId, username, password, server, mubuPath, newestDataFileList[0]);
                        if (mubuUsage != null && mubuUsage.Rows.Count > 1)
                        {
                            // bulk load
                            LogInfo(context, "STATUS", "MUBU Usage SQL Bulk Copy Start");
                            SqlBulkCopy(context, context.CentralDbConnectionString, mubuUsage, "TelegenceDeviceUsageMubuStaging", MubuReportReader.GetRecordColumnMapping());
                        }

                        // because the first file downloaded, so just save files have not downloaded to send message
                        if (newestDataFileList.Count > 1)
                        {
                            foreach (var fileName in newestDataFileList.Select((item, index) => new { item, index }))
                            {
                                if (fileName.index > 0)
                                {
                                    fileNamesNextDownload.Add(fileName.item.FilePath);
                                    writeTimesNextDownload.Add(fileName.item.WriteTime.ToString("yyyy-MM-dd HH:mm:ss"));
                                }
                            }
                        }
                    }

                    if (fileDownloadAgainDt != null && fileDownloadAgainDt.Rows.Count > 0)
                    {
                        LogInfo(context, "STATUS", "Insert the file download failed to DB");
                        SqlBulkCopy(context, context.CentralDbConnectionString, fileDownloadAgainDt, "TelegenceSFTPFileDownloadStatus");
                    }

                    // send message next sync MUBU
                    if (writeTimesNextDownload.Count > 0)
                    {
                        var fileNamesNextDownloadString = string.Join(",", fileNamesNextDownload);
                        var writeTimesNextDownloadString = string.Join(",", writeTimesNextDownload);
                        var fileDownLoadFailedIds = fileNameNeedDownloadFailed.Count > 0 ? string.Join(",", fileNameNeedDownloadFailed) : "";

                        await SendMessageToQueueNextDownloadAsync(context, serviceProviderId, isFromCloudwatchEvent, REPORT_TYPE_MUBU, fileNamesNextDownloadString,
                            writeTimesNextDownloadString, fileDownLoadFailedIds);
                    }
                    else if (fileNameNeedDownloadFailed.Count > 0)
                    {
                        var fileDownLoadFailedIds = string.Join(",", fileNameNeedDownloadFailed);
                        await SendMessageToQueueDownloadAgainAsync(context, serviceProviderId, isFromCloudwatchEvent, REPORT_TYPE_MUBU, fileDownLoadFailedIds, 0);
                    }
                    else
                    {
                        await BuildNotifyDownLoadFileBlank(context, serviceProviderId);
                        if (isFromCloudwatchEvent)
                        {
                            await SendProcessMessageToQueueAsync(context, serviceProviderId, string.Empty, REPORT_TYPE_MUBU, isFromCloudwatchEvent, (int)TelegenceSyncDataStepEnum.UpdateTelegenceMubuUsageFromStaging);
                        }
                    }
                }
                else
                {
                    LogInfo(context, "WARNING", "MUBU report will not be loaded. No MUBU Path specified.");
                }
            }
            else if (reportType == REPORT_TYPE_FINAL)
            {
                if (!string.IsNullOrWhiteSpace(finalUsagePath))
                {
                    // process final usage
                    var finalUsage = GetLatestFinalUsage(context, serviceProviderId, username, password, server, finalUsagePath);
                    if (finalUsage != null && finalUsage.Rows.Count > 0)
                    {
                        // bulk load
                        LogInfo(context, "STATUS", "Final Usage SQL Bulk Copy Start");
                        SqlBulkCopy(context, context.CentralDbConnectionString, finalUsage, "TelegenceDeviceFinalUsageStaging");

                        var billingPeriodYear = BillingPeriodYearFromFinalUsageFileName(finalUsage.TableName);
                        var billingPeriodMonth = BillingPeriodMonthFromFinalUsageFileName(finalUsage.TableName);
                        // load final usage                       
                        sqlRetryPolicy.Execute(() => UpdateTelegenceFinalUsageFromStaging(context, serviceProviderId, billingPeriodYear, billingPeriodMonth));
                    }
                }
            }
        }

        private async Task BuildNotifyDownLoadFileBlank(KeySysLambdaContext context, int serviceProviderId)
        {
            LogInfo(context, LogTypeConstant.Sub, $"({serviceProviderId})");
            var serviceProvider = ServiceProviderCommon.GetServiceProvider(context.CentralDbConnectionString, serviceProviderId);
            var serviceProviderName = serviceProvider.DisplayName;
            int tenantId = serviceProvider.TenantId ?? 0;
            string tenantName = context.TenantRepo.GetTenantNameByTenantId(tenantId);
            var subject = $"{serviceProviderName} Blank MUBU Report(s) Received ({tenantName})";

            // get file blank from db
            var usageFileList = GetFileBlankNotify(context);

            if (usageFileList.Count > 0)
            {
                var body = BuildDownloadMUBUFileEmptyNotificationBody(usageFileList, subject);
                await SendEmailAsync(context, subject, body);
                UpdateBlankFileNotifyComplete(context);
            }
        }

        private async Task ProcessTelegenceMubuUsageDataSync(KeySysLambdaContext context, int serviceProviderId, int telegenceSyncDataStep)
        {
            LogInfo(context, Amop.Core.Constants.CommonConstants.SUB, "");
            var sqlRetryPolicy = GetSqlRetryPolicy(context);
            if (telegenceSyncDataStep == (int)TelegenceSyncDataStepEnum.UpdateTelegenceMubuUsageFromStaging)
            {
                sqlRetryPolicy.Execute(() => UpdateTelegenceMubuUsageFromStaging(context, serviceProviderId));
                await SendProcessMessageToQueueAsync(context, serviceProviderId, string.Empty, REPORT_TYPE_MUBU, true, (int)TelegenceSyncDataStepEnum.UpdateMobilityMubuUsageFromTelegence);
            }
            else if (telegenceSyncDataStep == (int)TelegenceSyncDataStepEnum.UpdateMobilityMubuUsageFromTelegence)
            {
                sqlRetryPolicy.Execute(() => UpdateMobilityMubuUsageFromTelegence(context, serviceProviderId));
                TruncateTableByTableName(context, Amop.Core.Constants.DatabaseTableNames.TELEGENCE_DEVICE_USAGE_MUBU_STAGING);
                await SendProcessMessageToQueueAsync(context, serviceProviderId, string.Empty, REPORT_TYPE_MUBU, true, (int)TelegenceSyncDataStepEnum.UpdateLateMubuUsageFromTelegence);
            }
            else if (telegenceSyncDataStep == (int)TelegenceSyncDataStepEnum.UpdateLateMubuUsageFromTelegence)
            {
                sqlRetryPolicy.Execute(() => UpdateLateMubuUsageFromTelegence(context, serviceProviderId));
                TruncateTableByTableName(context, Amop.Core.Constants.DatabaseTableNames.TELEGENCE_DEVICE_USAGE_MUBU_STAGING);
            }
        }

        public async Task ProcessDownloadFileAgain(KeySysLambdaContext context, int serviceProviderId, string fan, string reportType,
            string username, string password, string server, string path, string mubuPath, string finalUsagePath, bool isFromCloudwatchEvent, List<string> downloadFailedIds)
        {
            LogInfo(context, "SUB", $"ProcessDownloadFileAgain({serviceProviderId},{fan},{reportType},{username},,{server},{path},{mubuPath},{finalUsagePath})");
            var sqlRetryPolicy = GetSqlRetryPolicy(context);
            if (reportType == REPORT_TYPE_MUBU)
            {
                if (!string.IsNullOrWhiteSpace(mubuPath))
                {
                    // process MUBU
                    var downLoadFailedId = int.Parse(downloadFailedIds[0]);
                    var mubuData = await DownloadAgainMubuFile(context, serviceProviderId, username, password, server, mubuPath, downLoadFailedId);
                    if (mubuData != null && mubuData.Rows.Count > 0)
                    {
                        // bulk load
                        LogInfo(context, "STATUS", "MUBU Voice SQL Bulk Copy Start");
                        SqlBulkCopy(context, context.CentralDbConnectionString, mubuData, "TelegenceDeviceUsageMubuStaging", MubuReportReader.GetRecordColumnMapping());
                    }

                    downloadFailedIds.RemoveAt(0);
                    if (downloadFailedIds.Count > 0)
                    {
                        var downloadFailedIdsString = string.Join(",", downloadFailedIds);
                        await SendMessageToQueueDownloadAgainAsync(context, serviceProviderId, isFromCloudwatchEvent, REPORT_TYPE_MUBU, downloadFailedIdsString, 0);
                    }
                    else
                    {
                        await BuildNotifyDownLoadFileBlank(context, serviceProviderId);
                        if (isFromCloudwatchEvent)
                        {
                            await SendProcessMessageToQueueAsync(context, serviceProviderId, string.Empty, REPORT_TYPE_MUBU, isFromCloudwatchEvent, (int)TelegenceSyncDataStepEnum.UpdateTelegenceMubuUsageFromStaging);
                        }
                    }
                }
                else
                {
                    LogInfo(context, "WARNING", "MUBU report will not be loaded. No MUBU Path specified.");
                }
            }
        }

        public async Task ProcessDownloadFileNextInstance(KeySysLambdaContext context, int serviceProviderId, string fan, string reportType,
            string username, string password, string server, string path, string mubuPath, string finalUsagePath, bool isFromCloudwatchEvent,
             List<string> fileNamesNextDownload, List<string> writeTimesNextDownload, List<string> downloadFailedIds)
        {
            LogInfo(context, "SUB", $"ProcessDownloadFileNextInstance({serviceProviderId},{fan},{reportType},{username},,{server},{path},{mubuPath},{finalUsagePath})");
            var sqlRetryPolicy = GetSqlRetryPolicy(context);
            if (reportType == REPORT_TYPE_MUBU)
            {
                if (!string.IsNullOrWhiteSpace(mubuPath))
                {
                    var fileDownloadAgainDt = InitFileNameDownloadFailedDataTable();

                    // process MUBU
                    var writeTime = DateTime.Parse(writeTimesNextDownload[0]);
                    var mubuData = await DownloadMubuFile(context, fileDownloadAgainDt, serviceProviderId, username, password, server, mubuPath, fileNamesNextDownload[0], writeTime);
                    if (mubuData != null && mubuData.Rows.Count > 0)
                    {
                        // bulk load
                        LogInfo(context, "STATUS", "MUBU Voice SQL Bulk Copy Start");
                        SqlBulkCopy(context, context.CentralDbConnectionString, mubuData, "TelegenceDeviceUsageMubuStaging", MubuReportReader.GetRecordColumnMapping());
                    }

                    if (fileDownloadAgainDt != null && fileDownloadAgainDt.Rows.Count > 0)
                    {
                        LogInfo(context, "STATUS", "Insert the file download failed to DB");
                        SqlBulkCopy(context, context.CentralDbConnectionString, fileDownloadAgainDt, "TelegenceSFTPFileDownloadStatus");
                    }

                    writeTimesNextDownload.RemoveAt(0);
                    fileNamesNextDownload.RemoveAt(0);
                    if (writeTimesNextDownload.Count > 0)
                    {
                        var fileNamesNextDownloadString = string.Join(",", fileNamesNextDownload);
                        var writeTimesNextDownloadString = string.Join(",", writeTimesNextDownload);
                        var fileDownLoadFailedIds = downloadFailedIds.Count > 0 ? string.Join(",", downloadFailedIds) : "";

                        await SendMessageToQueueNextDownloadAsync(context, serviceProviderId, isFromCloudwatchEvent, REPORT_TYPE_MUBU, fileNamesNextDownloadString,
                            writeTimesNextDownloadString, fileDownLoadFailedIds);
                    }
                    else if (downloadFailedIds.Count > 0)
                    {
                        var downloadFailedIdsString = string.Join(",", downloadFailedIds);
                        await SendMessageToQueueDownloadAgainAsync(context, serviceProviderId, isFromCloudwatchEvent, REPORT_TYPE_MUBU, downloadFailedIdsString, 0);
                    }
                    else
                    {
                        await BuildNotifyDownLoadFileBlank(context, serviceProviderId);
                        if (isFromCloudwatchEvent)
                        {
                            await SendProcessMessageToQueueAsync(context, serviceProviderId, string.Empty, REPORT_TYPE_MUBU, isFromCloudwatchEvent, (int)TelegenceSyncDataStepEnum.UpdateTelegenceMubuUsageFromStaging);
                        }
                    }
                }
                else
                {
                    LogInfo(context, "WARNING", "MUBU report will not be loaded. No MUBU Path specified.");
                }
            }
        }

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

        private static void Dequeue(KeySysLambdaContext context, int serviceProviderId, string fan)
        {
            LogInfo(context, "SUB", $"Dequeue({serviceProviderId},{fan})");
            using (var con = new SqlConnection(context.CentralDbConnectionString))
            {
                using (var cmd = new SqlCommand("DELETE FROM [dbo].[TelegenceDeviceUsageIdsToProcess] WHERE ServiceProviderId = @ServiceProviderId AND FoundationAccountNumber = @FAN", con)
                {
                    CommandType = CommandType.Text
                })
                {
                    cmd.Parameters.AddWithValue("@ServiceProviderId", serviceProviderId);
                    cmd.Parameters.AddWithValue("@FAN", fan);
                    con.Open();
                    cmd.ExecuteNonQuery();
                }
            }
        }

        private static void ZeroOutUsage(string connectionString, int serviceProviderId, IKeysysLogger logger, int daysOffset = 0)
        {
            logger.LogInfo("SUB", $"ZeroOutUsage(...,{serviceProviderId},...,{daysOffset}");

            using var connection = new SqlConnection(connectionString);
            using (var command = new SqlCommand("usp_Telegence_Zero_Usage_For_New_Billing_Cycle", connection))
            {
                command.CommandType = CommandType.StoredProcedure;
                command.Parameters.AddWithValue("@serviceProviderId", serviceProviderId);
                command.Parameters.AddWithValue("@DaysOffset", daysOffset);
                connection.Open();

                command.ExecuteNonQuery();

                connection.Close();
            }
        }

        private DataTable GetLatestUsage(KeySysLambdaContext context, int serviceProviderId, string fan, string username, string password, string server, string path)
        {
            LogInfo(context, "SUB", $"GetLatestUsage({fan},{username},,{server},{path})");
            DataTable usage = null;
            var connectionInfo = new ConnectionInfo(server, username, new PasswordAuthenticationMethod(username, password));
            using (SftpClient client = new SftpClient(connectionInfo))
            {
                client.Connect();

                var newestFile = GetLatestUsageFile(fan, path, client);

                if (HasRecentUsage(newestFile))
                {
                    using (var ms = new MemoryStream())
                    {
                        client.DownloadFile(newestFile.FilePath, ms);

                        ms.Position = 0;
                        using (var archive = new System.IO.Compression.ZipArchive(ms))
                        {
                            using (var reader = new StreamReader(archive.Entries.First().Open(), Encoding.UTF8, true))
                            {
                                var fileReader = new PremierUnbilledUsageReportReader(new PremierUnbilledUsageRecordFactory());
                                usage = fileReader.ReadRecords(serviceProviderId, 1, newestFile.WriteTime, reader);
                            }
                        }
                    }
                }
            }

            return usage;
        }

        private async Task<DataTable> GetLatestMubuVoice(KeySysLambdaContext context, DataTable fileDownloadAgainDt, int serviceProviderId, string username, string password, string server, string path, UsageFile newestFile)
        {
            LogInfo(context, "SUB", $"GetLatestMubuVoice({serviceProviderId},{username},,{server},{path})");
            return await InsertMUBURecords(context, fileDownloadAgainDt, serviceProviderId, username, password, server, path, newestFile);
        }


        private async Task<DataTable> DownloadAgainMubuFile(KeySysLambdaContext context, int serviceProviderId, string username, string password, string server, string path, int downLoadFailId)
        {
            LogInfo(context, "SUB", $"DownloadAgainMubuFile - file Id: {downLoadFailId}");

            // get file download error from DB
            TelegenceSFTPFileDownloadStatus fileFromDb = GetFileDownloadFailedByFileName(context, serviceProviderId, REPORT_TYPE_MUBU, downLoadFailId);
            if (fileFromDb == null || fileFromDb.Id < 1)
            {
                LogInfo(context, "EXCEPTION", $"Not found file name {fileFromDb.FileName}");
                return null;
            }
            var newestFile = new UsageFile()
            {
                FilePath = fileFromDb.FileName,
                WriteTime = fileFromDb.WriteTime
            };

            return await InsertMUBURecords(context, null, serviceProviderId, username, password, server, path, newestFile, true, true, false);
        }

        private async Task<DataTable> DownloadMubuFile(KeySysLambdaContext context, DataTable fileDownloadAgainDt, int serviceProviderId, string username, string password, string server, string path, string fileName, DateTime writeTime)
        {
            LogInfo(context, "SUB", $"DownloadMubuFile({serviceProviderId},{username},,{server},{path})");
            var newestFile = new UsageFile()
            {
                FilePath = fileName,
                WriteTime = writeTime
            };

            return await InsertMUBURecords(context, fileDownloadAgainDt, serviceProviderId, username, password, server, path, newestFile);
        }

        public async Task<DataTable> InsertMUBURecords(KeySysLambdaContext context, DataTable retryDataTable, int serviceProviderId, string username, string password,
            string server, string path, UsageFile file,
            bool shouldCheckRecentUsage = true, bool shouldMarkedFileAsSuccess = false, bool shouldRetrySync = true)
        {
            LogInfo(context, "SUB", $"InsertMUBURecords(...,{serviceProviderId},{username},,{server},{path})");
            DataTable usage = null;
            var connectionInfo = new ConnectionInfo(server, username, new PasswordAuthenticationMethod(username, password));
            var fanFilter = GetFANFilter(context, serviceProviderId);
            using (TransactionScope transactionScope = new TransactionScope(TransactionScopeOption.Required, new TimeSpan(0, 15, 0), TransactionScopeAsyncFlowOption.Enabled))
            {
                using (SftpClient client = new SftpClient(connectionInfo))
                {
                    client.Connect();

                    try
                    {
                        var savedFileId = GetMubuFtpFileNameId(context, file.FilePath);
                        var savedRecordCount = 0;
                        //check creation time of file and the file name records
                        if (!shouldCheckRecentUsage || (shouldCheckRecentUsage && HasRecentUsage(file)))
                        {
                            LogInfo(context, "INFO", $"Reading file {file.FilePath}");

                            if (file.WriteTimeUtc == null)
                            {
                                var mubuFileWriteTime = GetMubuFileWriteTimes(path, client, file.FilePath);
                                if (mubuFileWriteTime == null)
                                {
                                    LogInfo(context, LogTypeConstant.Warning, $"Not found the file path {file.FilePath}.");
                                    return usage;
                                }

                                file = mubuFileWriteTime;
                            }

                            using (var ms = new MemoryStream())
                            {
                                client.DownloadFile(file.FilePath, ms);
                                var fileId = -1;
                                //save file name and check if file already synced
                                if (savedFileId <= 0)
                                {
                                    //save file name and check if file already synced
                                    fileId = InsertMubuTableNameMapping(context, file);
                                    if (fileId <= 0)
                                    {
                                        var errorMsg = $"Error saving file name {file.FilePath}.";
                                        LogInfo(context, "ERROR", errorMsg);
                                        throw new Exception(errorMsg);
                                    }
                                }
                                else
                                {
                                    var isBlankFile = IsBlankFileFromDB(context, file.FilePath);
                                    if (isBlankFile)
                                    {
                                        LogInfo(context, LogTypeConstant.Info, $"The file name {file.FileName} is the blank file.");
                                        transactionScope.Complete();
                                        return usage;
                                    }

                                    fileId = savedFileId;
                                    savedRecordCount = GetRecordCountFromDatabase(context, savedFileId);
                                }

                                ms.Position = 0;
                                using (var archive = new System.IO.Compression.ZipArchive(ms))
                                {
                                    using (var reader = new StreamReader(archive.Entries.First().Open(), Encoding.UTF8, true))
                                    {
                                        var fileReader = new MubuReportReader(new MubuRecordFactory());
                                        var records = fileReader.ReadBatchedRecords(serviceProviderId, file.FilePath, savedRecordCount + 1, fileId, reader, MUBURowsCountLimit, fanFilter);
                                        if (records.Records.Rows.Count < 1)
                                        {
                                            MarkMubuFileAsBlank(context, fileId);
                                        }

                                        usage = records.Records;
                                        if (!records.IsEndOfFile)
                                        {
                                            await SendMessageToQueueDownloadAsync(context, serviceProviderId, IsFromCloudwatchEvent, REPORT_TYPE_MUBU, file, DefaultDelaySQS);
                                        }
                                    }
                                }
                            }
                        }
                        if (shouldMarkedFileAsSuccess)
                        {
                            // update status SUCCESS in db
                            UpdateFileNamesDownloadSuccess(context, serviceProviderId, file.FilePath, REPORT_TYPE_MUBU);
                        }
                    }
                    catch (Exception ex)
                    {
                        transactionScope.Dispose();
                        // save filename
                        LogInfo(context, "WARN", $"Error download again file name {file.FilePath}. Error detail: {ex.Message} - {ex.StackTrace}");
                        if (shouldRetrySync && retryDataTable != null)
                        {
                            AddToDataRow(retryDataTable, file, ex.Message, serviceProviderId, REPORT_TYPE_MUBU);
                        }
                        return usage;
                    }
                }
                transactionScope.Complete();
            }

            return usage;
        }


        private DataTable InitFileNameDownloadFailedDataTable()
        {
            DataTable table = new DataTable();
            table.Columns.Add("Id");
            table.Columns.Add("FileName");
            table.Columns.Add("Status");
            table.Columns.Add("ErrorDetail");
            table.Columns.Add("ReportType");
            table.Columns.Add("WriteTime");
            table.Columns.Add("ServiceProviderId");
            table.Columns.Add("CreatedBy");
            table.Columns.Add("CreatedDate");
            table.Columns.Add("ModifiedBy");
            table.Columns.Add("ModifiedDate");

            return table;
        }

        private void AddToDataRow(DataTable table, UsageFile usageFile, string errorDetail, int serviceProviderId, string reportType)
        {
            var dr = table.NewRow();
            dr[1] = usageFile.FilePath;
            dr[2] = "FAILED";
            dr[3] = errorDetail;
            dr[4] = reportType;
            dr[5] = usageFile.WriteTime;
            dr[6] = serviceProviderId;
            dr[7] = "Telegence AWS Get Device Usage Lambda";
            dr[8] = DateTime.UtcNow;
            dr[9] = null;
            dr[10] = null;
            table.Rows.Add(dr);
        }

        private List<string> GetFileNamesDownloadFailed(KeySysLambdaContext context, int serviceProviderId, string reportType)
        {
            LogInfo(context, "SUB", $"GetFileNamesDownloadFailed({serviceProviderId})");

            var fileNameList = new List<string>();
            var queryString = @"SELECT Id FROM TelegenceSFTPFileDownloadStatus
                                    WHERE [ReportType] = @reportType
                                    AND [Status] = @status
                                    AND [ServiceProviderId] = @serviceProviderId";
            using (var con = new SqlConnection(context.CentralDbConnectionString))
            {
                using (var cmd = con.CreateCommand())
                {
                    cmd.CommandType = CommandType.Text;
                    cmd.CommandText = queryString;
                    cmd.Parameters.AddWithValue("@reportType", reportType);
                    cmd.Parameters.AddWithValue("@status", "FAILED");
                    cmd.Parameters.AddWithValue("@serviceProviderId", serviceProviderId);

                    con.Open();
                    SqlDataReader rdr = cmd.ExecuteReader();
                    while (rdr.Read())
                    {
                        if (!string.IsNullOrEmpty(rdr[0].ToString()))
                            fileNameList.Add(rdr[0].ToString());
                    }
                }
            }
            return fileNameList;
        }

        private List<string> GetFilesDownLoaded(KeySysLambdaContext context, DateTime startDate, DateTime endDate)
        {
            LogInfo(context, LogTypeConstant.Sub, "");

            var fileNameList = new List<string>();
            var today = DateTime.UtcNow;
            var queryString = @"SELECT FtpFileName FROM TelegenceMubuFtpFile WHERE [CreatedDate] BETWEEN @startDate AND @endDate";
            using (var con = new SqlConnection(context.CentralDbConnectionString))
            {
                using (var cmd = con.CreateCommand())
                {
                    cmd.CommandType = CommandType.Text;
                    cmd.CommandText = queryString;
                    cmd.Parameters.AddWithValue("@startDate", startDate);
                    cmd.Parameters.AddWithValue("@endDate", endDate);

                    con.Open();
                    SqlDataReader rdr = cmd.ExecuteReader();
                    while (rdr.Read())
                    {
                        if (!string.IsNullOrEmpty(rdr[0].ToString()))
                            fileNameList.Add(rdr[0].ToString());
                    }
                }
            }
            return fileNameList;
        }

        private List<DateTime> GetLatestUsageFileFTP(KeySysLambdaContext context, int serviceProviderId, string reportType)
        {
            LogInfo(context, "SUB", $"GetLatestUsageFileFTP({serviceProviderId})");
            List<DateTime> writeTimes = new List<DateTime>();

            var queryString = @"SELECT TOP 1 CreatedDate
                                 FROM [TelegenceMubuFtpFile] 
                                 WHERE FtpFileName like '%_voice.zip%'
                                 order by createdDate desc";
            using (var con = new SqlConnection(context.CentralDbConnectionString))
            {
                using (var cmd = con.CreateCommand())
                {
                    cmd.CommandType = CommandType.Text;
                    cmd.CommandText = queryString;

                    con.Open();
                    SqlDataReader rdr = cmd.ExecuteReader();
                    while (rdr.Read())
                    {
                        writeTimes.Add(DateTime.Parse(rdr[0].ToString()));
                    }
                }
            }

            var queryDataString = @"SELECT TOP 1 CreatedDate
                                 FROM [TelegenceMubuFtpFile] 
                                 WHERE FtpFileName like '%_data.zip%'
                                 order by createdDate desc";
            using (var con = new SqlConnection(context.CentralDbConnectionString))
            {
                using (var cmd = con.CreateCommand())
                {
                    cmd.CommandType = CommandType.Text;
                    cmd.CommandText = queryDataString;

                    con.Open();
                    SqlDataReader rdr = cmd.ExecuteReader();
                    while (rdr.Read())
                    {
                        writeTimes.Add(DateTime.Parse(rdr[0].ToString()));
                    }
                }
            }
            return writeTimes;
        }

        private TelegenceSFTPFileDownloadStatus GetFileDownloadFailedByFileName(KeySysLambdaContext context, int serviceProviderId, string reportType, int Id)
        {
            LogInfo(context, "SUB", $"GetFileNamesDownloadFailed({serviceProviderId})");

            var usageFileFailed = new TelegenceSFTPFileDownloadStatus();
            var queryString = @"SELECT Id, FileName, WriteTime, ServiceProviderId FROM TelegenceSFTPFileDownloadStatus
                                   WHERE [Id] = @Id";
            using (var con = new SqlConnection(context.CentralDbConnectionString))
            {
                using (var cmd = con.CreateCommand())
                {
                    cmd.CommandType = CommandType.Text;
                    cmd.CommandText = queryString;
                    cmd.Parameters.AddWithValue("@Id", Id);

                    con.Open();
                    SqlDataReader rdr = cmd.ExecuteReader();
                    while (rdr.Read())
                    {
                        usageFileFailed.Id = int.Parse(rdr[0].ToString());
                        usageFileFailed.FileName = rdr[1].ToString();
                        usageFileFailed.WriteTime = DateTime.Parse(rdr[2].ToString());
                        usageFileFailed.ServiceProviderId = int.Parse(rdr[3].ToString());
                    }
                }
            }
            return usageFileFailed;
        }

        private void UpdateFileNamesDownloadSuccess(KeySysLambdaContext context, int serviceProviderId, string fileName, string reportType)
        {
            LogInfo(context, "SUB", $"UpdateFileNamesDownloadSuccess({serviceProviderId})");

            var queryString = @"UPDATE TelegenceSFTPFileDownloadStatus
                                SET [Status] = @status
                                WHERE [ReportType] = @reportType
                                AND [FileName] = @fileName
                                AND [ServiceProviderId] = @serviceProviderId";
            using (var con = new SqlConnection(context.CentralDbConnectionString))
            {
                using (var cmd = con.CreateCommand())
                {
                    cmd.CommandType = CommandType.Text;
                    cmd.CommandText = queryString;
                    cmd.Parameters.AddWithValue("@reportType", reportType);
                    cmd.Parameters.AddWithValue("@status", "SUCCESS");
                    cmd.Parameters.AddWithValue("@fileName", fileName);
                    cmd.Parameters.AddWithValue("@serviceProviderId", serviceProviderId);

                    con.Open();
                    cmd.ExecuteNonQuery();
                }
            }
        }

        private async Task<DataTable> GetLatestMubuUsage(KeySysLambdaContext context, DataTable fileDownloadAgainDt, int serviceProviderId, string username, string password, string server, string path, UsageFile newestFile)
        {
            LogInfo(context, "SUB", $"GetLatestMubuUsage({serviceProviderId},{username},,{server},{path})");

            return await InsertMUBURecords(context, fileDownloadAgainDt, serviceProviderId, username, password, server, path, newestFile);
        }

        private DataTable GetLatestFinalUsage(KeySysLambdaContext context, int serviceProviderId, string username, string password, string server, string path)
        {
            LogInfo(context, "SUB", $"GetLatestFinalUsage({serviceProviderId},{username},,{server},{path})");
            DataTable usage = null;
            var connectionInfo = new ConnectionInfo(server, username, new PasswordAuthenticationMethod(username, password));
            using (SftpClient client = new SftpClient(connectionInfo))
            {
                client.Connect();

                var newestFile = GetLatestFinalUsageFile(path, client);

                if (!string.IsNullOrEmpty(newestFile.FilePath))
                {
                    LogInfo(context, "INFO", $"Reading file {newestFile.FilePath}");
                    var billingPeriodYear = BillingPeriodYearFromFinalUsageFileName(newestFile.FilePath);
                    var billingPeriodMonth = BillingPeriodMonthFromFinalUsageFileName(newestFile.FilePath);
                    if (HasOpenBillingPeriod(context, serviceProviderId, billingPeriodYear, billingPeriodMonth))
                    {
                        LogInfo(context, "INFO", $"Has open/pending Billing Period");
                        using (var ms = new MemoryStream())
                        {
                            client.DownloadFile(newestFile.FilePath, ms);

                            ms.Position = 0;
                            using (var archive = new System.IO.Compression.ZipArchive(ms))
                            {
                                using (var stream = archive.Entries.First().Open())
                                {
                                    var fileReader = new PremierFinalUsageReportReader();
                                    var isXlsFormat = newestFile.FilePath.Contains(".xls");
                                    LogInfo(context, "SUB", $"The format file is Xls: {isXlsFormat}");
                                    usage = fileReader.ReadRecords(serviceProviderId, newestFile.WriteTime, stream, isXlsFormat);
                                    usage.TableName = newestFile.FilePath;
                                }
                            }
                        }
                    }
                }
            }

            return usage;
        }

        private int InsertMubuTableNameMapping(KeySysLambdaContext context, UsageFile file)
        {
            var fileId = -1;
            using (var Conn = new SqlConnection(context.CentralDbConnectionString))
            {
                using (var Cmd = new SqlCommand(
                    @"INSERT INTO [TelegenceMubuFtpFile] ([FtpFileName], [CreatedDate], [WriteTimeUTC])
                        OUTPUT INSERTED.id
                        SELECT @FileName, @WriteTime, @WriteTimeUTC
                        WHERE NOT EXISTS (SELECT 1 FROM [TelegenceMubuFtpFile] WHERE FtpFileName =@FileName);", Conn))
                {
                    Cmd.CommandType = CommandType.Text;
                    Cmd.Parameters.AddWithValue("@FileName", file.FilePath);
                    Cmd.Parameters.AddWithValue("@WriteTime", file.WriteTime);
                    Cmd.Parameters.AddWithValue("@WriteTimeUTC", file.WriteTimeUtc);
                    Cmd.CommandTimeout = SQLConstant.TimeoutSeconds;
                    Conn.Open();
                    var resultId = Cmd.ExecuteScalar();
                    if (resultId != null)
                    {
                        fileId = (int)resultId;
                    }
                    Conn.Close();
                }
            }
            return fileId;
        }

        private void MarkMubuFileAsBlank(KeySysLambdaContext context, int mubuFtpFileId)
        {
            LogInfo(context, "SUB", $"MarkMubuFileAsBlank({mubuFtpFileId})");
            using (var Conn = new SqlConnection(context.CentralDbConnectionString))
            {
                using (var Cmd = new SqlCommand(
                    @"UPDATE [TelegenceMubuFtpFile]
                        SET [IsFileBlank] = @isFileBlank,
                            [HasNotificationBeenSent] = @hasNotificationBeenSent
                        WHERE Id = @id AND [HasNotificationBeenSent] IS NULL", Conn))
                {
                    Cmd.CommandType = CommandType.Text;
                    Cmd.Parameters.AddWithValue("@isFileBlank", true);
                    Cmd.Parameters.AddWithValue("@hasNotificationBeenSent", false);
                    Cmd.Parameters.AddWithValue("@id", mubuFtpFileId);
                    Cmd.CommandTimeout = SQLConstant.TimeoutSeconds;

                    Conn.Open();
                    Cmd.ExecuteNonQuery();
                    Conn.Close();
                }
            }
        }

        private void UpdateBlankFileNotifyComplete(KeySysLambdaContext context)
        {
            LogInfo(context, "SUB", $"UpdateBlankFileNotifyComplete()");
            using (var Conn = new SqlConnection(context.CentralDbConnectionString))
            {
                using (var Cmd = new SqlCommand(
                    @"UPDATE [TelegenceMubuFtpFile]
                        SET [IsFileBlank] = @isFileBlank,
                            [HasNotificationBeenSent] = @hasNotificationBeenSent
                        WHERE [HasNotificationBeenSent] = 0", Conn))
                {
                    Cmd.CommandType = CommandType.Text;
                    Cmd.Parameters.AddWithValue("@isFileBlank", true);
                    Cmd.Parameters.AddWithValue("@hasNotificationBeenSent", true);
                    Cmd.CommandTimeout = SQLConstant.TimeoutSeconds;

                    Conn.Open();
                    Cmd.ExecuteNonQuery();
                    Conn.Close();
                }
            }
        }

        private List<UsageFile> GetFileBlankNotify(KeySysLambdaContext context)
        {
            LogInfo(context, "SUB", $"GetFileBlankNotify()");
            var usageFileList = new List<UsageFile>();

            using (var Conn = new SqlConnection(context.CentralDbConnectionString))
            {
                using (var Cmd = new SqlCommand(
                    @"SELECT [FtpFileName], [CreatedDate], [WriteTimeUTC] FROM [TelegenceMubuFtpFile] WHERE [IsFileBlank] = @isFileBlank AND [HasNotificationBeenSent] = @hasNotificationBeenSent", Conn))
                {
                    Cmd.CommandType = CommandType.Text;
                    Cmd.Parameters.AddWithValue("@isFileBlank", true);
                    Cmd.Parameters.AddWithValue("@hasNotificationBeenSent", false);
                    Cmd.CommandTimeout = SQLConstant.TimeoutSeconds;

                    Conn.Open();

                    using (var reader = Cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var file = new UsageFile()
                            {
                                FilePath = reader["FtpFileName"].ToString(),
                                WriteTime = DateTime.Parse(reader["CreatedDate"].ToString()),
                                WriteTimeUtc = DateTime.Parse(reader["WriteTimeUTC"].ToString()),
                            };
                            usageFileList.Add(file);
                        }
                    }

                    Conn.Close();
                }
            }

            return usageFileList;
        }

        private int GetMubuFtpFileNameId(KeySysLambdaContext context, string fileName)
        {
            var fileId = -1;

            try
            {
                using (var Conn = new SqlConnection(context.CentralDbConnectionString))
                {
                    Conn.Open();

                    using (var Cmd = new SqlCommand(
                        @"SELECT TOP 1 Id
                            FROM [TelegenceMubuFtpFile] 
                            WHERE FtpFileName = @FileName", Conn))
                    {
                        Cmd.CommandType = CommandType.Text;
                        Cmd.Parameters.AddWithValue("@FileName", fileName);
                        Cmd.CommandTimeout = 800;
                        var resultId = Cmd.ExecuteScalar();
                        if (resultId != null)
                        {
                            fileId = (int)resultId;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                LogInfo(context, "EXCEPTION", $"GetMubuFtpFileNameId({fileName}) - {ex.Message}");
            }

            return fileId;
        }

        private bool IsBlankFileFromDB(KeySysLambdaContext context, string fileName)
        {
            LogInfo(context, LogTypeConstant.Sub, $"({fileName})");

            var isBlankFile = false;
            try
            {
                using (var Conn = new SqlConnection(context.CentralDbConnectionString))
                {
                    Conn.Open();

                    using (var Cmd = new SqlCommand(
                        @"SELECT TOP 1 [Id]
                            FROM [TelegenceMubuFtpFile] 
                            WHERE [FtpFileName] = @FileName
                            AND [IsFileBlank] = @isBlankFile
	                        AND [HasNotificationBeenSent] = @hasNotificationBeenSent", Conn))
                    {
                        Cmd.CommandType = CommandType.Text;
                        Cmd.Parameters.AddWithValue("@FileName", fileName);
                        Cmd.Parameters.AddWithValue("@isBlankFile", true);
                        Cmd.Parameters.AddWithValue("@hasNotificationBeenSent", true);
                        Cmd.CommandTimeout = SQLConstant.TimeoutSeconds;

                        var resultId = Cmd.ExecuteScalar();
                        if (resultId != null)
                        {
                            isBlankFile = (int)resultId > -1;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                LogInfo(context, LogTypeConstant.Exception, $"IsBlankFileFromDB({fileName}) - {ex.Message}");
            }

            return isBlankFile;
        }

        private int GetRecordCountFromDatabase(KeySysLambdaContext context, int savedFileId)
        {
            var recordCount = 0;

            try
            {
                using (var Conn = new SqlConnection(context.CentralDbConnectionString))
                {
                    Conn.Open();

                    using (var Cmd = new SqlCommand(
                        @"SELECT COUNT(*) 
                                    FROM [TelegenceDeviceUsageMubuStaging] 
                                    WHERE [FtpFileId] = @FileId", Conn))
                    {
                        Cmd.CommandType = CommandType.Text;
                        Cmd.Parameters.AddWithValue("@FileId", savedFileId);
                        Cmd.CommandTimeout = 800;
                        var resultId = Cmd.ExecuteScalar();
                        if (resultId != null)
                        {
                            recordCount = (int)resultId;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                LogInfo(context, "EXCEPTION", $"GetRecordCountFromDatabase({savedFileId}) - {ex.Message}");
            }

            return recordCount;
        }

        public static int BillingPeriodYearFromFinalUsageFileName(string fileName)
        {
            // grab date part
            var datePart = DatePartFromFinalUsageFileName(fileName);

            // grab year part
            var yearPart = datePart.Substring(4, 4);

            // return year as int
            return int.Parse(yearPart);
        }

        public static int BillingPeriodMonthFromFinalUsageFileName(string fileName)
        {
            // grab date part
            var datePart = DatePartFromFinalUsageFileName(fileName);

            // grab month part
            var monthPart = datePart.Substring(0, 2).TrimStart('0');

            // return month as int
            return int.Parse(monthPart);
        }

        public static string DatePartFromFinalUsageFileName(string fileName)
        {
            // find prefix
            var prefixIndex = fileName.IndexOf("CUS_FANALL_", StringComparison.InvariantCulture);

            // grab date part (MM00YYYY)
            return fileName.Substring(prefixIndex + 11, 8);
        }

        private bool HasOpenBillingPeriod(KeySysLambdaContext context, int serviceProviderId, int billingPeriodYear, int billingPeriodMonth)
        {
            LogInfo(context, "SUB", $"HasOpenBillingPeriod({serviceProviderId},{billingPeriodYear},{billingPeriodMonth})");
            using (var conn = new SqlConnection(context.CentralDbConnectionString))
            {
                using (var cmd = new SqlCommand("usp_BillingPeriodIsPending", conn))
                {
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.CommandTimeout = 120;
                    cmd.Parameters.AddWithValue("@ServiceProviderId", serviceProviderId);
                    cmd.Parameters.AddWithValue("@BillingPeriodYear", billingPeriodYear);
                    cmd.Parameters.AddWithValue("@BillingPeriodMonth", billingPeriodMonth);

                    conn.Open();

                    var billingPeriodCount = (int)cmd.ExecuteScalar();
                    return billingPeriodCount > 0;
                }
            }
        }

        private bool HasRecentMubuUsage(UsageFile usageFile)
        {
            return HasMubuRecentlyBeenSynced(usageFile) && !string.IsNullOrEmpty(usageFile.FilePath);
        }

        private bool HasMubuRecentlyBeenSynced(UsageFile usageFile)
        {
            // should be delivered every three hours (6 hour gap indicates that one was missed)
            return usageFile.WriteTime >= DateTime.Now.AddHours(-6);
        }

        private bool HasRecentUsage(UsageFile usageFile)
        {
            return HasRecentlyBeenSynced(usageFile) && !string.IsNullOrEmpty(usageFile.FilePath);
        }

        private bool HasRecentlyBeenSynced(UsageFile usageFile)
        {
            return usageFile.WriteTime >= DateTime.Now.AddDays(FtpReportNotificationThresholdDays * -1);
        }

        private static UsageFile GetLatestUsageFile(string fan, string path, SftpClient client)
        {
            string fileNamePrefix = $"ALL_BAN_FULL_DATA_EXPORT_REPORT_{fan}_";
            var newestFile = new UsageFile { FilePath = string.Empty, WriteTime = DateTime.MinValue };

            foreach (Renci.SshNet.Sftp.SftpFile item in client.ListDirectory(path))
            {
                if (item.IsRegularFile)
                {
                    if (item.FullName.Contains(fileNamePrefix) && item.FullName.EndsWith(".zip"))
                    {
                        var time = item.LastWriteTime;
                        if (time > newestFile.WriteTime)
                        {
                            newestFile.WriteTime = time;
                            newestFile.FilePath = item.FullName;
                        }
                    }
                }
            }

            return newestFile;
        }

        private static UsageFile GetLatestMubuVoiceFile(string path, SftpClient client)
        {
            var newestFile = new UsageFile { FilePath = string.Empty, WriteTime = DateTime.MinValue };

            foreach (Renci.SshNet.Sftp.SftpFile item in client.ListDirectory(path))
            {
                if (item.IsRegularFile)
                {
                    if (item.FullName.EndsWith("_voice.zip"))
                    {
                        var time = item.LastWriteTime;
                        if (time > newestFile.WriteTime)
                        {
                            newestFile.WriteTime = time;
                            newestFile.FilePath = item.FullName;
                        }
                    }
                }
            }

            return newestFile;
        }

        private static List<UsageFile> GetLatestMubuVoiceFileList(string path, SftpClient client, int limitFilesDownloadNumber, List<string> fileNameThresholdDays, DateTime startDate, DateTime endDate)
        {
            var newestFileList = new List<UsageFile>();

            foreach (Renci.SshNet.Sftp.SftpFile item in client.ListDirectory(path))
            {
                if (item.IsRegularFile)
                {
                    if (item.FullName.EndsWith("_voice.zip"))
                    {
                        // get files latest
                        var timeinSFTP = item.LastWriteTime;
                        var isNotExists = fileNameThresholdDays.Any(x => x.Equals(item.FullName));
                        if (timeinSFTP >= startDate && timeinSFTP <= endDate && !isNotExists)
                        {
                            var newestFile = new UsageFile()
                            {
                                WriteTime = timeinSFTP,
                                FilePath = item.FullName,
                                WriteTimeUtc = item.LastWriteTimeUtc
                            };
                            newestFileList.Add(newestFile);
                        }
                    }
                }
            }

            if (newestFileList.Count > 0)
            {
                newestFileList = newestFileList.OrderByDescending(x => x.WriteTime).Take(limitFilesDownloadNumber).ToList();
            }
            return newestFileList;
        }

        private static UsageFile GetLatestMubuUsageFile(string path, SftpClient client)
        {
            var newestFile = new UsageFile { FilePath = string.Empty, WriteTime = DateTime.MinValue };

            foreach (Renci.SshNet.Sftp.SftpFile item in client.ListDirectory(path))
            {
                if (item.IsRegularFile)
                {
                    if (item.FullName.EndsWith("_data.zip"))
                    {
                        var time = item.LastWriteTime;

                        if (time > newestFile.WriteTime)
                        {
                            newestFile.WriteTime = time;
                            newestFile.FilePath = item.FullName;
                        }
                    }
                }
            }

            return newestFile;
        }

        private static List<UsageFile> GetLatestMubuUsageFileList(string path, SftpClient client, int limitFilesDownloadNumber, List<string> fileNameThresholdDays, DateTime startDate, DateTime endDate)
        {
            var newestFileList = new List<UsageFile>();

            foreach (Renci.SshNet.Sftp.SftpFile item in client.ListDirectory(path))
            {
                if (item.IsRegularFile)
                {
                    if (item.FullName.EndsWith("_data.zip"))
                    {
                        var timeinSFTP = item.LastWriteTime;
                        var isNotExists = fileNameThresholdDays.Any(x => x.Equals(item.FullName));
                        if (timeinSFTP >= startDate && timeinSFTP <= endDate && !isNotExists)
                        {
                            var newestFile = new UsageFile()
                            {
                                WriteTime = timeinSFTP,
                                FilePath = item.FullName,
                                WriteTimeUtc = item.LastWriteTimeUtc
                            };
                            newestFileList.Add(newestFile);
                        }
                    }
                }
            }

            if (newestFileList.Count > 0)
            {
                newestFileList = newestFileList.OrderByDescending(x => x.WriteTime).Take(limitFilesDownloadNumber).ToList();
            }

            return newestFileList;
        }

        private static UsageFile GetMubuFileWriteTimes(string path, SftpClient client, string filePath)
        {
            var newestFileList = new List<UsageFile>();
            var mubuFileList = client.ListDirectory(path).ToList();

            return mubuFileList.Where(x => x.FullName.Equals(filePath)).Select(x => new UsageFile()
            {
                WriteTime = x.LastWriteTime,
                FilePath = x.FullName,
                WriteTimeUtc = x.LastWriteTimeUtc
            }).FirstOrDefault();
        }

        private static UsageFile GetLatestFinalUsageFile(string path, SftpClient client)
        {
            var newestFile = new UsageFile { FilePath = string.Empty, WriteTime = DateTime.MinValue };

            foreach (Renci.SshNet.Sftp.SftpFile item in client.ListDirectory(path))
            {
                if (item.IsRegularFile)
                {
                    if (item.FullName.Contains("CUS_FANALL_") && item.FullName.EndsWith(".zip"))
                    {
                        var time = item.LastWriteTime;

                        if (time > newestFile.WriteTime)
                        {
                            newestFile.WriteTime = time;
                            newestFile.FilePath = item.FullName;
                        }
                    }
                }
            }

            return newestFile;
        }

        private void CleanUpFtp(KeySysLambdaContext context, string username, string password, string server, string path)
        {
            LogInfo(context, "SUB", "CleanUpFtp");
            var connectionInfo = new ConnectionInfo(server, username, new PasswordAuthenticationMethod(username, password));

            if (!int.TryParse(DaysToKeep, out var daysToKeep))
            {
                daysToKeep = 90;
            }

            using (var client = new SftpClient(connectionInfo))
            {
                client.Connect();

                var cutOffTime = DateTime.Now.AddDays(-1 * daysToKeep);
                foreach (Renci.SshNet.Sftp.SftpFile item in client.ListDirectory(path))
                {
                    if (item != null && item.IsRegularFile)
                    {
                        var time = item.LastWriteTime;
                        if (time < cutOffTime)
                        {
                            try
                            {
                                item.Delete();
                            }
                            catch (Exception ex)
                            {
                                LogInfo(context, "EXCEPTION", $"Delete file: {item.Name} failed by follow issue {ex.Message} {ex.StackTrace}");
                            }
                        }
                    }
                }
            }
        }

        private async Task StartDailyDeviceUsageProcessingAsync(KeySysLambdaContext context, bool fromCloudwatchEvent = false)
        {
            LogInfo(context, "SUB", "StartDailyDeviceUsageProcessingAsync");
            InitializeSync(context, context.CentralDbConnectionString);
            ClearQueue(context, context.CentralDbConnectionString);

            var currentServiceProviderId = ServiceProviderCommon.GetNextServiceProviderId(context.CentralDbConnectionString, Amop.Core.Models.IntegrationType.Telegence, 0);
            while (currentServiceProviderId > 0)
            {
                // Add Provider to Queue
                await AddProviderToQueueAsync(context, context.CentralDbConnectionString, currentServiceProviderId, DeviceNotificationQueueURL, fromCloudwatchEvent);

                if (!fromCloudwatchEvent)
                {
                    // queue cleanup
                    await SendNotificationMessageToQueueAsync(context, currentServiceProviderId);
                }

                // Get Next Provider
                currentServiceProviderId = ServiceProviderCommon.GetNextServiceProviderId(context.CentralDbConnectionString, Amop.Core.Models.IntegrationType.Telegence, currentServiceProviderId);
            }
        }

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

                using (var conn = new SqlConnection(dbConnectionString))
                {
                    using (var cmd = new SqlCommand("insert into [dbo].[TelegenceDeviceUsageLastSyncDate](LastSyncDate, QueueCount) SELECT GETDATE(), 0", conn))
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

        private void UpdateTelegenceFinalUsageFromStaging(KeySysLambdaContext context, int serviceProviderId,
            int billingPeriodYear, int billingPeriodMonth)
        {
            LogInfo(context, "SUB", $"UpdateTelegenceFinalUsageFromStaging({serviceProviderId},{billingPeriodYear},{billingPeriodMonth})");
            using (var conn = new SqlConnection(context.CentralDbConnectionString))
            {
                using (var cmd = new SqlCommand("usp_Telegence_Update_DeviceFinalUsage_FromStaging", conn))
                {
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.CommandTimeout = 240;
                    cmd.Parameters.AddWithValue("@ServiceProviderId", serviceProviderId);
                    cmd.Parameters.AddWithValue("@BillingPeriodYear", billingPeriodYear);
                    cmd.Parameters.AddWithValue("@BillingPeriodMonth", billingPeriodMonth);

                    conn.Open();

                    cmd.ExecuteNonQuery();
                }
            }
        }

        private void UpdateTelegenceMubuUsageFromStaging(KeySysLambdaContext context, int serviceProviderId)
        {
            LogInfo(context, "SUB", $"UpdateTelegenceMubuUsageFromStaging({serviceProviderId})");
            using (var conn = new SqlConnection(context.CentralDbConnectionString))
            {
                using (var cmd = new SqlCommand("usp_Telegence_Update_DeviceMubuUsage_FromStaging", conn))
                {
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.CommandTimeout = 800;
                    cmd.Parameters.AddWithValue("@ServiceProviderId", serviceProviderId);

                    conn.Open();

                    cmd.ExecuteNonQuery();
                }
            }
        }

        private void UpdateTelegenceKafkaUsage(KeySysLambdaContext context, int serviceProviderId)
        {
            LogInfo(context, Amop.Core.Constants.CommonConstants.SUB, $"({serviceProviderId})");

            var parameters = new List<SqlParameter>()
                {
                    new SqlParameter(Amop.Core.Constants.CommonSQLParameterNames.SERVICE_PROVIDER_ID_PASCAL_CASE, serviceProviderId)
                };

            SqlQueryHelper.ExecuteStoredProcedureWithRowCountResult((type, message) =>
                ParameterizedLog(context),
                context.CentralDbConnectionString,
                Amop.Core.Constants.SQLConstant.StoredProcedureName.TELEGENCE_UPDATE_DEVICE_KAFKA_USAGE,
                parameters,
                Amop.Core.Constants.SQLConstant.TimeoutSeconds);
        }

        private void UpdateMobilityMubuUsageFromTelegence(KeySysLambdaContext context, int serviceProviderId)
        {
            LogInfo(context, "SUB", $"UpdateMobilityMubuUsageFromTelegence({serviceProviderId})");
            using (var conn = new SqlConnection(context.CentralDbConnectionString))
            {
                using (var cmd = new SqlCommand("usp_Telegence_DeviceSync", conn))
                {
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.CommandTimeout = 800;
                    cmd.Parameters.AddWithValue("@ServiceProviderId", serviceProviderId);

                    conn.Open();

                    cmd.ExecuteNonQuery();
                }
            }
        }

        private void UpdateLateMubuUsageFromTelegence(KeySysLambdaContext context, int serviceProviderId)
        {
            LogInfo(context, "SUB", $"UpdateLateMubuUsageFromTelegence({serviceProviderId})");
            using (var conn = new SqlConnection(context.CentralDbConnectionString))
            {
                using (var cmd = new SqlCommand("usp_MobilityDeviceUsage_UpdateLateRecords", conn))
                {
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.CommandTimeout = SQLConstant.TimeoutSeconds;
                    cmd.Parameters.AddWithValue("@ServiceProviderId", serviceProviderId);

                    conn.Open();

                    cmd.ExecuteNonQuery();
                }
            }
        }

        private static void TruncateTableByTableName(KeySysLambdaContext context, string tableName)
        {
            LogInfo(context, "SUB", $"TruncateTableByTableName: {tableName}");
            using (var Conn = new SqlConnection(context.CentralDbConnectionString))
            {
                using (var Cmd = new SqlCommand($"TRUNCATE TABLE dbo.{tableName}", Conn))
                {
                    Cmd.CommandType = CommandType.Text;
                    Cmd.CommandTimeout = 800;
                    Conn.Open();
                    Cmd.ExecuteNonQuery();
                    Conn.Close();
                }
            }
        }

        private async Task AddProviderToQueueAsync(KeySysLambdaContext context, string dbConnectionString, int serviceProviderId, string deviceNotificationQueueURL, bool fromCloudwatchEvent)
        {
            try
            {
                var now = DateTime.UtcNow;
                var telegenceAuth = TelegenceCommon.GetTelegenceAuthenticationInformation(context.CentralDbConnectionString, serviceProviderId);
                var billPeriodEndDay = telegenceAuth.BillPeriodEndDay;

                if (IsPremiereReportDelaySimulator == 1)
                {
                    LogInfo(context, "INFO", $"Bill Period End Day Simulator: {DayEndBillingSimulator};");
                    billPeriodEndDay = DayEndBillingSimulator;
                }

                var zeroDay = billPeriodEndDay + 1;
                var settings = context.SettingsRepo.GetTelegenceDeviceSettings(serviceProviderId);
                var mubuPath = settings.TelegenceFtpMubuPath;

                // check to see if new billingperiod has been created yet
                // Only zero out usage if we are in new billing period
                // In worse case where new billing period is delayed, zero out the usage in dbo.usp_Telegence_Update_DeviceDetail
                var dateTimeInNextMonth = now.AddMonths(1);
                var nextBillingPeriod = BillingPeriodHelper.GetBillingPeriodForServiceProvider(context.CentralDbConnectionString, serviceProviderId, dateTimeInNextMonth.Year, dateTimeInNextMonth.Month, context.OptimizationSettings.BillingTimeZone);

                // Since it takes AT&T a couple of days to switch over usage to new bill cycle, we need to zero out usage
                // from the old bill cycle for a couple of days so it doesn't linger and pollute the data
                if (now.Day == zeroDay && nextBillingPeriod.Id > 0)
                {
                    LogInfo(context, "INFO", $"Zeroing out usage for the new bill cycle. Bill period end day: {billPeriodEndDay}");
                    ZeroOutUsage(context.CentralDbConnectionString, serviceProviderId, context.logger);
                }
                else if (string.IsNullOrWhiteSpace(mubuPath) && (now.Day >= zeroDay + 1 && now.Day <= zeroDay + (PremiereReportDelayDays - 1)) && nextBillingPeriod.Id > 0) //(now.Day == zeroDay + 1 || now.Day == zeroDay + 2)
                {
                    // only keep zeroing if there are no MUBU reports to load
                    LogInfo(context, "INFO", $"Zeroing out usage for the new bill cycle. Bill period end day: {billPeriodEndDay}");
                    ZeroOutUsage(context.CentralDbConnectionString, serviceProviderId, context.logger, PremiereReportDelayDays - 1);
                }

                var fanList = GetFoundationAccountList(context, dbConnectionString, serviceProviderId);

                if (fanList != null)
                {
                    using (var conn = new SqlConnection(dbConnectionString))
                    {
                        conn.Open();

                        if (!string.IsNullOrWhiteSpace(settings.TelegenceFtpUsername) && !string.IsNullOrWhiteSpace(settings.TelegenceFtpPassword))
                        {
                            var server = settings.TelegenceFtpServer;
                            var path = settings.TelegenceFtpPath;
                            var username = settings.TelegenceFtpUsername;
                            var password = context.Base64Service.Base64Decode(settings.TelegenceFtpPassword);
                            var finalUsagePath = settings.TelegenceFtpFinalUsagePath;

                            var connectionInfo = new ConnectionInfo(server, username, new PasswordAuthenticationMethod(username, password));
                            using (SftpClient client = new SftpClient(connectionInfo))
                            {
                                client.Connect();
                                if (now.Day >= billPeriodEndDay && now.Day <= billPeriodEndDay + (PremiereReportDelayDays - 1))
                                {
                                    context.logger.LogInfo("INFO", $"Skipping Premiere Report Download in First {PremiereReportDelayDays} Days of the Billing Cycle");
                                }
                                else
                                {
                                    foreach (var fan in fanList)
                                    {
                                        var latestUsageFile = GetLatestUsageFile(fan, path, client);
                                        if (HasRecentUsage(latestUsageFile))
                                        {
                                            InsertTelegenceUsageQueueRecord(fan, serviceProviderId, deviceNotificationQueueURL, conn);

                                            await SendProcessMessageToQueueAsync(context, serviceProviderId, fan, REPORT_TYPE_PREMIER, fromCloudwatchEvent);
                                        }
                                        else
                                        {
                                            context.logger.LogInfo("WARNING", $"Stale usage for FAN {fan} - last write time: {latestUsageFile.WriteTime}");
                                        }
                                    }
                                }

                                // is MUBU even defined/configured?
                                if (!string.IsNullOrWhiteSpace(mubuPath))
                                {
                                    // check MUBU voice
                                    var latestMubuVoice = GetLatestMubuVoiceFile(mubuPath, client);
                                    // MUBU voice is once daily, not every 3 hours
                                    var hasMubuVoice = HasRecentUsage(latestMubuVoice);
                                    if (!hasMubuVoice)
                                    {
                                        context.logger.LogInfo("WARNING", $"Stale voice usage for MUBU - last write time: {latestMubuVoice.WriteTime}");

                                        var serviceProvider = ServiceProviderCommon.GetServiceProvider(context.CentralDbConnectionString, serviceProviderId);
                                        var serviceProviderName = serviceProvider.DisplayName;

                                        int tenantId = serviceProvider.TenantId ?? 0;
                                        string tenantName = context.TenantRepo.GetTenantNameByTenantId(tenantId);
                                        var subject = $"{serviceProviderName} MUBU Voice Report not being delivered ({tenantName})";
                                        var body = BuildStaleMubuVoiceSyncNotificationBody(serviceProviderName, latestMubuVoice);
                                        await SendEmailAsync(context, subject, body);
                                    }

                                    // check MUBU usage
                                    var latestMubuUsage = GetLatestMubuUsageFile(mubuPath, client);
                                    var hasMubuUsage = HasRecentMubuUsage(latestMubuUsage);
                                    if (!hasMubuUsage)
                                    {
                                        context.logger.LogInfo("WARNING", $"Stale usage for MUBU - last write time: {latestMubuUsage.WriteTime}");

                                        var serviceProvider = ServiceProviderCommon.GetServiceProvider(context.CentralDbConnectionString, serviceProviderId);
                                        var serviceProviderName = serviceProvider.DisplayName;
                                        var subject = $"{serviceProviderName} MUBU Usage Report not being delivered";
                                        var body = BuildStaleMubuUsageSyncNotificationBody(serviceProviderName, latestMubuUsage);
                                        await SendEmailAsync(context, subject, body);
                                    }

                                    // queue once for usage and/or voice
                                    if (hasMubuUsage || hasMubuVoice)
                                    {
                                        InsertTelegenceUsageQueueRecord(string.Empty, serviceProviderId, deviceNotificationQueueURL, conn);
                                        await SendProcessMessageToQueueAsync(context, serviceProviderId, string.Empty, REPORT_TYPE_MUBU, fromCloudwatchEvent);
                                    }
                                }

                                // is final usage even defined/configured?
                                if (!string.IsNullOrWhiteSpace(finalUsagePath))
                                {
                                    // check Final Usage file
                                    var latestFinalUsage = GetLatestFinalUsageFile(finalUsagePath, client);

                                    // final usage is once monthly
                                    string fileName = latestFinalUsage.FilePath;
                                    if (!string.IsNullOrWhiteSpace(fileName))
                                    {
                                        var billingPeriodYear = BillingPeriodYearFromFinalUsageFileName(fileName);
                                        var billingPeriodMonth = BillingPeriodMonthFromFinalUsageFileName(fileName);

                                        // queue once for usage and/or voice
                                        if (HasOpenBillingPeriod(context, serviceProviderId, billingPeriodYear, billingPeriodMonth))
                                        {
                                            await SendProcessMessageToQueueAsync(context, serviceProviderId, string.Empty, REPORT_TYPE_FINAL, fromCloudwatchEvent);
                                        }
                                        else
                                        {
                                            context.logger.LogInfo("WARNING", $"Final Usage Not Current: {latestFinalUsage.WriteTime}");
                                        }
                                    }
                                }
                            }
                        }
                        if (!string.IsNullOrWhiteSpace(settings.KafkaOauthBearerClientId) && !string.IsNullOrWhiteSpace(settings.KafkaOauthBearerClientSecret) && !string.IsNullOrWhiteSpace(settings.KafkaBootstrapServer))
                        {
                            var sqlRetryPolicy = GetSqlRetryPolicy(context);
                            sqlRetryPolicy.Execute(() => UpdateTelegenceKafkaUsage(context, serviceProviderId));
                            sqlRetryPolicy.Execute(() => UpdateMobilityMubuUsageFromTelegence(context, serviceProviderId));
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                LogInfo(context, "EXCEPTION", $"AddProviderToQueueAsync({serviceProviderId}) - {ex.Message}");
            }
        }

        private static void InsertTelegenceUsageQueueRecord(string fan, int serviceProviderId, string deviceNotificationQueueURL, SqlConnection conn)
        {
            using (var cmd =
                new SqlCommand(
                    "INSERT INTO [dbo].[TelegenceDeviceUsageIdsToProcess](DeviceNotificationQueueURL,ServiceProviderId,RetryCount,FoundationAccountNumber) VALUES(@DeviceNotificationQueueURL, @ServiceProviderId, 0, @FoundationAccountNumber)",
                    conn))
            {
                cmd.CommandType = CommandType.Text;
                cmd.Parameters.AddWithValue("@DeviceNotificationQueueURL", deviceNotificationQueueURL);
                cmd.Parameters.AddWithValue("@ServiceProviderId", serviceProviderId);
                cmd.Parameters.AddWithValue("@FoundationAccountNumber", fan);

                cmd.ExecuteNonQuery();
            }
        }


        private static List<string> GetFoundationAccountList(KeySysLambdaContext context, string dbConnectionString, int serviceProviderId)
        {
            var fanList = new List<string>();

            try
            {
                using (var conn = new SqlConnection(dbConnectionString))
                {
                    conn.Open();

                    using (var cmd = new SqlCommand(
                        @"SELECT DISTINCT td.FoundationAccountNumber
                          FROM TelegenceDevice td
                          LEFT JOIN MobilityDevice md on md.MSISDN = td.SubscriberNumber AND md.ServiceProviderId = td.ServiceProviderId
                          WHERE td.ServiceProviderId = @ServiceProviderId
                          AND td.IsDeleted = 0
                          AND (md.IsDeleted = 0 OR md.IsDeleted IS NULL)", conn))
                    {
                        cmd.CommandType = CommandType.Text;
                        cmd.Parameters.AddWithValue("@ServiceProviderId", serviceProviderId);
                        cmd.CommandTimeout = 800;

                        using (var reader = cmd.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                fanList.Add(reader[0].ToString());
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                LogInfo(context, "EXCEPTION", $"GetFoundationAccountList({serviceProviderId}) - {ex.Message}");
            }

            return fanList;
        }

        private async Task SendProcessMessageToQueueAsync(KeySysLambdaContext context, int serviceProviderId, string fan, string reportType, bool fromCloudwatchEvent, int telegenceSyncDataStep = (int)TelegenceSyncDataStepEnum.None, int delay = DefaultDelaySQS)
        {
            LogInfo(context, "SUB", "SendProcessMessageToQueueAsync");
            LogInfo(context, "InitializeProcessing", false);
            LogInfo(context, "TelegenceDeviceUsageQueueURL", ExportDeviceUsageQueueURL);
            LogInfo(context, "ServiceProviderId", serviceProviderId.ToString());
            LogInfo(context, "FAN", fan);
            LogInfo(context, "ReportType", reportType);
            LogInfo(context, "fromCloudwatchEvent", fromCloudwatchEvent);
            LogInfo(context, "telegenceSyncDataStep", Enum.GetName(typeof(TelegenceSyncDataStepEnum), telegenceSyncDataStep));

            if (string.IsNullOrEmpty(ExportDeviceUsageQueueURL))
            {
                return; // so we don't have to enqueue message in test
            }

            var awsCredentials = AwsCredentials(context);
            using (var client = new AmazonSQSClient(awsCredentials, Amazon.RegionEndpoint.USEast1))
            {
                var requestMsgBody = $"Requesting records to process for Service Provider {serviceProviderId} and FAN {fan}";
                LogInfo(context, "Sending message for", $"{requestMsgBody} to DeviceUsage queue: {ExportDeviceUsageQueueURL}");

                var request = new SendMessageRequest
                {
                    DelaySeconds = (int)TimeSpan.FromSeconds(delay).TotalSeconds,
                    MessageAttributes = new Dictionary<string, MessageAttributeValue>
                    {
                        {"InitializeProcessing", new MessageAttributeValue {DataType = "String", StringValue = false.ToString()}},
                        {"ServiceProviderId", new MessageAttributeValue {DataType = "String", StringValue = serviceProviderId.ToString()}},
                        {"IsFromCloudwatchEvent", new MessageAttributeValue {DataType = "String", StringValue = fromCloudwatchEvent.ToString()}},
                        {"TelegenceSyncDataStep", new MessageAttributeValue {DataType = "String", StringValue = telegenceSyncDataStep.ToString()}},
                    },
                    MessageBody = requestMsgBody,
                    QueueUrl = ExportDeviceUsageQueueURL
                };

                // only add FAN if it's not empty
                if (!string.IsNullOrWhiteSpace(fan))
                {
                    request.MessageAttributes.Add("FAN", new MessageAttributeValue { DataType = "String", StringValue = fan });
                }

                // only add ReportType if it's not empty
                if (!string.IsNullOrWhiteSpace(reportType))
                {
                    request.MessageAttributes.Add("ReportType", new MessageAttributeValue { DataType = "String", StringValue = reportType });
                }

                LogInfo(context, "MessageBody", request.MessageBody);

                var response = await client.SendMessageAsync(request);
                LogInfo(context, "RESPONSE STATUS", $"{response.HttpStatusCode:d} {response.HttpStatusCode:g}");
            }
        }

        private async Task SendMessageToQueueDownloadAgainAsync(KeySysLambdaContext context, int serviceProviderId, bool fromCloudwatchEvent, string reportType, string fileName, int secondDelays)
        {
            LogInfo(context, "SUB", "SendMessageToQueueDownloadAgainAsync");
            LogInfo(context, "ServiceProviderId", serviceProviderId.ToString());
            LogInfo(context, "ReportType", reportType);
            LogInfo(context, "DownloadFailedIds", fileName);

            if (string.IsNullOrEmpty(ExportDeviceUsageQueueURL))
            {
                return; // so we don't have to enqueue message in test
            }

            var awsCredentials = AwsCredentials(context);
            using (var client = new AmazonSQSClient(awsCredentials, Amazon.RegionEndpoint.USEast1))
            {
                var requestMsgBody = $"Downloading again files download failed";
                LogInfo(context, "Sending message for", $"{requestMsgBody} to DeviceUsage queue: {ExportDeviceUsageQueueURL}");

                var request = new SendMessageRequest
                {
                    DelaySeconds = secondDelays,
                    MessageAttributes = new Dictionary<string, MessageAttributeValue>
                    {
                        {"InitializeProcessing", new MessageAttributeValue {DataType = "String", StringValue = false.ToString()}},
                        {"ServiceProviderId", new MessageAttributeValue {DataType = "String", StringValue = serviceProviderId.ToString()}},
                        {"IsFromCloudwatchEvent", new MessageAttributeValue {DataType = "String", StringValue = fromCloudwatchEvent.ToString()}},
                        {"IsDownLoadFileAgain", new MessageAttributeValue {DataType = "String", StringValue = true.ToString()}},
                        {"ReportType", new MessageAttributeValue {DataType = "String", StringValue = reportType }},
                    },
                    MessageBody = requestMsgBody,
                    QueueUrl = ExportDeviceUsageQueueURL
                };

                if (!string.IsNullOrWhiteSpace(fileName))
                {
                    request.MessageAttributes.Add("DownloadFailedIds", new MessageAttributeValue { DataType = "String", StringValue = fileName });
                }

                LogInfo(context, "MessageBody", request.MessageBody);

                var response = await client.SendMessageAsync(request);
                LogInfo(context, "RESPONSE STATUS", $"{response.HttpStatusCode:d} {response.HttpStatusCode:g}");
            }
        }

        private async Task SendMessageToQueueDownloadAsync(KeySysLambdaContext context, int serviceProviderId, bool fromCloudwatchEvent, string reportType, UsageFile usageFile, int secondDelays)
        {
            LogInfo(context, "SUB", "SendMessageToQueueDownloadAgainAsync");
            LogInfo(context, "ServiceProviderId", serviceProviderId.ToString());
            LogInfo(context, "ReportType", reportType);

            if (string.IsNullOrEmpty(ExportDeviceUsageQueueURL))
            {
                return; // so we don't have to enqueue message in test
            }

            var awsCredentials = AwsCredentials(context);
            using (var client = new AmazonSQSClient(awsCredentials, Amazon.RegionEndpoint.USEast1))
            {
                var requestMsgBody = $"Downloading again files download failed";
                LogInfo(context, "Sending message for", $"{requestMsgBody} to DeviceUsage queue: {ExportDeviceUsageQueueURL}");

                var request = new SendMessageRequest
                {
                    DelaySeconds = secondDelays,
                    MessageAttributes = new Dictionary<string, MessageAttributeValue>
                    {
                        {"InitializeProcessing", new MessageAttributeValue {DataType = "String", StringValue = false.ToString()}},
                        {"ServiceProviderId", new MessageAttributeValue {DataType = "String", StringValue = serviceProviderId.ToString()}},
                        {"IsFromCloudwatchEvent", new MessageAttributeValue {DataType = "String", StringValue = fromCloudwatchEvent.ToString()}},
                        {"ReportType", new MessageAttributeValue {DataType = "String", StringValue = reportType }},
                        {"FileNamesNextDownload", new MessageAttributeValue {DataType = "String", StringValue = usageFile.FilePath }},
                        {"WriteTimesNextDownload", new MessageAttributeValue {DataType = "String", StringValue =  usageFile.WriteTime.ToString("yyyy-MM-dd HH:mm:ss")}},
                        {"IsDownloadNextInstance", new MessageAttributeValue {DataType = "String", StringValue = true.ToString() }},
                    },
                    MessageBody = requestMsgBody,
                    QueueUrl = ExportDeviceUsageQueueURL
                };

                LogInfo(context, "MessageBody", request.MessageBody);

                var response = await client.SendMessageAsync(request);
                LogInfo(context, "RESPONSE STATUS", $"{response.HttpStatusCode:d} {response.HttpStatusCode:g}");
            }
        }

        private async Task SendMessageToQueueNextDownloadAsync(KeySysLambdaContext context, int serviceProviderId, bool fromCloudwatchEvent, string reportType,
           string fileNamesNextDownloadString, string writeTimesNextDownloadString, string fileDownLoadFailedIds)
        {
            LogInfo(context, "SUB", "SendMessageToQueueDownloadAgainAsync");
            LogInfo(context, "ServiceProviderId", serviceProviderId.ToString());
            LogInfo(context, "ReportType", reportType);
            LogInfo(context, "FileNamesNextDownloadString", fileNamesNextDownloadString);
            LogInfo(context, "WriteTimesNextDownloadString", writeTimesNextDownloadString);
            LogInfo(context, "FileDownLoadFailedIds", fileDownLoadFailedIds);

            if (string.IsNullOrEmpty(ExportDeviceUsageQueueURL))
            {
                return; // so we don't have to enqueue message in test
            }

            var awsCredentials = AwsCredentials(context);
            using (var client = new AmazonSQSClient(awsCredentials, Amazon.RegionEndpoint.USEast1))
            {
                var requestMsgBody = $"Downloading again files download failed";
                LogInfo(context, "Sending message for", $"{requestMsgBody} to DeviceUsage queue: {ExportDeviceUsageQueueURL}");

                var request = new SendMessageRequest
                {
                    DelaySeconds = 0,
                    MessageAttributes = new Dictionary<string, MessageAttributeValue>
                    {
                        {"InitializeProcessing", new MessageAttributeValue {DataType = "String", StringValue = false.ToString()}},
                        {"ServiceProviderId", new MessageAttributeValue {DataType = "String", StringValue = serviceProviderId.ToString()}},
                        {"IsFromCloudwatchEvent", new MessageAttributeValue {DataType = "String", StringValue = fromCloudwatchEvent.ToString()}},
                        {"ReportType", new MessageAttributeValue {DataType = "String", StringValue = reportType }},
                        {"IsDownloadNextInstance", new MessageAttributeValue {DataType = "String", StringValue = true.ToString() }},
                    },
                    MessageBody = requestMsgBody,
                    QueueUrl = ExportDeviceUsageQueueURL
                };

                if (!string.IsNullOrWhiteSpace(fileDownLoadFailedIds))
                {
                    request.MessageAttributes.Add("DownLoadFailedIds", new MessageAttributeValue { DataType = "String", StringValue = fileDownLoadFailedIds });
                }
                if (!string.IsNullOrWhiteSpace(fileNamesNextDownloadString))
                {
                    request.MessageAttributes.Add("FileNamesNextDownload", new MessageAttributeValue { DataType = "String", StringValue = fileNamesNextDownloadString });
                }
                if (!string.IsNullOrWhiteSpace(writeTimesNextDownloadString))
                {
                    request.MessageAttributes.Add("WriteTimesNextDownload", new MessageAttributeValue { DataType = "String", StringValue = writeTimesNextDownloadString });
                }

                LogInfo(context, "MessageBody", request.MessageBody);

                var response = await client.SendMessageAsync(request);
                LogInfo(context, "RESPONSE STATUS", $"{response.HttpStatusCode:d} {response.HttpStatusCode:g}");
            }
        }


        private async Task SendNotificationMessageToQueueAsync(KeySysLambdaContext context, int serviceProviderId)
        {
            LogInfo(context, "SUB", $"SendNotificationMessageToQueueAsync({serviceProviderId})");
            LogInfo(context, "DeviceNotificationQueueURL", DeviceNotificationQueueURL);

            if (string.IsNullOrEmpty(DeviceNotificationQueueURL))
            {
                return; // so we don't have to enqueue message in test
            }

            var awsCredentials = AwsCredentials(context);
            using (var client = new AmazonSQSClient(awsCredentials, Amazon.RegionEndpoint.USEast1))
            {
                var request = new SendMessageRequest
                {
                    DelaySeconds = SQSMaxDelaySeconds,
                    MessageAttributes = new Dictionary<string, MessageAttributeValue>
                    {
                        {"RetryCount", new MessageAttributeValue {DataType = "String", StringValue = "0"}},
                        {"IntegrationType", new MessageAttributeValue {DataType = "String", StringValue = ((int)Amop.Core.Models.IntegrationType.Telegence).ToString()}},
                        {"ServiceProviderId", new MessageAttributeValue {DataType = "String", StringValue = serviceProviderId.ToString()}},
                        {"DelayBetweenRetries", new MessageAttributeValue {DataType = "Number", StringValue = SQSMaxDelaySeconds.ToString()}},
                        {"MaxRetries", new MessageAttributeValue {DataType = "Number", StringValue = DeviceCleanupMaxRetries}}
                    },
                    MessageBody = "Sending device sync cleanup/notification message",
                    QueueUrl = DeviceNotificationQueueURL
                };
                LogInfo(context, "MessageBody", request.MessageBody);

                var response = await client.SendMessageAsync(request);
                LogInfo(context, "RESPONSE STATUS", $"{response.HttpStatusCode:d} {response.HttpStatusCode:g}");
            }
        }

        private async Task SendEmailAsync(KeySysLambdaContext context, string subject, BodyBuilder bodyBuilder)
        {
            LogInfo(context, "SUB", "SendEmailAsync()");

            var emailFactory = new SimpleEmailServiceFactory();
            using var client = emailFactory.getClient(AwsSesCredentials(context), Amazon.RegionEndpoint.USEast1);
            var awsEnv = context.EnvironmentRepo.GetEnvironmentVariable(context.Context, "AWSEnv");
            var emailSender = new EmailSender(client, context.logger, awsEnv);
            var fromEmailAddress = context.GeneralProviderSettings.DeviceSyncFromEmailAddress;
            var recipientAddressList = context.GeneralProviderSettings.DeviceSyncToEmailAddresses.Split(';').ToList();
            try
            {
                await emailSender.SendEmailAsync(fromEmailAddress, recipientAddressList, subject, bodyBuilder);
            }
            catch (Exception ex)
            {
                LogInfo(context, "EXCEPTION", ex.Message + " " + ex.StackTrace);
            }
        }

        private BodyBuilder BuildDownloadMUBUFileEmptyNotificationBody(List<UsageFile> mubuFileList, string title)
        {
            StringBuilder sb = new StringBuilder();

            sb.AppendLine("<html>");
            sb.AppendLine(
                $"<div>{title}</div>");
            sb.AppendLine("<br/>");
            sb.AppendLine("<div>");
            sb.AppendLine(@"<table border=""0"" cellpadding=""0"" cellspacing=""0"" height=""100%"" width=""100%"">");
            sb.AppendLine(
                @$"<thead>
                    <tr>
                        <th align=""left"" valign=""top"">File name</th>
                        <th align=""left"" valign=""top"">Last uploaded</th>
                    </tr>
                </thead>");
            sb.AppendLine("<tbody>");
            foreach (var file in mubuFileList)
            {
                sb.AppendLine(
                    @$"<tr>
                        <td align=""left"" valign=""top"">{file.FileName}</td>
                        <td align=""left"" valign=""top"">{file.WriteTimeUtc} (UTC)</td>
                    </tr>");
            }

            sb.AppendLine("</tbody>");
            sb.AppendLine("</table>");
            sb.AppendLine("</div>");
            sb.AppendLine("<br/>");
            sb.AppendLine("</html>");

            var body = sb.ToString();

            return new BodyBuilder
            {
                HtmlBody = body,
                TextBody = body
            };
        }

        private BodyBuilder BuildStaleMubuVoiceSyncNotificationBody(string serviceProvider, UsageFile mostRecentFile)
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendLine("<html>");
            sb.AppendLine(
                $"<div>{serviceProvider} MUBU voice report has not been delivered to FTP since {mostRecentFile.WriteTime}. AMOP voice metrics may be stale until FTP delivery resumes.</div>");
            sb.AppendLine("<br/>");
            sb.AppendLine("<div>");
            sb.AppendLine("<b>Note:</b> Premiere Unbilled Usage report will be used in place of MUBU reports, if subscribed and delivering Premiere Unbilled Usage report to AMOP.");
            sb.AppendLine("</div>");
            sb.AppendLine("</html>");

            var body = sb.ToString();

            return new BodyBuilder
            {
                HtmlBody = body,
                TextBody = body
            };
        }

        private BodyBuilder BuildStaleMubuUsageSyncNotificationBody(string serviceProvider, UsageFile mostRecentFile)
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendLine("<html>");
            sb.AppendLine(
                $"<div>{serviceProvider} MUBU usage report has not been delivered to FTP since {mostRecentFile.WriteTime}. AMOP usage metrics may be stale until FTP delivery resumes.</div>");
            sb.AppendLine("<br/>");
            sb.AppendLine("<div>");
            sb.AppendLine("<b>Note:</b> Premiere Unbilled Usage report will be used in place of MUBU reports, if subscribed and delivering Premiere Unbilled Usage report to AMOP.");
            sb.AppendLine("</div>");
            sb.AppendLine("</html>");

            var body = sb.ToString();

            return new BodyBuilder
            {
                HtmlBody = body,
                TextBody = body
            };
        }

        public class UsageFile
        {
            public string FileName
            {
                get
                {
                    return FilePath.Split("/").Last();
                }
            }
            public DateTime WriteTime { get; set; }
            public DateTime? WriteTimeUtc { get; set; }
            public string FilePath { get; set; }
        }
    }
}
