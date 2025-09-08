using System;
using System.Collections.Generic;
using System.Data;
using Amazon.Runtime;
using Amop.Core.Constants;
using Amop.Core.Helpers;
using Amop.Core.Helpers.Pond;
using Amop.Core.Logger;
using Amop.Core.Models.eBonding;
using Amop.Core.Resilience;
using Amop.Core.Services.Base64Service;
using Microsoft.Data.SqlClient;
using Newtonsoft.Json;
using Polly;

namespace Amop.Core.Models.Settings
{
    public class SettingsRepository : ISettingsRepository
    {
        private readonly IKeysysLogger _logger;
        private readonly string _connectionString;
        private readonly IBase64Service _base64Service;

        public SettingsRepository(IKeysysLogger logger, string connectionString,
            IBase64Service base64Service)
        {
            _logger = logger;
            _connectionString = connectionString;
            _base64Service = base64Service;
        }

        public GeneralProviderSettings GetGeneralProviderSettings()
        {
            _logger.LogInfo("SUB", "GetGeneralProviderSettings");

            var settings = new GeneralProviderSettings();

            using (var Conn = new SqlConnection(_connectionString))
            {
                using (var Cmd = new SqlCommand("SELECT SettingKey, SettingValue FROM ServiceProviderSetting WHERE IsDeleted = 0 AND ServiceProviderId IS NULL", Conn))
                {
                    Cmd.CommandType = System.Data.CommandType.Text;
                    Conn.Open();

                    SqlDataReader rdr = Cmd.ExecuteReader();
                    while (rdr.Read())
                    {
                        var setting = SettingFromReader(rdr);
                        if (setting.SettingKey == SettingsKeys.SETTINGKEY_DEVICESYNC_TOEMAILADDRESSES)
                        {
                            settings.DeviceSyncToEmailAddresses = setting.SettingValue;
                        }
                        else if (setting.SettingKey == SettingsKeys.SETTINGKEY_DEVICESYNC_FROMEMAILADDRESS)
                        {
                            settings.DeviceSyncFromEmailAddress = setting.SettingValue;
                        }
                        else if (setting.SettingKey == SettingsKeys.SETTINGKEY_DEVICESYNC_RESULTSEMAILSUBJECT)
                        {
                            settings.DeviceSyncResultsEmailSubject = setting.SettingValue;
                        }
                        else if (setting.SettingKey == SettingsKeys.SETTINGKEY_DEVICESYNC_ERROREMAILSUBJECT)
                        {
                            settings.DeviceSyncErrorEmailSubject = setting.SettingValue;
                        }
                        else if (setting.SettingKey == SettingsKeys.SETTINGKEY_CUSTOMERCHARGE_TOEMAILADDRESSES)
                        {
                            settings.CustomerChargeToEmailAddresses = setting.SettingValue;
                        }
                        else if (setting.SettingKey == SettingsKeys.SETTINGKEY_CUSTOMERCHARGE_FROMEMAILADDRESS)
                        {
                            settings.CustomerChargeFromEmailAddress = setting.SettingValue;
                        }
                        else if (setting.SettingKey == SettingsKeys.SETTINGKEY_CUSTOMERCHARGE_RESULTSEMAILSUBJECT)
                        {
                            settings.CustomerChargeResultsEmailSubject = setting.SettingValue;
                        }
                        else if (setting.SettingKey == SettingsKeys.SETTINGKEY_AWS_ACCESS_KEY_ID)
                        {
                            settings.AWSAccesKeyID = setting.SettingValue;
                        }
                        else if (setting.SettingKey == SettingsKeys.SETTINGKEY_AWS_SECRET_ACCESS_KEY)
                        {
                            settings.AWSSecretAccessKey = setting.SettingValue;
                        }
                        else if (setting.SettingKey == SettingsKeys.SETTINGKEY_AWS_ACCESS_KEY_ID_SES)
                        {
                            settings.AWSAccesKeyID_SES = setting.SettingValue;
                        }
                        else if (setting.SettingKey == SettingsKeys.SETTINGKEY_AWS_SECRET_ACCESS_KEY_SES)
                        {
                            settings.AWSSecretAccessKey_SES = setting.SettingValue;
                        }
                        else if (setting.SettingKey == SettingsKeys.SETTINGKEY_JASPER_DB_CONNECTION_STRING)
                        {
                            settings.JasperDbConnectionString = setting.SettingValue;
                        }
                    }

                    settings.AwsCredentials = new BasicAWSCredentials(settings.AWSAccesKeyID, _base64Service.Base64Decode(settings.AWSSecretAccessKey));
                    settings.AwsSesCredentials = new BasicAWSCredentials(settings.AWSAccesKeyID_SES, _base64Service.Base64Decode(settings.AWSSecretAccessKey_SES));

                    Conn.Close();
                }
            }
            _logger.LogInfo("INFO", "Done getting GetGeneralProviderSettings");
            return settings;
        }

        public OptimizationSettings GetOptimizationSettings(int? tenantId = null)
        {
            _logger.LogInfo(CommonConstants.SUB, $"{tenantId}");
            var policyFactory = new PolicyFactory(_logger);
            var sqlRetryPolicy = policyFactory.GetSqlRetryPolicy(PondHelper.CommonConfig.RETRY_NUMBER);

            var settingsValues = sqlRetryPolicy.Execute(() =>
            {
                var parameters = new List<SqlParameter>()
                {
                    new SqlParameter(CommonSQLParameterNames.TENANT_ID, tenantId ?? (object)DBNull.Value),
                };

                return SqlQueryHelper.ExecuteStoredProcedureWithListResult(ParameterizedLog(),
                                _connectionString,
                                SQLConstant.StoredProcedureName.OPTIMIZATION_GET_OPTIMIZATION_SETTINGS_BY_TENANT_ID,
                                (dataReader) => OptimizationSettingFromReader(dataReader),
                                parameters,
                                SQLConstant.ShortTimeoutSeconds);
            });

            var settings = MapToOptimizationSettingsModel(settingsValues);

            try
            {
                // try linux first
                settings.BillingTimeZone = TimeZoneInfo.FindSystemTimeZoneById(settings.LinuxTimeZoneName);
            }
            catch (TimeZoneNotFoundException)
            {
                // didn't find it, so try the windows one
                settings.BillingTimeZone = TimeZoneInfo.FindSystemTimeZoneById(settings.WindowsTimeZoneName);
            }

            return settings;
        }

        public JasperProviderSettings GetJasperDeviceSettings(int serviceProviderId)
        {
            _logger.LogInfo("SUB", "LoadJasperDeviceSettings");
            var settings = new JasperProviderSettings();
            using (var conn = new SqlConnection(_connectionString))
            {
                using (var cmd =
                    new SqlCommand("SELECT SettingKey, SettingValue FROM ServiceProviderSetting WHERE IsDeleted = 0 AND ServiceProviderId = @ServiceProviderId",
                        conn))
                {
                    cmd.CommandType = CommandType.Text;
                    cmd.Parameters.AddWithValue("@ServiceProviderId", serviceProviderId);
                    conn.Open();

                    var rdr = cmd.ExecuteReader();
                    while (rdr.Read())
                    {
                        var setting = SettingFromReader(rdr);

                        if (setting.SettingKey == SettingsKeys.SETTINGKEY_JASPER_UI_USERNAME)
                        {
                            settings.JasperUIUsername = setting.SettingValue;
                        }
                        else if (setting.SettingKey == SettingsKeys.SETTINGKEY_JASPER_UI_PASSWORD)
                        {
                            settings.JasperUIPassword = setting.SettingValue;
                        }
                        else if (setting.SettingKey == SettingsKeys.SETTINGKEY_JASPER_UI_ADDRESS)
                        {
                            settings.JasperUIAddress = setting.SettingValue;
                        }
                        else if (setting.SettingKey == SettingsKeys.SETTINGKEY_JASPER_EXPORT_MAILBOX_SERVER)
                        {
                            settings.JasperExportMailServer = setting.SettingValue;
                        }
                        else if (setting.SettingKey == SettingsKeys.SETTINGKEY_JASPER_EXPORT_MAILBOX_PORT)
                        {
                            settings.JasperExportMailPort = setting.ValueAsInt();
                        }
                        else if (setting.SettingKey == SettingsKeys.SETTINGKEY_JASPER_EXPORT_MAILBOX_USERNAME)
                        {
                            settings.JasperExportMailUsername = setting.SettingValue;
                        }
                        else if (setting.SettingKey == SettingsKeys.SETTINGKEY_JASPER_EXPORT_MAILBOX_PASSWORD)
                        {
                            settings.JasperExportMailPassword = setting.SettingValue;
                        }
                        else if (setting.SettingKey == SettingsKeys.SETTINGKEY_JASPER_EXPORT_MAILBOX_ALIAS)
                        {
                            settings.JasperExportMailAlias = setting.SettingValue;
                        }
                        else if (setting.SettingKey == SettingsKeys.SETTINGKEY_JASPER_FTP_SERVER)
                        {
                            settings.JasperFtpServer = setting.SettingValue;
                        }
                        else if (setting.SettingKey == SettingsKeys.SETTINGKEY_JASPER_FTP_PATH)
                        {
                            settings.JasperFtpPath = setting.SettingValue;
                        }
                        else if (setting.SettingKey == SettingsKeys.SETTINGKEY_JASPER_FTP_USERNAME)
                        {
                            settings.JasperFtpUsername = setting.SettingValue;
                        }
                        else if (setting.SettingKey == SettingsKeys.SETTINGKEY_JASPER_FTP_PASSWORD)
                        {
                            settings.JasperFtpPassword = setting.SettingValue;
                        }
                        else if (setting.SettingKey == SettingsKeys.SETTINGKEY_JASPER_TRANSACTION_REPORT_PATH)
                        {
                            settings.JasperFtpTransactionReportPath = setting.SettingValue;
                        }
                        else if (setting.SettingKey == SettingsKeys.SETTINGKEY_JASPER_WRITE_BILLING_ACCOUNT_INFO)
                        {
                            settings.JasperWriteBillingAccountInfo = setting.SettingValue == "1";
                        }
                        else if (setting.SettingKey == SettingsKeys.SETTINGKEY_JASPER_USES_PRORATION)
                        {
                            settings.UsesProration = setting.SettingValue == "1";
                        }
                    }

                    conn.Close();
                }
            }

            return settings;
        }

        public TelegenceProviderSettings GetTelegenceDeviceSettings(int serviceProviderId)
        {
            _logger.LogInfo("SUB", "GetTelegenceDeviceSettings");
            _logger.LogInfo("SUB", _connectionString);
            var settings = new TelegenceProviderSettings();
            using (var Conn = new SqlConnection(_connectionString))
            {
                using (var Cmd =
                    new SqlCommand("SELECT SettingKey, SettingValue FROM ServiceProviderSetting WHERE IsDeleted = 0 AND ServiceProviderId = @ServiceProviderId",
                        Conn))
                {
                    Cmd.CommandType = CommandType.Text;
                    Cmd.Parameters.AddWithValue("@ServiceProviderId", serviceProviderId);
                    Conn.Open();

                    var rdr = Cmd.ExecuteReader();
                    while (rdr.Read())
                    {
                        var setting = SettingFromReader(rdr);

                        if (setting.SettingKey == SettingsKeys.SETTINGKEY_TELEGENCE_FTP_SERVER)
                        {
                            settings.TelegenceFtpServer = setting.SettingValue;
                        }
                        else if (setting.SettingKey == SettingsKeys.SETTINGKEY_TELEGENCE_FTP_PATH)
                        {
                            settings.TelegenceFtpPath = setting.SettingValue;
                        }
                        else if (setting.SettingKey == SettingsKeys.SETTINGKEY_TELEGENCE_FTP_MUBU_PATH)
                        {
                            settings.TelegenceFtpMubuPath = setting.SettingValue;
                        }
                        else if (setting.SettingKey == SettingsKeys.SETTINGKEY_TELEGENCE_FTP_FINAL_USAGE_PATH)
                        {
                            settings.TelegenceFtpFinalUsagePath = setting.SettingValue;
                        }
                        else if (setting.SettingKey == SettingsKeys.SETTINGKEY_TELEGENCE_FTP_USERNAME)
                        {
                            settings.TelegenceFtpUsername = setting.SettingValue;
                        }
                        else if (setting.SettingKey == SettingsKeys.SETTINGKEY_TELEGENCE_FTP_PASSWORD)
                        {
                            settings.TelegenceFtpPassword = setting.SettingValue;
                        }
                        else if (setting.SettingKey == SettingsKeys.SETTINGKEY_TELEGENCE_KAFKA_BOOTSTRAP_SERVER)
                        {
                            settings.KafkaBootstrapServer = setting.SettingValue;
                        }
                        else if (setting.SettingKey == SettingsKeys.SETTINGKEY_TELEGENCE_KAFKA_OAUTH_BEARER_TOKEN_ENDPOINT)
                        {
                            settings.KafkaOauthBearerTokenEndpoint = setting.SettingValue;
                        }
                        else if (setting.SettingKey == SettingsKeys.SETTINGKEY_TELEGENCE_KAFKA_OAUTH_BEARER_CLIENT_ID)
                        {
                            settings.KafkaOauthBearerClientId = setting.SettingValue;
                        }
                        else if (setting.SettingKey == SettingsKeys.SETTINGKEY_TELEGENCE_KAFKA_OAUTH_BEARER_CLIENT_SECRET)
                        {
                            settings.KafkaOauthBearerClientSecret = setting.SettingValue;
                        }
                        else if (setting.SettingKey == SettingsKeys.SETTINGKEY_TELEGENCE_KAFKA_CONSUMER_GROUP_ID)
                        {
                            settings.KafkaConsumerGroupId = setting.SettingValue;
                        }
                        else if (setting.SettingKey == SettingsKeys.SETTINGKEY_TELEGENCE_KAFKA_TOPIC_NAME)
                        {
                            settings.KafkaTopicName = setting.SettingValue;
                        }
                    }


                    Conn.Close();
                }
            }

            return settings;
        }

        public eBondingProviderSettings GetEbondingDeviceSettings(int serviceProviderId)
        {
            _logger.LogInfo("SUB", "GetEbondingDeviceSettings");
            var settings = new eBondingProviderSettings();
            using (var conn = new SqlConnection(_connectionString))
            {
                using (var cmd =
                    new SqlCommand("SELECT SettingKey, SettingValue FROM ServiceProviderSetting WHERE IsDeleted = 0 AND ServiceProviderId = @ServiceProviderId",
                        conn))
                {
                    cmd.CommandType = CommandType.Text;
                    cmd.Parameters.AddWithValue("@ServiceProviderId", serviceProviderId);
                    conn.Open();

                    var rdr = cmd.ExecuteReader();
                    while (rdr.Read())
                    {
                        var setting = SettingFromReader(rdr);

                        if (setting.SettingKey == SettingsKeys.SETTINGKEY_EBONDING_FTP_SERVER)
                        {
                            settings.FtpServer = setting.SettingValue;
                        }
                        else if (setting.SettingKey == SettingsKeys.SETTINGKEY_EBONDING_FTP_PATH)
                        {
                            settings.FtpPath = setting.SettingValue;
                        }
                        else if (setting.SettingKey == SettingsKeys.SETTINGKEY_EBONDING_FTP_USERNAME)
                        {
                            settings.FtpUsername = setting.SettingValue;
                        }
                        else if (setting.SettingKey == SettingsKeys.SETTINGKEY_EBONDING_FTP_PASSWORD)
                        {
                            settings.FtpPassword = setting.SettingValue;
                        }
                        else if (setting.SettingKey == SettingsKeys.SETTINGKEY_EBONDING_ADDITIONAL_API_CREDENTIALS)
                        {
                            try
                            {
                                settings.AdditionalApiCredentials = JsonConvert.DeserializeObject<eBondingApiCredentials[]>(setting.SettingValue);
                            }
                            catch (Exception ex)
                            {
                                _logger.LogInfo("EXCEPTION", $"Error deserializing additional eBonding API credentials: {ex.Message}");
                            }
                        }
                    }

                    conn.Close();
                }
            }

            return settings;
        }

        public PondProviderSettings GetPondDeviceSettings(int serviceProviderId, Action<string, string> logFunction)
        {
            _logger.LogInfo(CommonConstants.SUB, $"({serviceProviderId})");
            var settings = new PondProviderSettings();
            var policyFactory = new PolicyFactory(_logger);
            var sqlRetryPolicy = policyFactory.GetSqlRetryPolicy(PondHelper.CommonConfig.RETRY_NUMBER);

            var parameters = new List<SqlParameter>()
            {
                new SqlParameter(CommonSQLParameterNames.SERVICE_PROVIDER_ID_PASCAL_CASE, serviceProviderId),
            };

            var settingsFromDB = sqlRetryPolicy.Execute(() => SqlQueryHelper.ExecuteStoredProcedureWithListResult<OptimizationSetting>(logFunction,
                _connectionString,
                SQLConstant.StoredProcedureName.GET_DEVICE_SETTINGS_BY_SERVICE_PROVIDER,
                (dataReader) => ReadGetPondDeviceSettings(dataReader),
                parameters,
                SQLConstant.ShortTimeoutSeconds));

            foreach (var setting in settingsFromDB)
            {
                if (setting.SettingKey == SettingsKeys.SETTINGKEY_POND_SFTP_SERVER)
                {
                    settings.PondSFTPServer = setting.SettingValue;
                }
                else if (setting.SettingKey == SettingsKeys.SETTINGKEY_POND_SFTP_PATH)
                {
                    settings.PondSFTPPath = setting.SettingValue;
                }
                else if (setting.SettingKey == SettingsKeys.SETTINGKEY_POND_SFTP_USERNAME)
                {
                    settings.PondSFTPUsername = setting.SettingValue;
                }
                else if (setting.SettingKey == SettingsKeys.SETTINGKEY_POND_SFTP_PASSWORD)
                {
                    settings.PondSFTPPassword = setting.SettingValue;
                }
            }

            return settings;
        }

        private static Setting SettingFromReader(IDataRecord rdr)
        {
            return new OptimizationSetting
            {
                SettingKey = rdr["SettingKey"].ToString(),
                SettingValue = rdr["SettingValue"].ToString()
            };
        }

        private static Setting OptimizationSettingFromReader(SqlDataReader reader)
        {
            var columns = reader.GetColumnsFromReader();
            var optimizationSetting = new OptimizationSetting
            {
                SettingKey = reader.StringFromReader(columns, CommonColumnNames.SettingKey),
                SettingValue = reader.StringFromReader(columns, CommonColumnNames.SettingValue),
                CanOverride = reader.BooleanFromReader(columns, CommonColumnNames.CanOverride),
                OverrideValue = reader.StringFromReader(columns, CommonColumnNames.OverrideValue)
            };


            return new Setting()
            {
                SettingKey = optimizationSetting.SettingKey,
                SettingValue = (optimizationSetting.CanOverride && !string.IsNullOrWhiteSpace(optimizationSetting.OverrideValue)) ?
                    optimizationSetting.OverrideValue : optimizationSetting.SettingValue
            };
        }
        private Action<string, string> ParameterizedLog()
        {
            return (type, message) => _logger.LogInfo(type, message);
        }

        private OptimizationSetting ReadGetPondDeviceSettings(SqlDataReader dataReader)
        {
            var columns = dataReader.GetColumnsFromReader();
            var record = new OptimizationSetting()
            {
                SettingKey = dataReader.StringFromReader(columns, CommonColumnNames.SettingKey),
                SettingValue = dataReader.StringFromReader(columns, CommonColumnNames.SettingValue)
            };

            return record;
        }

        private static OptimizationSettings MapToOptimizationSettingsModel(List<Setting> settingsValues)
        {
            var settings = new OptimizationSettings();

            foreach (var setting in settingsValues)
            {
                switch (setting.SettingKey)
                {
                    case SettingsKeys.SETTINGKEY_LINUX_TIME_ZONE:
                        settings.LinuxTimeZoneName = setting.SettingValue;
                        break;
                    case SettingsKeys.SETTINGKEY_WINDOWS_TIME_ZONE:
                        settings.WindowsTimeZoneName = setting.SettingValue;
                        break;
                    case SettingsKeys.SETTINGKEY_EMAIL_SUBJECT:
                        settings.ResultsEmailSubject = setting.SettingValue;
                        break;
                    case SettingsKeys.SETTINGKEY_TO_EMAIL_ADDRESSES:
                        settings.ToEmailAddresses = setting.SettingValue;
                        break;
                    case SettingsKeys.SETTINGKEY_FROM_EMAIL_ADDRESS:
                        settings.FromEmailAddress = setting.SettingValue;
                        break;
                    case SettingsKeys.SETTINGKEY_BCC_EMAIL_ADDRESSES:
                        settings.BccEmailAddresses = setting.SettingValue;
                        break;
                    case SettingsKeys.SETTINGKEY_OPTIMIZATION_OU:
                        settings.ExecutionOU = setting.SettingValue;
                        break;
                    case SettingsKeys.SETTINGKEY_CUSTOMER_EMAIL_SUBJECT:
                        settings.ResultsCustomerEmailSubject = setting.SettingValue;
                        break;
                    case SettingsKeys.OPTIMIZATION_SYNC_DEVICE_ERROR_EMAIL_SUBJECT:
                        settings.OptimizationSyncDeviceErrorEmailSubject = setting.SettingValue;
                        break;
                    case SettingsKeys.SETTINGKEY_CUSTOMER_TO_EMAIL_ADDRESSES:
                        settings.CustomerToEmailAddresses = setting.SettingValue;
                        break;
                    case SettingsKeys.SETTINGKEY_CUSTOMER_FROM_EMAIL_ADDRESS:
                        settings.CustomerFromEmailAddress = setting.SettingValue;
                        break;
                    case SettingsKeys.SETTINGKEY_AUTO_UPDATE_RATE_PLANS:
                        string autoUpdateRatePlans = setting.SettingValue;
                        if (autoUpdateRatePlans == "1" || autoUpdateRatePlans == "true")
                        {
                            settings.CanAutoUpdateRatePlans = true;
                        }
                        else
                        {
                            settings.CanAutoUpdateRatePlans = false;
                        }

                        break;
                    case SettingsKeys.SETTINGKEY_OPT_INTO_LAST_DAY_OPTIMIZATION:
                        string optIntoLastDay = setting.SettingValue;
                        if (optIntoLastDay == "1" || optIntoLastDay == "true")
                        {
                            settings.OptIntoContinuousLastDayOptimization = true;
                        }
                        else
                        {
                            settings.OptIntoContinuousLastDayOptimization = false;
                        }

                        break;
                    case SettingsKeys.SETTINGKEY_GO_RATE_PLAN_UPDATE_EMAIL_SUBJECT:
                        settings.GoForRatePlanUpdateEmailSubject = setting.SettingValue;
                        break;
                    case SettingsKeys.SETTINGKEY_NOGO_RATE_PLAN_UPDATE_EMAIL_SUBJECT:
                        settings.NoGoForRatePlanUpdateEmailSubject = setting.SettingValue;
                        break;
                    case SettingsKeys.SETTINGKEY_USING_NEW_PROCESS_IN_CUSTOMER_CHARGE:
                        string useNewLogic = setting.SettingValue;
                        if (useNewLogic == "1" || useNewLogic == "true")
                        {
                            settings.UsingNewProcessInCustomerCharge = true;
                        }
                        else
                        {
                            settings.UsingNewProcessInCustomerCharge = false;
                        }
                        break;
                    case SettingsKeys.SETTINGKEY_OPT_INTO_CROSS_PROVIDER_CUSTOMER_OPTIMIZATION:
                        string optIntoCrossProviderCustomerOptimization = setting.SettingValue;
                        if (FormatHelper.ToBoolean(optIntoCrossProviderCustomerOptimization))
                        {
                            settings.OptIntoCrossProviderCustomerOptimization = true;
                        }
                        else
                        {
                            settings.OptIntoCrossProviderCustomerOptimization = false;
                        }
                        break;
                    case SettingsKeys.SETTINGKEY_PERMUTATION_LIMIT:
                        if (int.TryParse(setting.SettingValue, out int permutationLimit))
                        {
                            settings.PermutationLimit = permutationLimit;
                        }
                        break;
                }
            }

            return settings;
        }
    }
}
