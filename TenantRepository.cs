using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using Amop.Core.Constants;
using Amop.Core.Enumerations;
using Amop.Core.Helpers;
using Amop.Core.Helpers.Pond;
using Amop.Core.Logger;
using Amop.Core.Resilience;
using Microsoft.Data.SqlClient;
using Polly;

namespace Amop.Core.Repositories.Tenant
{
    public class TenantRepository : ITenantRepository
    {
        private const int MaxRetries = CommonConstants.DEFAULT_SQL_RETRY_COUNT;
        private readonly string connectionString;
        private readonly ISyncPolicy sqlRetryPolicy;

        public TenantRepository(string connectionString)
            : this(connectionString, new NoOpLogger())
        {
        }

        public TenantRepository(string connectionString, IKeysysLogger logger)
            : this(connectionString, new PolicyFactory(logger))
        {
        }

        public TenantRepository(string connectionString, IPolicyFactory policyFactory)
            : this(connectionString, policyFactory.GetSqlRetryPolicy(MaxRetries))
        {
        }

        public TenantRepository(string connectionString, ISyncPolicy sqlRetryPolicy)
        {
            this.connectionString = connectionString;
            this.sqlRetryPolicy = sqlRetryPolicy;
        }

        public virtual string GetPortalImageByTenantId(Action<string, string> logFunction, int tenantId)
        {
            return GetCustomObjectById(logFunction, tenantId, (int)TenantObject.CustomPortalImage);
        }

        public virtual string GetTenantNameByTenantId(int tenantId)
        {

            if (tenantId == 0) return null;

            return sqlRetryPolicy.Execute(() =>
            {
                using (var connection = new SqlConnection(connectionString))
                {
                    connection.Open();
                    using (var command = new SqlCommand("SELECT * FROM dbo.Tenant WHERE id = @id", connection))
                    {
                        command.CommandType = CommandType.Text;
                        command.Parameters.AddWithValue("@id", tenantId);

                        using (var rdr = command.ExecuteReader())
                        {
                            if (rdr.Read())
                            {
                                return rdr["Name"].ToString();
                            }
                        }
                    }
                }

                return null;
            });
        }

        public virtual int GetTenantIdByServiceProviderId(int serviceProviderId)
        {
            return sqlRetryPolicy.Execute(() =>
            {
                using (var connection = new SqlConnection(connectionString))
                {
                    connection.Open();
                    using (var command = new SqlCommand("SELECT * FROM dbo.ServiceProvider WHERE id = @id", connection))
                    {
                        command.CommandType = CommandType.Text;
                        command.Parameters.AddWithValue("@id", serviceProviderId);

                        using (var rdr = command.ExecuteReader())
                        {
                            if (rdr.Read())
                            {
                                return Convert.ToInt32(rdr["TenantId"]);
                            }
                        }
                    }
                }

                return 0;
            });
        }

        public virtual string GetCustomObjectById(Action<string, string> logFunction, int tenantId, int objectId)
        {
            var parameters = new List<SqlParameter>()
            {
                new SqlParameter(CommonSQLParameterNames.TENANT_ID, tenantId),
                new SqlParameter(CommonSQLParameterNames.OBJECT_ID, objectId),
            };
            return sqlRetryPolicy.Execute(() =>
                SqlQueryHelper.ExecuteStoredProcedureWithListResult(logFunction, connectionString,
                    SQLConstant.StoredProcedureName.GET_CUSTOM_OBJECT_BY_ID,
                    (dataReader) => ReadCustomObject(dataReader),
                    parameters,
                    SQLConstant.ShortTimeoutSeconds)).FirstOrDefault();
        }

        private string ReadCustomObject(SqlDataReader dataReader)
        {
            var columns = dataReader.GetColumnsFromReader();
            return dataReader.StringFromReader(columns, CommonColumnNames.Value);
        }
    }
}
