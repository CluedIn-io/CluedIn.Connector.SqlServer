using CluedIn.Connector.Common.Clients;
using CluedIn.Connector.Common.Configurations;
using CluedIn.Core;
using Microsoft.Data.SqlClient;
using System.Collections.Generic;

namespace CluedIn.Connector.SqlServer.Connector
{
    public class SqlClient : ClientBase<SqlConnection, SqlParameter>, ISqlClient
    {
        private readonly int _defaultPort = 1433;

        public override string BuildConnectionString(IDictionary<string, object> config)
        {
            var connectionString = config.GetValue(SqlServerConstants.KeyName.ConnectionString)?.ToString();
            if (!string.IsNullOrWhiteSpace(connectionString))
                return connectionString;

            var connectionStringBuilder = new SqlConnectionStringBuilder
            {
                Authentication = SqlAuthenticationMethod.SqlPassword,
                Password = (string)config[SqlServerConstants.KeyName.Password],
                UserID = (string)config[SqlServerConstants.KeyName.Username],
                DataSource = (string)config[SqlServerConstants.KeyName.Host],
                InitialCatalog = (string)config[SqlServerConstants.KeyName.DatabaseName],
                Pooling = true
            };

            if (config.TryGetValue(SqlServerConstants.KeyName.PortNumber, out var portEntry) && int.TryParse(portEntry.ToString(), out var port))
                connectionStringBuilder.DataSource = $"{connectionStringBuilder.DataSource},{port}";
            else
                connectionStringBuilder.DataSource = $"{connectionStringBuilder.DataSource},{_defaultPort}";

            return connectionStringBuilder.ToString();
        }
    }
}
