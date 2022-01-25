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
            var connectionStringBuilder = new SqlConnectionStringBuilder
            {
                Authentication = SqlAuthenticationMethod.SqlPassword,
                Password = (string)config[CommonConfigurationNames.Password.ToCamelCase()],
                UserID = (string)config[CommonConfigurationNames.Username.ToCamelCase()],
                DataSource = (string)config[CommonConfigurationNames.Host.ToCamelCase()],
                InitialCatalog = (string)config[CommonConfigurationNames.DatabaseName.ToCamelCase()],
                Pooling = true
            };

            if (config.TryGetValue(CommonConfigurationNames.PortNumber.ToCamelCase(), out var portEntry) && int.TryParse(portEntry.ToString(), out var port))
                connectionStringBuilder.DataSource = $"{connectionStringBuilder.DataSource},{port}";
            else
                connectionStringBuilder.DataSource = $"{connectionStringBuilder.DataSource},{_defaultPort}";

            return connectionStringBuilder.ToString();
        }
    }
}
