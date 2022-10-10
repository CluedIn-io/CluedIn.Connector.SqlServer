using CluedIn.Connector.Common.Clients;
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
                Password = (string)config[KeyName.Password],
                UserID = (string)config[KeyName.Username],
                DataSource = (string)config[KeyName.Host],
                InitialCatalog = (string)config[KeyName.DatabaseName],
                Pooling = true
            };

            if (config.TryGetValue(KeyName.PortNumber, out var portEntry) && int.TryParse(portEntry.ToString(), out var port))
                connectionStringBuilder.DataSource = $"{connectionStringBuilder.DataSource},{port}";
            else
                connectionStringBuilder.DataSource = $"{connectionStringBuilder.DataSource},{_defaultPort}";

            return connectionStringBuilder.ToString();
        }
    }
}
