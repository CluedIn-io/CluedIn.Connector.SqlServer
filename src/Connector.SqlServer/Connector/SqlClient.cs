using CluedIn.Connector.Common.Clients;
using CluedIn.Connector.Common.Configurations;
using Microsoft.Data.SqlClient;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace CluedIn.Connector.SqlServer.Connector
{
    public class SqlClient : ClientBase<SqlConnection, SqlParameter>, ISqlClient
    {
        private readonly int _defaultPort = 1433;

        public override async Task<SqlConnection> GetConnection(IDictionary<string, object> config)
        {
            while (true)
            {
                try
                {
                    return await base.GetConnection(config);
                }
                catch (System.InvalidOperationException ex)
                {
                    if (ex.ToString().Contains("The timeout period elapsed prior to obtaining a connection from the pool"))
                        await Task.Delay(1000);
                    else
                        throw;
                }
            }
        }

        public override string BuildConnectionString(IDictionary<string, object> config)
        {
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
