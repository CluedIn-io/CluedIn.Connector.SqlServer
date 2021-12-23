﻿using CluedIn.Connector.Common.Clients;
using CluedIn.Connector.Common.Configurations;
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
                Password = (string)config[CommonConfigurationNames.Password],
                UserID = (string)config[CommonConfigurationNames.Username],
                DataSource = (string)config[CommonConfigurationNames.Host],
                InitialCatalog = (string)config[CommonConfigurationNames.DatabaseName],
                Pooling = true
            };

            if (config.TryGetValue(CommonConfigurationNames.PortNumber, out var portEntry) && portEntry is int port)
                connectionStringBuilder.DataSource = $"{connectionStringBuilder.DataSource},{port}";
            else
                connectionStringBuilder.DataSource = $"{connectionStringBuilder.DataSource},{_defaultPort}";

            return connectionStringBuilder.ToString();
        }
    }
}
