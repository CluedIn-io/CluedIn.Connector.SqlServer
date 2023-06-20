﻿using CluedIn.Connector.Common.Clients;
using Microsoft.Data.SqlClient;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;

namespace CluedIn.Connector.SqlServer.Connector
{
    public class SqlClient : TransactionalClientBaseV2<SqlConnection, SqlTransaction, SqlParameter>, ISqlClient
    {
        private readonly int _defaultPort = 1433;

        public override string BuildConnectionString(IReadOnlyDictionary<string, object> config)
        {
            var connectionStringBuilder = new SqlConnectionStringBuilder
            {
                Authentication = SqlAuthenticationMethod.SqlPassword,
                Password = (string)config[SqlServerConstants.KeyName.Password],
                UserID = (string)config[SqlServerConstants.KeyName.Username],
                DataSource = (string)config[SqlServerConstants.KeyName.Host],
                InitialCatalog = (string)config[SqlServerConstants.KeyName.DatabaseName],
                Pooling = true,
                MultipleActiveResultSets = true,
                MaxPoolSize = 200
            };

            if (config.TryGetValue(SqlServerConstants.KeyName.PortNumber, out var portEntry) && int.TryParse(portEntry.ToString(), out var port))
                connectionStringBuilder.DataSource = $"{connectionStringBuilder.DataSource},{port}";
            else
                connectionStringBuilder.DataSource = $"{connectionStringBuilder.DataSource},{_defaultPort}";

            return connectionStringBuilder.ToString();
        }

        public async Task<SqlConnection> BeginConnection(IReadOnlyDictionary<string, object> config)
        {
            var connectionString = BuildConnectionString(config);
            var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();
            return connection;
        }

        public Task<DataTable> GetTableColumns(SqlConnection connection, string tableName, string schema)
        {
            if (string.IsNullOrWhiteSpace(tableName))
                tableName = (string)null;

            var restrictionValues = new string[4]
            {
                null,
                schema,
                tableName,
                null
            };

            return Task.FromResult(connection.GetSchema("Columns", restrictionValues));
        }

        public Task<DataTable> GetTables(SqlConnection connection, string tableName = null, string schema = null)
        {
            if (string.IsNullOrWhiteSpace(tableName))
                tableName = (string)null;

            var restrictionValues = new string[4]
            {
                null,
                schema,
                tableName,
                null
            };

            return Task.FromResult(connection.GetSchema("tables", restrictionValues));
        }
    }
}
