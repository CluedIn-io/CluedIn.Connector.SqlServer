﻿using System.Data;
using System.Threading.Tasks;
using CluedIn.Connector.Common.Helpers;
using CluedIn.Core.Connectors;
using Microsoft.Data.SqlClient;

namespace CluedIn.Connector.SqlServer.Connector
{
    public class BulkSqlClient : SqlClient, IBulkSqlClient
    {
        public async Task ExecuteBulkAsync(IConnectorConnection config, DataTable table, string containerName)
        {
            await using var connection = await GetConnection(config.Authentication);
            using var bulk = new SqlBulkCopy(connection) { DestinationTableName = SqlStringSanitizer.Sanitize(containerName) };
            await bulk.WriteToServerAsync(table);
        }
    }
}
