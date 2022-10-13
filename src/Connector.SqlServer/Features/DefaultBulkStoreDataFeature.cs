using CluedIn.Connector.Common.Helpers;
using CluedIn.Connector.SqlServer.Connector;
using CluedIn.Connector.SqlServer.Utility;
using CluedIn.Core;
using CluedIn.Core.Connectors;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Threading.Tasks;

namespace CluedIn.Connector.SqlServer.Features
{
    public class DefaultBulkStoreDataFeature : IBulkStoreDataFeature
    {
        public virtual async Task BulkTableUpdate(
            ExecutionContext executionContext,
            Guid providerDefinitionId,
            SanitizedSqlString tableName,
            IDictionary<string, object> data,
            int threshold,
            IBulkSqlClient client,
            Func<Task<IConnectorConnection>> connectionFactory,
            ILogger logger)
        {
            var table = GetDataTable(executionContext, tableName, data);
            CacheDataTableRow(data, table);

            if (table.Rows.Count >= threshold)
                await FlushTable(executionContext, connectionFactory, client, tableName, table, logger);
        }

        private static void CacheDataTableRow(IDictionary<string, object> data, DataTable table)
        {
            var row = table.NewRow();
            foreach (var item in data)
                row[item.Key] = item.Value;

            table.Rows.Add(row);
        }

        private async Task FlushTable(
            ExecutionContext executionContext,
            Func<Task<IConnectorConnection>> connectionFactory,
            IBulkSqlClient client,
            SanitizedSqlString tableName,
            DataTable table,
            ILogger logger)
        {
            var dataTableCacheName = GetDataTableCacheName(tableName);
            executionContext.ApplicationContext.System.Cache.RemoveItem(dataTableCacheName);

            var connection = await connectionFactory();

            var sw = new Stopwatch();
            sw.Start();
            await client.ExecuteBulkAsync(connection, table, tableName);
            logger.LogDebug($"Stream StoreData BulkInsert {table.Rows.Count} rows - {sw.ElapsedMilliseconds}ms");
        }

        private DataTable GetDataTable(ExecutionContext executionContext, SanitizedSqlString tableName,
            IDictionary<string, object> data)
        {
            var dataTableCacheName = GetDataTableCacheName(tableName);

            return executionContext.ApplicationContext.System.Cache.GetItem(dataTableCacheName, () =>
            {
                var table = new DataTable(tableName.GetValue());
                foreach (var col in data)
                    table.Columns.Add(new SanitizedSqlString(col.Key).GetValue(), typeof(string));

                return table;
            });
        }

        private static string GetDataTableCacheName(SanitizedSqlString tableName)
        {
            return $"Stream_cache_{tableName}";
        }
    }
}
