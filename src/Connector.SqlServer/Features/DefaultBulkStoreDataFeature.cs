using CluedIn.Connector.Common.Helpers;
using CluedIn.Connector.SqlServer.Connector;
using CluedIn.Connector.SqlServer.Utils;
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
            SqlTableName tableName,
            IDictionary<string, object> data,
            int threshold,
            IBulkSqlClient client,
            IConnectorConnection config,
            ILogger logger)
        {
            var table = GetDataTable(executionContext, tableName, data);
            CacheDataTableRow(data, table);

            if (table.Rows.Count >= threshold)
                await FlushTable(executionContext, config, client, tableName, table, logger);
        }

        private static void CacheDataTableRow(IDictionary<string, object> data, DataTable table)
        {
            var row = table.NewRow();
            foreach (var item in data)
                row[item.Key.ToSanitizedSqlName()] = item.Value;

            table.Rows.Add(row);
        }

        private async Task FlushTable(
            ExecutionContext executionContext,
            IConnectorConnection config,
            IBulkSqlClient client,
            SqlTableName tableName,
            DataTable table,
            ILogger logger)
        {
            var dataTableCacheName = GetDataTableCacheName(tableName);
            executionContext.ApplicationContext.System.Cache.RemoveItem(dataTableCacheName);

            var sw = new Stopwatch();
            sw.Start();
            await client.ExecuteBulkAsync(config, table, tableName);
            logger.LogDebug($"Stream StoreData BulkInsert {table.Rows.Count} rows - {sw.ElapsedMilliseconds}ms");
        }

        private DataTable GetDataTable(ExecutionContext executionContext, SqlTableName tableName, IDictionary<string, object> data)
        {
            var dataTableCacheName = GetDataTableCacheName(tableName);

            return executionContext.ApplicationContext.System.Cache.GetItem(dataTableCacheName, () =>
            {
                var table = new DataTable(tableName.FullyQualifiedName);
                foreach (var col in data)
                {
                    table.Columns.Add(col.Key.ToSanitizedSqlName(), typeof(string));
                }

                return table;
            });
        }

        private static string GetDataTableCacheName(SqlTableName tableName)
        {
            return $"Stream_cache_{tableName.Schema}_{tableName.LocalName}";
        }
    }
}
