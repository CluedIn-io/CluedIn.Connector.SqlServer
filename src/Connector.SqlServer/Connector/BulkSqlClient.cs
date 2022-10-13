using CluedIn.Connector.SqlServer.Utility;
using CluedIn.Core.Connectors;
using Microsoft.Data.SqlClient;
using System.Data;
using System.Threading.Tasks;

namespace CluedIn.Connector.SqlServer.Connector
{
    public class BulkSqlClient : SqlClient, IBulkSqlClient
    {
        public async Task ExecuteBulkAsync(IConnectorConnection config, DataTable table, SanitizedSqlName tableName)
        {
            await using var connection = await GetConnection(config.Authentication);
            using var bulk = new SqlBulkCopy(connection) { DestinationTableName = $"[{config.GetSchema()}].[{tableName}]" };
            await bulk.WriteToServerAsync(table);
        }
    }
}
