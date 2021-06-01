using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;
using CluedIn.Core.Connectors;
using Microsoft.Data.SqlClient;

namespace CluedIn.Connector.SqlServer.Connector
{
    public interface ISqlClient
    {
        Task ExecuteCommandAsync(IConnectorConnection config, string commandText, IEnumerable<SqlParameter> param = null);
        Task<SqlConnection> GetConnection(IDictionary<string, object> config);
        Task<DataTable> GetTables(IDictionary<string, object> config, string name = null);
        Task<DataTable> GetTableColumns(IDictionary<string, object> config, string tableName);
        Task ExecuteBulkAsync(IConnectorConnection config, DataTable table, string containerName);
    }
}
