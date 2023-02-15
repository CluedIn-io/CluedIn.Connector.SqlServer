using CluedIn.Connector.SqlServer.Utils;
using System.Data;
using System.Threading.Tasks;
using CluedIn.Core.Connectors;

namespace CluedIn.Connector.SqlServer.Connector
{
    public interface IBulkSqlClient : ISqlClient
    {
        Task ExecuteBulkAsync(IConnectorConnection config, DataTable table, SqlTableName tableName);
    }
}
