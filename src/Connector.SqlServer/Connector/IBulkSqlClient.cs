using CluedIn.Connector.SqlServer.Utility;
using CluedIn.Core.Connectors;
using System.Data;
using System.Threading.Tasks;

namespace CluedIn.Connector.SqlServer.Connector
{
    public interface IBulkSqlClient : ISqlClient
    {
        Task ExecuteBulkAsync(IConnectorConnection config, DataTable table, SanitizedSqlString tableName);
    }
}
