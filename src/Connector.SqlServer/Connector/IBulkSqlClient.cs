using CluedIn.Core.Connectors;
using System.Data;
using System.Threading.Tasks;

namespace CluedIn.Connector.SqlServer.Connector
{
    public interface IBulkSqlClient : ISqlClient
    {
        Task ExecuteBulkAsync(IConnectorConnection config, DataTable table, string containerName);
    }
}
