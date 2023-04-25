using CluedIn.Connector.Common.Clients;
using Microsoft.Data.SqlClient;

namespace CluedIn.Connector.SqlServer.Connector
{
    public interface ISqlClient : ITransactionalClientBaseV2<SqlConnection, SqlTransaction, SqlParameter>
    {
    }
}
