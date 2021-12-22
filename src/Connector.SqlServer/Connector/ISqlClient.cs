using CluedIn.Connector.Common;
using Microsoft.Data.SqlClient;

namespace CluedIn.Connector.SqlServer.Connector
{
    public interface ISqlClient : IClientBase<SqlConnection, SqlParameter>
    {
    }
}
