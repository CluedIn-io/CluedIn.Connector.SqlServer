using CluedIn.Connector.Common;
using Microsoft.Data.SqlClient;
using System.Linq;

namespace CluedIn.Connector.SqlServer.Connector
{
    public class SqlServerConnectorCommand : CommandBase<SqlParameter>
    {
    }

    public static class SqlServerConnectorCommandExtensions
    {
        public static SqlCommand ToSqlCommand(this SqlServerConnectorCommand sqlServerConnectorCommand, SqlTransaction transaction)
        {
            var sqlCommand = transaction.Connection.CreateCommand();
            sqlCommand.CommandText = sqlServerConnectorCommand.Text;
            sqlCommand.Parameters.AddRange(sqlServerConnectorCommand.Parameters.ToArray());
            sqlCommand.Transaction = transaction;

            return sqlCommand;
        }
    }
}
