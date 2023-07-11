using Microsoft.Data.SqlClient;
using System.Collections.Generic;
using System.Linq;

namespace CluedIn.Connector.SqlServer.Connector
{
    public class SqlServerConnectorCommand
    {
        public string Text;
        public IEnumerable<SqlParameter> Parameters;

        public SqlCommand ToSqlCommand(SqlTransaction transaction)
        {
            var sqlCommand = transaction.Connection.CreateCommand();
            sqlCommand.CommandText = Text;
            sqlCommand.Parameters.AddRange(Parameters.ToArray());
            sqlCommand.Transaction = transaction;

            return sqlCommand;
        }
    }
}
