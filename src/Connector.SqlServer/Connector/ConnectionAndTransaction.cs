using Microsoft.Data.SqlClient;
using System;
using System.Data;
using System.Threading.Tasks;

namespace CluedIn.Connector.SqlServer.Connector
{
    public class ConnectionAndTransaction : IDisposable, IAsyncDisposable
    {
        public readonly SqlConnection Connection;
        public readonly SqlTransaction Transaction;

        public ConnectionAndTransaction(SqlConnection connection, SqlTransaction transaction)
        {
            Connection = connection;
            this.Transaction = transaction;
        }

        public void Dispose()
        {
            Transaction.Dispose();
            Connection.Dispose();
        }

        public async ValueTask DisposeAsync()
        {
            if (Transaction is IAsyncDisposable t)
            {
                await t.DisposeAsync();
            }
            else
            {
                Transaction.Dispose();
            }

            if (Connection is IAsyncDisposable c)
            {
                await c.DisposeAsync();
            }
            else
            {
                Connection.Dispose();
            }
        }
    }
}
