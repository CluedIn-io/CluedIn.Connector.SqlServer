﻿using Microsoft.Data.SqlClient;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;

namespace CluedIn.Connector.SqlServer.Connector
{
    public interface ISqlClient
    {
        Task<SqlConnection> BeginConnection(IReadOnlyDictionary<string, object> config);

        Task<DataTable> GetTableColumns(SqlConnection connection, string tableName, string schema);

        Task<DataTable> GetTables(SqlConnection connection, string tableName = null, string schema = null);

        Task<ConnectionAndTransaction> BeginTransaction(IReadOnlyDictionary<string, object> config);

        Task ExecuteCommandInTransactionAsync(SqlTransaction transaction, string commandText, IEnumerable<SqlParameter> param = null);
    }
}
