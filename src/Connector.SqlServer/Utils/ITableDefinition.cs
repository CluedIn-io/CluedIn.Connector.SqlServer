using Microsoft.Data.SqlClient;
using Microsoft.Data.SqlClient.Server;
using System;
using System.Collections.Generic;

namespace CluedIn.Connector.SqlServer.Utils
{
    internal interface ITableDefinition
    {
        public ColumnDefinition[] ColumnDefinitions { get; }

        public IEnumerable<SqlDataRecord> GetSqlRecords(SqlConnectorEntityData connectorEntityData);
    }

    internal class TableDefinition : ITableDefinition
    {
        public ColumnDefinition[] ColumnDefinitions { get; }

        private Func<SqlConnectorEntityData, IEnumerable<SqlDataRecord>> _recordFunc;

        private Func<SqlConnectorEntityData, SqlParameter> _parameterFunc;

        public TableDefinition(ColumnDefinition[] columnDefinitions, Func<SqlConnectorEntityData, IEnumerable<SqlDataRecord>> recordFunc, Func<SqlConnectorEntityData, SqlParameter> parameterFunc)
        {
            ColumnDefinitions = columnDefinitions;
            _recordFunc = recordFunc;
            _parameterFunc = parameterFunc;
        }

        public IEnumerable<SqlDataRecord> GetSqlRecords(SqlConnectorEntityData connectorEntityData)
        {
            return _recordFunc.Invoke(connectorEntityData);
        }

        public SqlParameter GetPivotKeyParameter(SqlConnectorEntityData connectorEntityData)
        {
            return _parameterFunc.Invoke(connectorEntityData);
        }
    }


}
