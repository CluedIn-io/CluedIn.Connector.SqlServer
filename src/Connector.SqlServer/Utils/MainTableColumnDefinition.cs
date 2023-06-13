using System;
using static CluedIn.Connector.SqlServer.Utils.SqlColumnHelper;

namespace CluedIn.Connector.SqlServer.Utils
{
    internal record MainTableColumnDefinition(
        string Name,
        ConnectorSqlType ConnectorSqlType,
        Func<(SqlConnectorEntityData data, DateTimeOffset timeStamp), object> GetValueFunc,
        bool CanBeNull = false,
        bool IsPrimaryKey = false,
        bool IsPivotKey = false,
        bool IsPivotValueKey = false,
        bool AddIndex = false
    ) : ColumnDefinition(Name, ConnectorSqlType, CanBeNull, IsPrimaryKey, IsPivotKey, IsPivotValueKey, AddIndex);
}
