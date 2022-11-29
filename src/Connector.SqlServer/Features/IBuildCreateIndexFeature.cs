using CluedIn.Connector.SqlServer.Connector;
using CluedIn.Connector.SqlServer.Utils;
using System.Collections.Generic;

namespace CluedIn.Connector.SqlServer.Features
{
    public interface IBuildCreateIndexFeature
    {
        SqlServerConnectorCommand BuildCreateIndexSql(SqlTableName tableName, IEnumerable<string> keys, bool useUniqueIndex);
        string GetCreateIndexCommandText(SqlTableName tableName, IEnumerable<string> keys, bool useUniqueIndex);
        string GetIndexName(SqlTableName tableName);
    }
}
