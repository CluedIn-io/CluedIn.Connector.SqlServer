using CluedIn.Connector.SqlServer.Connector;
using CluedIn.Connector.SqlServer.Utils;
using System.Collections.Generic;

namespace CluedIn.Connector.SqlServer.Features
{
    public sealed class DefaultBuildCreateIndexFeature : IBuildCreateIndexFeature
    {
        public SqlServerConnectorCommand BuildCreateIndexSql(SqlTableName tableName, IEnumerable<string> keys, bool useUniqueIndex)
        {
            var commandText = GetCreateIndexCommandText(tableName, keys, useUniqueIndex);
            return new SqlServerConnectorCommand { Text = commandText };
        }

        public string GetCreateIndexCommandText(SqlTableName tableName, IEnumerable<string> keys, bool useUniqueIndex)
        {
            var indexName = GetIndexName(tableName);
            return $"CREATE {(useUniqueIndex ? "UNIQUE" : string.Empty)} INDEX [{indexName}] ON {tableName.FullyQualifiedName}({string.Join(", ", keys)}); ";
        }

        public string GetIndexName(SqlTableName tableName)
        {
            return $"idx_{tableName.Schema}_{tableName.LocalName}";
        }
    }
}
