using CluedIn.Connector.SqlServer.Connector;
using CluedIn.Connector.SqlServer.Utils.TableDefinitions;
using CluedIn.Core.Connectors;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace CluedIn.Connector.SqlServer.Utils
{
    internal static class CreateTableCommandUtility
    {
        public static SqlServerConnectorCommand BuildMainTableCommand(IReadOnlyCreateContainerModelV2 model, SqlName schema)
        {
            var columnDefinitions = MainTableDefinition.GetColumnDefinitions(model);
            var tableName = TableNameUtility.GetMainTableName(model, schema);

            var builder = new StringBuilder();
            CreateTable(builder, tableName, columnDefinitions);

            return new SqlServerConnectorCommand { Text = builder.ToString() };
        }

        public static SqlServerConnectorCommand BuildCodeTableCommand(IReadOnlyCreateContainerModelV2 model, SqlName schema)
        {
            var columnDefinitions = CodeTableDefinition.GetColumnDefinitions(model.StreamMode);
            var tableName = TableNameUtility.GetCodeTableName(model, schema);

            var builder = new StringBuilder();
            CreateTable(builder, tableName, columnDefinitions);

            return new SqlServerConnectorCommand() { Text = builder.ToString() };
        }

        public static SqlServerConnectorCommand BuildEdgeTableCommand(IReadOnlyCreateContainerModelV2 model, EdgeDirection edgeDirection, SqlName schema)
        {
            var columnDefinitions = EdgeTableDefinition.GetColumnDefinitions(model.StreamMode, edgeDirection);
            var tableName = TableNameUtility.GetEdgesTableName(model, edgeDirection, schema);

            var builder = new StringBuilder();
            CreateTable(builder, tableName, columnDefinitions);

            return new SqlServerConnectorCommand() { Text = builder.ToString() };
        }

        public static SqlServerConnectorCommand BuildEdgePropertiesTableCommand(IReadOnlyCreateContainerModelV2 model, EdgeDirection edgeDirection, SqlName schema)
        {
            var columnDefinitions = EdgePropertiesTableDefinition.GetColumnDefinitions(model.StreamMode);
            var tableName = TableNameUtility.GetEdgePropertiesTableName(model, edgeDirection, schema);

            var builder = new StringBuilder();
            CreateTable(builder, tableName, columnDefinitions);

            return new SqlServerConnectorCommand { Text = builder.ToString() };
        }

        private static void CreateTable(StringBuilder builder, SqlTableName tableName, ColumnDefinition[] columns)
        {
            builder.AppendLine($"CREATE TABLE {tableName.FullyQualifiedName}(");

            AddColumns(builder, tableName, columns);
            AddPrimaryConstraint(builder, tableName, columns.Where(column => column.IsPrimaryKey).ToArray());

            builder.AppendLine(") ON[PRIMARY]");

            AddIndexes(builder, tableName, columns.Where(column => column.AddIndex));
        }

        private static void AddColumns(StringBuilder builder, SqlTableName tableName, ColumnDefinition[] columns)
        {
            foreach (var column in columns)
            {
                AddColumn(builder, column);
            }
        }

        private static void AddColumn(StringBuilder builder, ColumnDefinition column)
        {
            var nullString = column.CanBeNull ? "NULL" : "NOT NULL";
            builder.AppendLine($"[{column.Name.ToSanitizedSqlName()}] {column.ConnectorSqlType.StringRepresentation} {nullString},");
        }

        private static void AddPrimaryConstraint(StringBuilder builder, SqlTableName tableName, ColumnDefinition[] primaryKeys)
        {
            if (!primaryKeys.Any())
            {
                return;
            }

            var keysJoinedWithUnderscore = string.Join("_", primaryKeys.Select(key => key.Name.ToSanitizedSqlName()));
            var keysJoinedWithComma = string.Join(", ", primaryKeys.Select(key => $"[{key.Name.ToSanitizedSqlName()}]"));
            var primaryKeyName = $"PK_{tableName.LocalName}_{keysJoinedWithUnderscore}".ToSanitizedSqlName();

            builder.AppendLine($"CONSTRAINT [{primaryKeyName}] PRIMARY KEY NONCLUSTERED ({keysJoinedWithComma})");
        }

        private static void AddIndexes(StringBuilder builder, SqlTableName tableName, IEnumerable<ColumnDefinition> indexKeys)
        {
            foreach (var column in indexKeys)
            {
                AddIndex(builder, tableName, column);
            }
        }

        private static void AddIndex(StringBuilder builder, SqlTableName tableName, ColumnDefinition indexKey)
        {
            var clusteredString = "NONCLUSTERED";
            var sanitizedName = indexKey.Name.ToSanitizedSqlName();
            var sanitizedIndexName = $"IX_{tableName.LocalName}_{sanitizedName}".ToSanitizedSqlName();
            builder.AppendLine($"CREATE {clusteredString} INDEX [{sanitizedIndexName}] ON {tableName.FullyQualifiedName} ([{sanitizedName}])");
        }
    }
}
