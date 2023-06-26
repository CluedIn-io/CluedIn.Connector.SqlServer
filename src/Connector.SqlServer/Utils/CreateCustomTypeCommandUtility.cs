using CluedIn.Connector.SqlServer.Connector;
using CluedIn.Connector.SqlServer.Utils.TableDefinitions;
using CluedIn.Core.Connectors;
using CluedIn.Core.Streams.Models;
using System.Linq;

namespace CluedIn.Connector.SqlServer.Utils
{
    internal static class CreateCustomTypeCommandUtility
    {
        public static SqlTableName GetCodeTableCustomTypeName(IReadOnlyStreamModel model, SqlName schema)
        {
            return GetCustomTypeName(TableNameUtility.GetCodeTableName(model, schema), schema);
        }

        public static SqlTableName GetCodeTableCustomTypeName(IReadOnlyCreateContainerModelV2 model, SqlName schema)
        {
            return GetCustomTypeName(TableNameUtility.GetCodeTableName(model, schema), schema);
        }

        public static SqlTableName GetEdgeTableCustomTypeName(IReadOnlyStreamModel model, EdgeDirection edgeDirection, SqlName schema)
        {
            return GetCustomTypeName(TableNameUtility.GetEdgesTableName(model, edgeDirection, schema), schema);
        }

        public static SqlTableName GetEdgeTableCustomTypeName(IReadOnlyCreateContainerModelV2 model, EdgeDirection edgeDirection, SqlName schema)
        {
            return GetCustomTypeName(TableNameUtility.GetEdgesTableName(model, edgeDirection, schema), schema);
        }

        public static SqlTableName GetEdgePropertiesTableCustomTypeName(IReadOnlyStreamModel model, EdgeDirection edgeDirection, SqlName schema)
        {
            return GetCustomTypeName(TableNameUtility.GetEdgePropertiesTableName(model, edgeDirection, schema), schema);
        }

        public static SqlTableName GetEdgePropertiesTableCustomTypeName(IReadOnlyCreateContainerModelV2 model, EdgeDirection edgeDirection, SqlName schema)
        {
            return GetCustomTypeName(TableNameUtility.GetEdgePropertiesTableName(model, edgeDirection, schema), schema);
        }

        public static SqlServerConnectorCommand BuildCodeTableCustomTypeCommand(IReadOnlyCreateContainerModelV2 model, SqlName schema)
        {
            var columnDefinitions = CodeTableDefinition.GetColumnDefinitions(model.StreamMode);
            var customTypeName = GetCodeTableCustomTypeName(model, schema);

            var command = CreateCommand(customTypeName, columnDefinitions);

            return new SqlServerConnectorCommand { Text = command };
        }

        public static SqlServerConnectorCommand BuildEdgeTableCustomTypeCommand(IReadOnlyCreateContainerModelV2 model, EdgeDirection edgeDirection, SqlName schema)
        {
            var columnDefinitions = EdgeTableDefinition.GetColumnDefinitions(model.StreamMode, edgeDirection);
            var customTypeName = GetEdgeTableCustomTypeName(model, edgeDirection, schema);

            var command = CreateCommand(customTypeName, columnDefinitions);

            return new SqlServerConnectorCommand { Text = command };
        }

        public static SqlServerConnectorCommand BuildEdgePropertiesTableCustomTypeCommand(IReadOnlyCreateContainerModelV2 model, EdgeDirection edgeDirection, SqlName schema)
        {
            var tableDefinition = EdgePropertiesTableDefinition.GetColumnDefinitions(model.StreamMode);
            var customTypeName = GetEdgePropertiesTableCustomTypeName(model, edgeDirection, schema);

            var command = CreateCommand(customTypeName, tableDefinition);

            return new SqlServerConnectorCommand { Text = command };
        }

        private static SqlTableName GetCustomTypeName(SqlTableName tableName, SqlName schema)
        {
            return SqlName.FromUnsafe($"{tableName.LocalName}Type").ToTableName(schema);
        }

        private static string CreateCommand(SqlTableName typeName, ColumnDefinition[] columnDefinitions)
        {
            var columnsString = string.Join(", ", columnDefinitions.Select(column => $"[{column.Name}] {column.ConnectorSqlType.StringRepresentation}"));
            return $@"
IF Type_ID(N'{typeName.FullyQualifiedName}') IS NULL
BEGIN
    CREATE TYPE {typeName.FullyQualifiedName} AS TABLE({columnsString});
END";
        }
    }
}
