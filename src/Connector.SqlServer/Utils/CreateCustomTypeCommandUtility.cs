using CluedIn.Connector.SqlServer.Connector;
using CluedIn.Connector.SqlServer.Utils.TableDefinitions;
using CluedIn.Core.Connectors;
using CluedIn.Core.Streams.Models;
using Microsoft.Data.SqlClient;
using System;
using System.Linq;

namespace CluedIn.Connector.SqlServer.Utils
{
    internal static class CreateCustomTypeCommandUtility
    {
        //*********************************************************************
        // Code table type name overloads
        //*********************************************************************
        public static SqlTableName GetCodeTableCustomTypeName(IReadOnlyStreamModel model, SqlName schema)
        {
            return GetCustomTypeName(TableNameUtility.GetCodeTableName(model, schema), schema);
        }

        public static SqlTableName GetCodeTableCustomTypeName(IReadOnlyCreateContainerModelV2 model, SqlName schema)
        {
            return GetCustomTypeName(TableNameUtility.GetCodeTableName(model, schema), schema);
        }

        public static SqlTableName GetCodeTableCustomTypeName(SqlTableName mainTableName, SqlName schema)
        {
            return GetCustomTypeName(TableNameUtility.GetCodeTableName(mainTableName, schema), schema);
        }

        public static SqlServerConnectorCommand BuildCreateCodeTableCustomTypeCommand(IReadOnlyCreateContainerModelV2 model, SqlName schema)
        {
            var columnDefinitions = CodeTableDefinition.GetColumnDefinitions(model.StreamMode);
            var customTypeName = GetCodeTableCustomTypeName(model, schema);

            var command = CreateCommand(customTypeName, columnDefinitions);

            return new SqlServerConnectorCommand { Text = command, Parameters = Array.Empty<SqlParameter>() };
        }

        public static SqlServerConnectorCommand BuildRenameCodeTableCustomTypeCommand(SqlTableName oldMainTableName, SqlTableName newMainTableName, SqlName schema)
        {
            var oldTypeName = GetCodeTableCustomTypeName(oldMainTableName, schema);
            var newTypeName = GetCodeTableCustomTypeName(newMainTableName, schema);

            var command = CreateRenameCommand(oldTypeName, newTypeName);

            return new SqlServerConnectorCommand { Text = command, Parameters = Array.Empty<SqlParameter>() };
        }

        //*********************************************************************
        // Edge table type name overloads
        //*********************************************************************
        public static SqlTableName GetEdgeTableCustomTypeName(IReadOnlyStreamModel model, EdgeDirection edgeDirection, SqlName schema)
        {
            return GetCustomTypeName(TableNameUtility.GetEdgesTableName(model, edgeDirection, schema), schema);
        }

        public static SqlTableName GetEdgeTableCustomTypeName(IReadOnlyCreateContainerModelV2 model, EdgeDirection edgeDirection, SqlName schema)
        {
            return GetCustomTypeName(TableNameUtility.GetEdgesTableName(model, edgeDirection, schema), schema);
        }

        public static SqlTableName GetEdgeTableCustomTypeName(SqlTableName mainTableName, EdgeDirection edgeDirection, SqlName schema)
        {
            return GetCustomTypeName(TableNameUtility.GetEdgesTableName(mainTableName, edgeDirection, schema), schema);
        }

        public static SqlServerConnectorCommand BuildRenameEdgeTableCustomTypeCommand(SqlTableName oldMainTableName, SqlTableName newMainTableName, EdgeDirection edgeDirection, SqlName schema)
        {
            var oldTypeName = GetEdgeTableCustomTypeName(oldMainTableName, edgeDirection, schema);
            var newTypeName = GetEdgeTableCustomTypeName(newMainTableName, edgeDirection, schema);

            var command = CreateRenameCommand(oldTypeName, newTypeName);

            return new SqlServerConnectorCommand { Text = command, Parameters = Array.Empty<SqlParameter>() };
        }

        public static SqlServerConnectorCommand BuildCreateEdgeTableCustomTypeCommand(IReadOnlyCreateContainerModelV2 model, EdgeDirection edgeDirection, SqlName schema)
        {
            var columnDefinitions = EdgeTableDefinition.GetColumnDefinitions(model.StreamMode, edgeDirection);
            var customTypeName = GetEdgeTableCustomTypeName(model, edgeDirection, schema);

            var command = CreateCommand(customTypeName, columnDefinitions);

            return new SqlServerConnectorCommand { Text = command, Parameters = Array.Empty<SqlParameter>() };
        }

        //*********************************************************************
        // Edge properties table type name overloads
        //*********************************************************************
        public static SqlTableName GetEdgePropertiesTableCustomTypeName(IReadOnlyStreamModel model, EdgeDirection edgeDirection, SqlName schema)
        {
            return GetCustomTypeName(TableNameUtility.GetEdgePropertiesTableName(model, edgeDirection, schema), schema);
        }

        public static SqlTableName GetEdgePropertiesTableCustomTypeName(IReadOnlyCreateContainerModelV2 model, EdgeDirection edgeDirection, SqlName schema)
        {
            return GetCustomTypeName(TableNameUtility.GetEdgePropertiesTableName(model, edgeDirection, schema), schema);
        }

        public static SqlTableName GetEdgePropertiesTableCustomTypeName(SqlTableName mainTableName, EdgeDirection edgeDirection, SqlName schema)
        {
            return GetCustomTypeName(TableNameUtility.GetEdgePropertiesTableName(mainTableName, edgeDirection, schema), schema);
        }

        public static SqlServerConnectorCommand BuildCreateEdgePropertiesTableCustomTypeCommand(IReadOnlyCreateContainerModelV2 model, EdgeDirection edgeDirection, SqlName schema)
        {
            var tableDefinition = EdgePropertiesTableDefinition.GetColumnDefinitions(model.StreamMode);
            var customTypeName = GetEdgePropertiesTableCustomTypeName(model, edgeDirection, schema);

            var command = CreateCommand(customTypeName, tableDefinition);

            return new SqlServerConnectorCommand { Text = command, Parameters = Array.Empty<SqlParameter>() };
        }

        public static SqlServerConnectorCommand BuildRenameEdgePropertiesTableCustomTypeCommand(SqlTableName oldMainTableName, SqlTableName newMainTableName, EdgeDirection edgeDirection, SqlName schema)
        {
            var oldTypeName = GetEdgePropertiesTableCustomTypeName(oldMainTableName, edgeDirection, schema);
            var newTypeName = GetEdgePropertiesTableCustomTypeName(newMainTableName, edgeDirection, schema);

            var command = CreateRenameCommand(oldTypeName, newTypeName);

            return new SqlServerConnectorCommand { Text = command, Parameters = Array.Empty<SqlParameter>() };
        }

        //*********************************************************************
        // Private utilities
        //*********************************************************************
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

        private static string CreateRenameCommand(SqlTableName oldTypeName, SqlTableName newTypeName)
        {
            return $@"
IF TYPE_ID(N'{oldTypeName}') IS NOT NULL
BEGIN
	EXEC sp_rename '{oldTypeName.FullyQualifiedName}', '{newTypeName.FullyQualifiedName}', 'USERDATATYPE'
END";
        }
    }
}
