﻿using CluedIn.Connector.SqlServer.Connector;
using CluedIn.Core.Streams.Models;
using Microsoft.Data.SqlClient;
using Microsoft.Data.SqlClient.Server;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace CluedIn.Connector.SqlServer.Utils.TableDefinitions
{
    internal static class CodeTableDefinition
    {
        private static readonly string variablePrefix = "code_table_";

        public static ColumnDefinition IsDataPartOriginEntityCodeColumnDefinition = new("IsDataPartOriginEntityCode", SqlColumnHelper.Bit, CanBeNull: true);

        public static ColumnDefinition[] GetColumnDefinitions(StreamMode mode)
        {
            switch (mode)
            {
                case StreamMode.EventStream:
                    return new ColumnDefinition[]
                    {
                        new("EntityId", SqlColumnHelper.UniqueIdentifier, AddIndex: true),
                        new("Code", SqlColumnHelper.NVarchar1024),
                        IsDataPartOriginEntityCodeColumnDefinition,
                        new("ChangeType", SqlColumnHelper.Int),
                        new("CorrelationId", SqlColumnHelper.UniqueIdentifier)
                    };

                case StreamMode.Sync:
                    return new ColumnDefinition[]
                    {
                        new("EntityId", SqlColumnHelper.UniqueIdentifier, AddIndex: true),
                        new("Code", SqlColumnHelper.NVarchar1024),
                        IsDataPartOriginEntityCodeColumnDefinition,
                    };

                default:
                    throw new ArgumentOutOfRangeException(nameof(mode), mode, null);
            }
        }

        public static IEnumerable<SqlDataRecord> GetSqlRecords(StreamMode mode, SqlConnectorEntityData connectorEntityData)
        {
            var columnDefinitions = GetColumnDefinitions(mode);

            switch (mode)
            {
                case StreamMode.EventStream:
                    return connectorEntityData.EntityCodes?.Select(code =>
                    {
                        var sqlMetaData = columnDefinitions.Select(column => column.ToSqlMetaData()).ToArray();

                        var record = new SqlDataRecord(sqlMetaData);
                        record.SetGuid(0, connectorEntityData.EntityId);
                        record.SetString(1, code.Key);
                        record.SetValue(2, code.GetIsOriginEntityCodeDBValue());
                        record.SetInt32(3, (int)connectorEntityData.ChangeType);
                        record.SetGuid(4, (Guid)connectorEntityData.CorrelationId.Value);
                        return record;
                    });

                case StreamMode.Sync:
                    return connectorEntityData.EntityCodes.Select(code =>
                    {
                        var sqlMetaData = columnDefinitions.Select(column => column.ToSqlMetaData()).ToArray();

                        var record = new SqlDataRecord(sqlMetaData);
                        record.SetGuid(0, connectorEntityData.EntityId);
                        record.SetString(1, code.Key);
                        record.SetValue(2, code.GetIsOriginEntityCodeDBValue());
                        return record;
                    });

                default:
                    throw new ArgumentOutOfRangeException(nameof(mode), mode, null);
            }
        }

        public static SqlServerConnectorCommand CreateUpsertCommand(IReadOnlyStreamModel streamModel, SqlConnectorEntityData connectorEntityData, SqlName schema)
        {
            var mode = streamModel.Mode ?? StreamMode.Sync;

            switch (mode)
            {
                case StreamMode.EventStream:
                    return BuildEventModeUpsertCommand(streamModel, connectorEntityData, schema);

                case StreamMode.Sync:
                    return BuildSyncModeUpsertCommand(streamModel, connectorEntityData, schema);
                
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        private static SqlServerConnectorCommand BuildEventModeUpsertCommand(IReadOnlyStreamModel streamModel,
            SqlConnectorEntityData connectorEntityData, SqlName schema)
        {
            var codeTableName = TableNameUtility.GetCodeTableName(streamModel, schema);
            var codeTableType = CreateCustomTypeCommandUtility.GetCodeTableCustomTypeName(streamModel, schema);

            var columns = GetColumnDefinitions(StreamMode.EventStream).Select(column => column.Name).ToArray();
            var columnsConcatenated = string.Join(", ", columns);

            var tableTypeValuesName = $"{variablePrefix}newValues";
            var columnsBoxedAndConcatenated = string.Join(", ", columns.Select(column => $"{tableTypeValuesName}.[{column}]"));

            var insertText = $"""
                INSERT INTO {codeTableName.FullyQualifiedName} ({columnsConcatenated})
                SELECT {columnsBoxedAndConcatenated} FROM @{variablePrefix}{codeTableType.LocalName} {tableTypeValuesName}
                """;

            var eventStreamRecords = GetSqlRecords(StreamMode.EventStream, connectorEntityData);
            if (!eventStreamRecords.Any())
            {
                eventStreamRecords = null;
            }

            var eventStreamRecordsParameter = new SqlParameter($"@{variablePrefix}{codeTableType.LocalName}", SqlDbType.Structured) { Value = eventStreamRecords, TypeName = codeTableType.FullyQualifiedName };
            return new SqlServerConnectorCommand { Text = insertText, Parameters = new[] { eventStreamRecordsParameter } };
        }

        private static SqlServerConnectorCommand BuildSyncModeUpsertCommand(IReadOnlyStreamModel streamModel, SqlConnectorEntityData connectorEntityData, SqlName schema)
        {
            var codeTableName = TableNameUtility.GetCodeTableName(streamModel, schema);
            var codeTableType = CreateCustomTypeCommandUtility.GetCodeTableCustomTypeName(streamModel, schema);

            var commandText = $"""
                -- Delete existing columns that no longer exist
                DELETE {codeTableName.FullyQualifiedName}
                WHERE
                [EntityId] = @{variablePrefix}EntityId
                AND
                NOT EXISTS(SELECT 1 FROM @{variablePrefix}{codeTableType.LocalName} newValues WHERE newValues.[Code] = {codeTableName.FullyQualifiedName}.[Code] AND newValues.[IsDataPartOriginEntityCode] = {codeTableName.FullyQualifiedName}.[IsDataPartOriginEntityCode])
                
                -- Add new columns
                INSERT INTO {codeTableName.FullyQualifiedName}
                SELECT @{variablePrefix}EntityId, newValues.[Code], newValues.[IsDataPartOriginEntityCode]
                FROM @{variablePrefix}{codeTableType.LocalName} newValues
                LEFT JOIN {codeTableName.FullyQualifiedName} existingValues
                ON existingValues.[EntityId] = @{variablePrefix}EntityId AND existingValues.[Code] = newValues.[Code] AND existingValues.[IsDataPartOriginEntityCode] = newValues.[IsDataPartOriginEntityCode]
                WHERE existingValues.[EntityId] IS NULL
                """;

            var sqlRecords = GetSqlRecords(StreamMode.Sync, connectorEntityData);
            if (!sqlRecords.Any())
            {
                sqlRecords = null;
            }

            var entityIdParameter = new SqlParameter($"@{variablePrefix}EntityId", SqlDbType.UniqueIdentifier) { Value = connectorEntityData.EntityId };
            var recordsParameter = new SqlParameter($"@{variablePrefix}{codeTableType.LocalName}", SqlDbType.Structured) { Value = sqlRecords, TypeName = codeTableType.FullyQualifiedName };

            return new SqlServerConnectorCommand { Text = commandText, Parameters = new[] { entityIdParameter, recordsParameter } };
        }
    }
}
