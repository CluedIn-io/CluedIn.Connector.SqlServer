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
        public static ColumnDefinition[] GetColumnDefinitions(StreamMode mode)
        {
            switch (mode)
            {
                case StreamMode.EventStream:
                    return new ColumnDefinition[]
                    {
                        new("EntityId", SqlColumnHelper.UniqueIdentifier, AddIndex: true),
                        new("Code", SqlColumnHelper.NVarchar1024),
                        new("IsDataPartOriginEntityCode", SqlColumnHelper.Bit),
                        new("ChangeType", SqlColumnHelper.Int),
                        new("CorrelationId", SqlColumnHelper.UniqueIdentifier)
                    };

                case StreamMode.Sync:
                    return new ColumnDefinition[]
                    {
                        new("EntityId", SqlColumnHelper.UniqueIdentifier, AddIndex: true),
                        new("Code", SqlColumnHelper.NVarchar1024),
                        new("IsDataPartOriginEntityCode", SqlColumnHelper.Bit),
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
                        record.SetBoolean(2, code.IsOriginEntityCode());
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
                        record.SetBoolean(2, code.IsOriginEntityCode());
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

            var insertText = $"""
                INSERT INTO {codeTableName.FullyQualifiedName}
                SELECT * FROM @{codeTableType.LocalName}
                """;

            var eventStreamRecords = GetSqlRecords(StreamMode.EventStream, connectorEntityData);
            if (!eventStreamRecords.Any())
            {
                eventStreamRecords = null;
            }

            var eventStreamRecordsParameter = new SqlParameter($"@{codeTableType.LocalName}", SqlDbType.Structured) { Value = eventStreamRecords, TypeName = codeTableType.FullyQualifiedName };
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
                [EntityId] = @EntityId
                AND
                NOT EXISTS(SELECT 1 FROM @{codeTableType.LocalName} newValues WHERE newValues.[Code] = {codeTableName.FullyQualifiedName}.[Code] AND newValues.[IsDataPartOriginEntityCode] = {codeTableName.FullyQualifiedName}.[IsDataPartOriginEntityCode])
                
                -- Add new columns
                INSERT INTO {codeTableName.FullyQualifiedName}
                SELECT @EntityId, newValues.[Code], newValues.[IsDataPartOriginEntityCode]
                FROM @{codeTableType.LocalName} newValues
                LEFT JOIN {codeTableName.FullyQualifiedName} existingValues
                ON existingValues.[EntityId] = @EntityId AND existingValues.[Code] = newValues.[Code] AND existingValues.[IsDataPartOriginEntityCode] = newValues.[IsDataPartOriginEntityCode]
                WHERE existingValues.[EntityId] IS NULL
                """;

            var sqlRecords = GetSqlRecords(StreamMode.Sync, connectorEntityData);
            if (!sqlRecords.Any())
            {
                sqlRecords = null;
            }

            var entityIdParameter = new SqlParameter("@EntityId", SqlDbType.UniqueIdentifier) { Value = connectorEntityData.EntityId };
            var recordsParameter = new SqlParameter($"@{codeTableType.LocalName}", SqlDbType.Structured) { Value = sqlRecords, TypeName = codeTableType.FullyQualifiedName };

            return new SqlServerConnectorCommand { Text = commandText, Parameters = new[] { entityIdParameter, recordsParameter } };
        }
    }
}
