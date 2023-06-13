using CluedIn.Connector.SqlServer.Connector;
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
                        new("EntityId", SqlColumnHelper.UniqueIdentifier, AddIndex: true, IsPivotKey: true),
                        new("Code", SqlColumnHelper.NVarchar1024, IsPivotValueKey: true),
                        new("ChangeType", SqlColumnHelper.NVarchar256),
                        new("CorrelationId", SqlColumnHelper.UniqueIdentifier)
                    };

                case StreamMode.Sync:
                    return new ColumnDefinition[]
                    {
                        new("EntityId", SqlColumnHelper.UniqueIdentifier, AddIndex: true, IsPivotKey: true),
                        new("Code", SqlColumnHelper.NVarchar1024, IsPivotValueKey: true),
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
                        record.SetString(2, connectorEntityData.ChangeType.ToString());
                        record.SetGuid(3, connectorEntityData.CorrelationId.Value);
                        return record;
                    });

                case StreamMode.Sync:
                    return connectorEntityData.EntityCodes.Select(code =>
                    {
                        var sqlMetaData = columnDefinitions.Select(column => column.ToSqlMetaData()).ToArray();

                        var record = new SqlDataRecord(sqlMetaData);
                        record.SetGuid(0, connectorEntityData.EntityId);
                        record.SetString(1, code.Key);
                        return record;
                    });

                default:
                    throw new ArgumentOutOfRangeException(nameof(mode), mode, null);
            }
        }

        public static SqlParameter GetPivotKeyParameter(StreamMode mode, SqlConnectorEntityData connectorEntityData)
        {
            return new SqlParameter("@EntityId", SqlDbType.UniqueIdentifier) { Value = connectorEntityData.EntityId };
        }

        public static SqlServerConnectorCommand CreateUpsertCommand(IReadOnlyStreamModel streamModel, SqlConnectorEntityData connectorEntityData, SqlName schema)
        {
            var mode = streamModel.Mode ?? StreamMode.Sync; // TODO: Correct fallback?

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
            var eventStreamRecords = GetSqlRecords(StreamMode.EventStream, connectorEntityData);
            var eventStreamRecordsParameter = new SqlParameter($"@{codeTableType.LocalName}", SqlDbType.Structured) { Value = eventStreamRecords, TypeName = codeTableType.FullyQualifiedName };

            var insertText = $@"
INSERT INTO {codeTableName.FullyQualifiedName}
SELECT * FROM @{codeTableType.LocalName}";

            return new SqlServerConnectorCommand { Text = insertText, Parameters = new[] { eventStreamRecordsParameter } };
        }

        private static SqlServerConnectorCommand BuildSyncModeUpsertCommand(IReadOnlyStreamModel streamModel, SqlConnectorEntityData connectorEntityData, SqlName schema)
        {
            var codeTableName = TableNameUtility.GetCodeTableName(streamModel, schema);
            var codeTableType = CreateCustomTypeCommandUtility.GetCodeTableCustomTypeName(streamModel, schema);

            var commandText = $@"
-- Delete existing columns that no longer exist
DELETE {codeTableName.FullyQualifiedName}
WHERE
[EntityId] = @EntityId
AND
NOT EXISTS(SELECT 1 FROM @{codeTableType.LocalName} newValues WHERE newValues.[Code] = {codeTableName.FullyQualifiedName}.[Code])

-- Add new columns
INSERT INTO {codeTableName.FullyQualifiedName}
SELECT @EntityId, newValues.[Code]
FROM @{codeTableType.LocalName} newValues
LEFT JOIN {codeTableName.FullyQualifiedName} existingValues
ON existingValues.[EntityId] = @EntityId AND existingValues.[Code] = newValues.[Code]
WHERE existingValues.[EntityId] IS NULL";

            var sqlRecords = GetSqlRecords(StreamMode.Sync, connectorEntityData);

            var entityIdParameter = new SqlParameter("@EntityId", SqlDbType.UniqueIdentifier) { Value = connectorEntityData.EntityId };
            var recordsParameter = new SqlParameter($"@{codeTableType.LocalName}", SqlDbType.Structured) { Value = sqlRecords, TypeName = codeTableType.FullyQualifiedName };

            return new SqlServerConnectorCommand { Text = commandText, Parameters = new[] { entityIdParameter, recordsParameter } };
        }
    }
}
