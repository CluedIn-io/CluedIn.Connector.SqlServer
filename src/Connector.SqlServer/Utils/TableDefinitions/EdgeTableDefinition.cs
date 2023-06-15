using CluedIn.Connector.SqlServer.Connector;
using CluedIn.Core.Data;
using CluedIn.Core.Streams.Models;
using Microsoft.Data.SqlClient;
using Microsoft.Data.SqlClient.Server;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Security.Cryptography;
using System.Text;

namespace CluedIn.Connector.SqlServer.Utils.TableDefinitions
{
    internal class EdgeTableDefinition
    {
        public static Guid GetEdgeId(Guid entityId, EntityEdge edge, EdgeDirection edgeDirection)
        {
            var code = GetRelevantReference(edge, edgeDirection).Code.Key;
            var propertiesConcatenated = string.Join("|", edge.Properties.Select(property => $"{property.Key}{property.Value}"));

            var concatenatedStrings = $"{entityId}{edge.EdgeType}{code}{propertiesConcatenated}";
            var md5 = MD5.Create();
            var data = md5.ComputeHash(Encoding.Default.GetBytes(concatenatedStrings));
            return new Guid(data);
        }

        private static EntityReference GetRelevantReference(EntityEdge edge, EdgeDirection edgeDirection)
        {
            return edgeDirection switch
            {
                EdgeDirection.Outgoing => edge.ToReference,
                EdgeDirection.Incoming => edge.FromReference,
                _ => throw new ArgumentOutOfRangeException(nameof(edgeDirection), edgeDirection, null)
            };
        }

        public static ColumnDefinition[] GetColumnDefinitions(StreamMode mode, EdgeDirection direction)
        {
            var codeColumnName = direction == EdgeDirection.Outgoing
                ? "ToCode"
                : "FromCode";

            switch (mode)
            {
                case StreamMode.EventStream:
                    return new ColumnDefinition[]
                    {
                        new("Id", SqlColumnHelper.UniqueIdentifier, IsPrimaryKey: true),
                        new("EntityId", SqlColumnHelper.UniqueIdentifier),
                        new("EdgeType", SqlColumnHelper.NVarcharMax),
                        new(codeColumnName, SqlColumnHelper.NVarchar1024),
                        new("ChangeType", SqlColumnHelper.NVarchar256),
                        new("CorrelationId", SqlColumnHelper.UniqueIdentifier, IsPrimaryKey: true)
                    };

                case StreamMode.Sync:
                    return new ColumnDefinition[]
                    {
                        new("Id", SqlColumnHelper.UniqueIdentifier, IsPrimaryKey: true),
                        new("EntityId", SqlColumnHelper.UniqueIdentifier),
                        new("EdgeType", SqlColumnHelper.NVarcharMax),
                        new(codeColumnName, SqlColumnHelper.NVarchar1024),
                    };

                default:
                    throw new ArgumentOutOfRangeException(nameof(mode), mode, null);
            }
        }

        public static IEnumerable<SqlDataRecord> GetSqlRecords(StreamMode streamMode, EdgeDirection direction, SqlConnectorEntityData connectorEntityData)
        {
            var edges = direction == EdgeDirection.Incoming
                ? connectorEntityData.IncomingEdges
                : connectorEntityData.OutgoingEdges;

            switch (streamMode)
            {
                case StreamMode.EventStream:
                    var eventModeColumnDefinitions = GetColumnDefinitions(streamMode, direction);

                    return edges.Select(edge =>
                    {
                        var sqlMetaData = eventModeColumnDefinitions.Select(column => column.ToSqlMetaData()).ToArray();

                        var code = direction == EdgeDirection.Outgoing
                            ? edge.ToReference.Code
                            : edge.FromReference.Code;

                        var guid = GetEdgeId(connectorEntityData.EntityId, edge, direction);
                        var record = new SqlDataRecord(sqlMetaData);
                        record.SetGuid(0, guid);
                        record.SetGuid(1, connectorEntityData.EntityId);
                        record.SetString(2, edge.EdgeType);
                        record.SetString(3, code.Key);
                        record.SetString(4, connectorEntityData.ChangeType.ToString());
                        record.SetGuid(5, connectorEntityData.CorrelationId.Value);
                        return record;
                    });

                case StreamMode.Sync:
                    var syncModeColumnDefinitions = GetColumnDefinitions(streamMode, direction);

                    return edges.Select(edge =>
                    {
                        var sqlMetaData = syncModeColumnDefinitions.Select(column => column.ToSqlMetaData()).ToArray();

                        var code = direction == EdgeDirection.Outgoing
                            ? edge.ToReference.Code
                            : edge.FromReference.Code;

                        var guid = GetEdgeId(connectorEntityData.EntityId, edge, direction);
                        var record = new SqlDataRecord(sqlMetaData);
                        record.SetGuid(0, guid);
                        record.SetGuid(1, connectorEntityData.EntityId);
                        record.SetString(2, edge.EdgeType);
                        record.SetString(3, code.Key);
                        return record;
                    });

                default:
                    throw new ArgumentOutOfRangeException(nameof(streamMode), streamMode, null);
            }
        }

        public static SqlParameter GetPivotKeyParameter(SqlConnectorEntityData connectorEntityData)
        {
            return new SqlParameter("@EntityId", SqlDbType.UniqueIdentifier) { Value = connectorEntityData.EntityId };
        }

        public static SqlServerConnectorCommand CreateUpsertCommand(IReadOnlyStreamModel streamModel, EdgeDirection direction, SqlConnectorEntityData connectorEntityData, SqlName schema)
        {
            var mode = streamModel.Mode ?? StreamMode.Sync; // TODO: Correct fallback?

            switch (mode)
            {
                case StreamMode.EventStream:
                    return BuildEventModeUpsertCommand(streamModel, direction, connectorEntityData, schema);

                case StreamMode.Sync:
                    return BuildSyncModeUpsertCommand(streamModel, direction, connectorEntityData, schema);

                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        private static SqlServerConnectorCommand BuildEventModeUpsertCommand(IReadOnlyStreamModel streamModel, EdgeDirection direction, SqlConnectorEntityData connectorEntityData, SqlName schema)
        {
            var edgeTableName = TableNameUtility.GetEdgesTableName(streamModel, direction, schema);
            var edgeTableType = CreateCustomTypeCommandUtility.GetEdgeTableCustomTypeName(streamModel, direction, schema);

            var insertText = $@"
INSERT INTO {edgeTableName.FullyQualifiedName}
SELECT * FROM @{edgeTableType.LocalName}";

            var eventStreamRecords = GetSqlRecords(StreamMode.EventStream, direction, connectorEntityData);
            if (!eventStreamRecords.Any())
            {
                eventStreamRecords = null;
            }

            var eventStreamRecordsParameter = new SqlParameter($"@{edgeTableType.LocalName}", SqlDbType.Structured) { Value = eventStreamRecords, TypeName = edgeTableType.FullyQualifiedName };
            return new SqlServerConnectorCommand { Text = insertText, Parameters = new[] { eventStreamRecordsParameter } };
        }

        private static SqlServerConnectorCommand BuildSyncModeUpsertCommand(IReadOnlyStreamModel streamModel, EdgeDirection direction, SqlConnectorEntityData connectorEntityData, SqlName schema)
        {
            var edgeTableName = TableNameUtility.GetEdgesTableName(streamModel, direction, schema);
            var edgeTableType = CreateCustomTypeCommandUtility.GetEdgeTableCustomTypeName(streamModel, direction, schema);

            var codeColumnName = direction == EdgeDirection.Outgoing
                ? "ToCode"
                : "FromCode";

            var commandText = $@"
-- Delete existing columns that no longer exist
DELETE {edgeTableName.FullyQualifiedName}
WHERE
    [EntityId] = @EntityId
    AND
    NOT EXISTS(
        SELECT
        1
        FROM
            @{edgeTableType.LocalName} newValues
        WHERE
            newValues.[Id] = {edgeTableName.FullyQualifiedName}.[Id]
    )

-- Add new columns
INSERT INTO {edgeTableName.FullyQualifiedName}
SELECT
    newValues.[Id],
    newValues.[EntityId],
    newValues.[EdgeType],
    newValues.[{codeColumnName}]
FROM
    @{edgeTableType.LocalName} newValues
    LEFT JOIN {edgeTableName.FullyQualifiedName} existingValues
    ON newValues.[Id] = existingValues.[Id]
WHERE existingValues.[Id] IS NULL";

            var sqlRecords = GetSqlRecords(StreamMode.Sync, direction, connectorEntityData);
            if (!sqlRecords.Any())
            {
                sqlRecords = null;
            }

            var entityIdParameter = new SqlParameter("@EntityId", SqlDbType.UniqueIdentifier) { Value = connectorEntityData.EntityId };
            var recordsParameter = new SqlParameter($"@{edgeTableType.LocalName}", SqlDbType.Structured) { Value = sqlRecords, TypeName = edgeTableType.FullyQualifiedName };

            return new SqlServerConnectorCommand { Text = commandText, Parameters = new[] { entityIdParameter, recordsParameter } };
        }
    }
}
