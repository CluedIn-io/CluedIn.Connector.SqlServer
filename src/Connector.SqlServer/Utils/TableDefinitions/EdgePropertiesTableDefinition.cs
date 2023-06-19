using CluedIn.Connector.SqlServer.Connector;
using CluedIn.Core.Data;
using CluedIn.Core.Streams.Models;
using Microsoft.Data.SqlClient;
using Microsoft.Data.SqlClient.Server;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace CluedIn.Connector.SqlServer.Utils.TableDefinitions
{
    internal static class EdgePropertiesTableDefinition
    {
        public static ColumnDefinition[] GetColumnDefinitions(StreamMode mode)
        {
            switch (mode)
            {
                case StreamMode.EventStream:
                    return new ColumnDefinition[]
                    {
                        new("EdgeId", SqlColumnHelper.UniqueIdentifier, IsPrimaryKey: true),
                        new("KeyName", SqlColumnHelper.NVarchar256, IsPrimaryKey: true),
                        new("Value", SqlColumnHelper.NVarcharMax),
                        new("ChangeType", SqlColumnHelper.NVarchar256),
                        new("CorrelationId", SqlColumnHelper.UniqueIdentifier, IsPrimaryKey: true)
                    };

                case StreamMode.Sync:
                    return new ColumnDefinition[]
                    {
                        new("EdgeId", SqlColumnHelper.UniqueIdentifier, IsPrimaryKey: true),
                        new("KeyName", SqlColumnHelper.NVarchar256, IsPrimaryKey: true),
                        new("Value", SqlColumnHelper.NVarcharMax),
                    };

                default:
                    throw new ArgumentOutOfRangeException(nameof(mode), mode, null);
            }
        }

        public static IEnumerable<SqlDataRecord> GetSqlDataRecords(StreamMode mode, SqlConnectorEntityData connectorEntityData, EdgeDirection direction)
        {
            var edges = direction == EdgeDirection.Incoming
                ? connectorEntityData.IncomingEdges
                : connectorEntityData.OutgoingEdges;

            switch (mode)
            {
                case StreamMode.EventStream:
                    var eventStreamPropertyRecords = edges
                        .SelectMany(edge => edge.Properties
                            .Select(property =>
                            {
                                var sqlMetaData = GetColumnDefinitions(mode).Select(column => column.ToSqlMetaData()).ToArray();
                                var edgeId = EdgeTableDefinition.GetEdgeId(connectorEntityData.EntityId, edge, direction);

                                var record = new SqlDataRecord(sqlMetaData);
                                record.SetGuid(0, edgeId);
                                record.SetString(1, property.Key);
                                record.SetString(2, property.Value);
                                record.SetString(3, connectorEntityData.ChangeType.ToString());
                                record.SetGuid(4, connectorEntityData.CorrelationId.Value);

                                return record;
                            }));

                    return eventStreamPropertyRecords;

                case StreamMode.Sync:
                    var syncStreamPropertyRecords = edges
                        .SelectMany(edge => edge.Properties
                            .Select(property =>
                            {
                                var sqlMetaData = GetColumnDefinitions(mode).Select(column => column.ToSqlMetaData()).ToArray();
                                var edgeId = EdgeTableDefinition.GetEdgeId(connectorEntityData.EntityId, edge, direction);

                                var record = new SqlDataRecord(sqlMetaData);
                                record.SetGuid(0, edgeId);
                                record.SetString(1, property.Key);
                                record.SetString(2, property.Value);

                                return record;
                            }));

                    return syncStreamPropertyRecords;

                default:
                    throw new ArgumentOutOfRangeException(nameof(mode), mode, null);
            }
        }

        public static SqlServerConnectorCommand CreateUpsertCommands(IReadOnlyStreamModel streamModel, EdgeDirection direction, SqlConnectorEntityData connectorEntityData, SqlName schema)
        {
            var mode = streamModel.Mode ?? StreamMode.Sync;

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
            var edgePropertiesTableName = TableNameUtility.GetEdgePropertiesTableName(streamModel, direction, schema);
            var edgePropertiesTableType = CreateCustomTypeCommandUtility.GetEdgePropertiesTableCustomTypeName(streamModel, direction, schema);

            var edges = direction == EdgeDirection.Incoming
                ? connectorEntityData.IncomingEdges
                : connectorEntityData.OutgoingEdges;

            var insertText = $"""
                INSERT INTO {edgePropertiesTableName.FullyQualifiedName}
                SELECT * FROM @{edgePropertiesTableType.LocalName}
                """;

            var eventStreamRecords = GetSqlDataRecords(StreamMode.EventStream, connectorEntityData, direction);
            if (!eventStreamRecords.Any())
            {
                eventStreamRecords = null;
            }

            var eventStreamRecordsParameter = new SqlParameter($"@{edgePropertiesTableType.LocalName}", SqlDbType.Structured) { Value = eventStreamRecords, TypeName = edgePropertiesTableType.FullyQualifiedName };
            return new SqlServerConnectorCommand { Text = insertText, Parameters = new[] { eventStreamRecordsParameter } };
        }

        private static SqlServerConnectorCommand BuildSyncModeUpsertCommand(IReadOnlyStreamModel streamModel, EdgeDirection direction, SqlConnectorEntityData connectorEntityData, SqlName schema)
        {
            var edgePropertiesTableName = TableNameUtility.GetEdgePropertiesTableName(streamModel, direction, schema);
            var edgePropertiesTableType = CreateCustomTypeCommandUtility.GetEdgePropertiesTableCustomTypeName(streamModel, direction, schema);
            var edgeTableName = TableNameUtility.GetEdgesTableName(streamModel, direction, schema);

            var commandText = $"""
                -- Delete existing columns that no longer exist
                DELETE {edgePropertiesTableName.FullyQualifiedName}
                WHERE
                    EXISTS(
                        SELECT
                            1
                        FROM
                            {edgeTableName.FullyQualifiedName} edge
                        WHERE
                            edge.[Id] = {edgePropertiesTableName.FullyQualifiedName}.[EdgeId]
                            AND edge.[EntityId] = @EntityId
                    )
                    AND NOT EXISTS(
                        SELECT
                            1
                        FROM
                            @{edgePropertiesTableType.LocalName} newValues
                        WHERE
                            newValues.[EdgeId] = {edgePropertiesTableName.FullyQualifiedName}.[EdgeId]
                    )
                
                -- Add new columns
                INSERT INTO
                    {edgePropertiesTableName.FullyQualifiedName}
                SELECT
                    newValues.[EdgeId],
                    newValues.[KeyName],
                    newValues.[Value]
                FROM
                    @{edgePropertiesTableType.LocalName} newValues
                    LEFT JOIN {edgePropertiesTableName.FullyQualifiedName} existingValues 
                	ON newValues.[EdgeId] = existingValues.[EdgeId]
                WHERE
                    existingValues.[EdgeId] IS NULL
                """;


            var sqlDataRecords = GetSqlDataRecords(StreamMode.Sync, connectorEntityData, direction).ToArray();
            if (!sqlDataRecords.Any())
            {
                sqlDataRecords = null;
            }

            var entityIdParameter = new SqlParameter("@EntityId", SqlDbType.UniqueIdentifier) { Value = connectorEntityData.EntityId };
            var recordsParameter = new SqlParameter($"@{edgePropertiesTableType.LocalName}", SqlDbType.Structured) { Value = sqlDataRecords, TypeName = edgePropertiesTableType.FullyQualifiedName };

            return new SqlServerConnectorCommand { Text = commandText, Parameters = new[] { entityIdParameter, recordsParameter } };
        }
    }
}
