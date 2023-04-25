using CluedIn.Connector.SqlServer.Features;
using CluedIn.Core;
using CluedIn.Core.Connectors;
using CluedIn.Core.Data;
using CluedIn.Core.Streams.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;

namespace CluedIn.Connector.SqlServer.Utils
{
    internal class ColumnDefinitionsUtility
    {
        //public static MainTableColumnDefinition[] GetMainTableDefinitions(IReadOnlyCreateContainerModelV2 model)
        //{
        //    return GetMainTableColumnDefinitions(model.StreamMode, model.Properties.Select(property => (property.Name, property.DataType)).ToList());
        //}

        //public static MainTableColumnDefinition[] GetMainTableDefinitions(IReadOnlyConnectorEntityData model)
        //{
        //    return GetMainTableColumnDefinitions(model.StreamMode, model.Properties.Select(property => (property.Name, property.DataType)).ToList());
        //}

        public static MainTableColumnDefinition[] GetMainTableColumnDefinitions(StreamMode streamMode, IReadOnlyCollection<(string name, ConnectorPropertyDataType dataType)> properties)
        {
            var columnDefinitions = new MainTableColumnDefinition[]
            {
                new ("Id", SqlColumnHelper.UniqueIdentifier, input => input.data.EntityId, IsPrimaryKey: true),
                new ("PersistVersion", SqlColumnHelper.Int, input => input.data.PersistInfo!.PersistVersion),
                new ("PersistHash", SqlColumnHelper.Char24, input => input.data.PersistInfo!.PersistHash),
                new ("OriginEntityCode", SqlColumnHelper.NVarchar1024, input => input.data.OriginEntityCode.ToString()),
                new ("EntityType", SqlColumnHelper.NVarcharMax, input => input.data.EntityType.ToString()),
                new ("TimeStamp", SqlColumnHelper.DateTimeOffset7, input => input.timeStamp)
            };

            if (streamMode == StreamMode.EventStream)
            {
                var eventStreamColumnDefinitions = new MainTableColumnDefinition[]
                {
                    new("ChangeType", SqlColumnHelper.NVarchar256, input => input.data.ChangeType),
                    new("CorrelationId", SqlColumnHelper.UniqueIdentifier, input => input.data.CorrelationId)
                };

                columnDefinitions.AddRange(eventStreamColumnDefinitions);
            }

            var propertyDefinitions = properties.Select(property =>
            {
                var name = property.name.ToSanitizedSqlName();
                var sqlType = SqlColumnHelper.GetColumnType(property.dataType);
                return new MainTableColumnDefinition(
                    name,
                    sqlType,
                    input => input.data.Properties.First(x => x.Name == property.name).Value,
                    CanBeNull: true);
            });

            return columnDefinitions.Concat(propertyDefinitions).ToArray();
        }

        //public static TableDefinition GetCodeTableDefinition(IReadOnlyStreamModel model) =>
        //    GetCodeTableDefinition(model.Mode ?? StreamMode.Sync); // TODO

        //public static TableDefinition GetCodeTableDefinition(IReadOnlyCreateContainerModelV2 model) =>
        //    GetCodeTableDefinition(model.StreamMode);

        //private static TableDefinition GetCodeTableDefinition(StreamMode streamMode)
        //{
        //    switch (streamMode)
        //    {
        //        case StreamMode.EventStream:
        //            var eventModeColumnDefinitions = new ColumnDefinition[]
        //            {
        //                new("EntityId", SqlColumnHelper.UniqueIdentifier, SqlDbType.UniqueIdentifier, AddIndex: true, IsPivotKey: true),
        //                new("Code", SqlColumnHelper.NVarchar1024, SqlDbType.NVarChar, IsPivotValueKey: true),
        //                new("ChangeType", SqlColumnHelper.NVarchar256, SqlDbType.NVarChar),
        //                new("CorrelationId", SqlColumnHelper.UniqueIdentifier, SqlDbType.UniqueIdentifier)
        //            };

        //            var eventTableDefinition = new TableDefinition(eventModeColumnDefinitions,
        //                recordFunc: input =>
        //                {
        //                    return input.EntityCodes.Select(code =>
        //                    {
        //                        var sqlMetaData = eventModeColumnDefinitions.Select(column => column.ToSqlMetaData()).ToArray();

        //                        var record = new SqlDataRecord(sqlMetaData);
        //                        record.SetGuid(0, input.EntityId);
        //                        record.SetString(1, code.Key);
        //                        record.SetString(2, input.ChangeType.ToString());
        //                        record.SetGuid(3, input.CorrelationId.Value);
        //                        return record;
        //                    });
        //                },
        //                parameterFunc: input =>
        //                {
        //                    return new SqlParameter("@EntityId", SqlDbType.UniqueIdentifier) { Value = input.EntityId };
        //                });

        //            return eventTableDefinition;

        //        case StreamMode.Sync:
        //            var syncModeColumnDefinitions = new ColumnDefinition[]
        //            {
        //                new("EntityId", SqlColumnHelper.UniqueIdentifier, SqlDbType.UniqueIdentifier, AddIndex: true, IsPivotKey: true),
        //                new("Code", SqlColumnHelper.NVarchar1024, SqlDbType.NVarChar, IsPivotValueKey: true),
        //            };

        //            var syncTableDefinition = new TableDefinition(syncModeColumnDefinitions,
        //                input =>
        //                {
        //                    return input.EntityCodes.Select(code =>
        //                    {
        //                        var sqlMetaData = syncModeColumnDefinitions.Select(column => column.ToSqlMetaData()).ToArray();

        //                        var record = new SqlDataRecord(sqlMetaData);
        //                        record.SetGuid(0, input.EntityId);
        //                        record.SetString(1, code.Key);
        //                        return record;
        //                    });
        //                },
        //                parameterFunc: input =>
        //                {
        //                    return new SqlParameter("@EntityId", SqlDbType.UniqueIdentifier) { Value = input.EntityId };
        //                });

        //            return syncTableDefinition;

        //        default:
        //            throw new ArgumentOutOfRangeException(nameof(streamMode), streamMode, null);
        //    }
        //}

        //public static TableDefinition GetEdgeTableDefinitions(IReadOnlyStreamModel model, EdgeDirection direction) =>
        //    GetEdgeTableDefinitions(model.Mode ?? StreamMode.Sync, direction);

        //public static TableDefinition GetEdgeTableDefinitions(IReadOnlyCreateContainerModelV2 model, EdgeDirection direction) =>
        //    GetEdgeTableDefinitions(model.StreamMode, direction);

        //public static TableDefinition GetEdgeTableDefinitions(StreamMode streamMode, EdgeDirection direction)
        //{
        //    var codeColumnName = direction == EdgeDirection.Outgoing
        //        ? "ToCode"
        //        : "FromCode";

        //    switch (streamMode)
        //    {
        //        case StreamMode.EventStream:
        //            var eventModeColumnDefinitions = new ColumnDefinition[]
        //            {
        //                new("Id", SqlColumnHelper.UniqueIdentifier, SqlDbType.UniqueIdentifier, IsPrimaryKey: true),
        //                new("EntityId", SqlColumnHelper.UniqueIdentifier, SqlDbType.UniqueIdentifier, IsPivotKey: true),
        //                new("EdgeType", SqlColumnHelper.NVarcharMax, SqlDbType.NVarChar, IsPivotValueKey: true),
        //                new(codeColumnName, SqlColumnHelper.NVarchar1024, SqlDbType.NVarChar, IsPivotValueKey: true),
        //                new("ChangeType", SqlColumnHelper.NVarchar256, SqlDbType.Char),
        //                new("CorrelationId", SqlColumnHelper.UniqueIdentifier, SqlDbType.UniqueIdentifier)
        //            };

        //            var eventTableDefinition = new TableDefinition(
        //                eventModeColumnDefinitions,
        //                recordFunc: input =>
        //                {
        //                    var edges = direction == EdgeDirection.Incoming
        //                        ? input.IncomingEdges
        //                        : input.OutgoingEdges;

        //                    return edges.Select(edge =>
        //                    {
        //                        var sqlMetaData = eventModeColumnDefinitions.Select(column => column.ToSqlMetaData()).ToArray();

        //                        var code = direction == EdgeDirection.Outgoing
        //                            ? edge.ToReference.Code
        //                            : edge.FromReference.Code;

        //                        var guid = GetEdgeId(input.EntityId, edge, direction);
        //                        var record = new SqlDataRecord(sqlMetaData);
        //                        record.SetGuid(0, guid);
        //                        record.SetGuid(1, input.EntityId);
        //                        record.SetString(2, edge.EdgeType);
        //                        record.SetString(3, code.Key);
        //                        record.SetString(4, input.ChangeType.ToString());
        //                        record.SetGuid(5, input.CorrelationId.Value);
        //                        return record;
        //                    });
        //                },
        //                parameterFunc: input =>
        //                {
        //                    return new SqlParameter("@EntityId", SqlDbType.UniqueIdentifier) { Value = input.EntityId };
        //                });

        //            return eventTableDefinition;

        //        case StreamMode.Sync:
        //            var syncModeColumnDefinitions = new ColumnDefinition[]
        //            {
        //                new("Id", SqlColumnHelper.UniqueIdentifier, SqlDbType.UniqueIdentifier, IsPrimaryKey: true),
        //                new("EntityId", SqlColumnHelper.UniqueIdentifier, SqlDbType.UniqueIdentifier, IsPivotKey: true),
        //                new("EdgeType", SqlColumnHelper.NVarcharMax, SqlDbType.NVarChar, IsPivotValueKey: true),
        //                new(codeColumnName, SqlColumnHelper.NVarchar1024, SqlDbType.NVarChar, IsPivotValueKey: true),
        //            };

        //            var syncTableDefinition = new TableDefinition(
        //                syncModeColumnDefinitions,
        //                recordFunc: input =>
        //                {
        //                    var edges = direction == EdgeDirection.Incoming
        //                        ? input.IncomingEdges
        //                        : input.OutgoingEdges;

        //                    return edges.Select(edge =>
        //                    {
        //                        var sqlMetaData = syncModeColumnDefinitions.Select(column => column.ToSqlMetaData()).ToArray();

        //                        var code = direction == EdgeDirection.Outgoing
        //                            ? edge.ToReference.Code
        //                            : edge.FromReference.Code;

        //                        var guid = GetEdgeId(input.EntityId, edge, direction);
        //                        var record = new SqlDataRecord(sqlMetaData);
        //                        record.SetGuid(0, guid);
        //                        record.SetGuid(1, input.EntityId);
        //                        record.SetString(2, edge.EdgeType);
        //                        record.SetString(3, code.Key);
        //                        return record;
        //                    });
        //                },
        //                parameterFunc: input =>
        //                {
        //                    return new SqlParameter("EntityId", SqlDbType.UniqueIdentifier) { Value = input.EntityId };
        //                });

        //            return syncTableDefinition;
        //        default:
        //            throw new ArgumentOutOfRangeException(nameof(streamMode), streamMode, null);
        //    }
        //}

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

        //public static TableDefinition GetEdgePropertiesTableDefinitions(IReadOnlyCreateContainerModelV2 model, EdgeDirection direction)
        //{
        //    var columnDefinitions = new ColumnDefinition[]
        //    {
        //        new ("EdgeId", SqlColumnHelper.UniqueIdentifier, SqlDbType.UniqueIdentifier, IsPrimaryKey: true),
        //        new ("KeyName", SqlColumnHelper.NVarchar256, SqlDbType.NVarChar, IsPrimaryKey: true),
        //        new ("Value", SqlColumnHelper.NVarcharMax, SqlDbType.NVarChar)
        //    };

        //    if (model.StreamMode == StreamMode.EventStream)
        //    {
        //        var eventStreamColumnDefinitions = new ColumnDefinition[]
        //        {
        //            new("ChangeType", SqlColumnHelper.NVarchar256, SqlDbType.Char),
        //            new("CorrelationId", SqlColumnHelper.UniqueIdentifier, SqlDbType.UniqueIdentifier)
        //        };

        //        columnDefinitions.AddRange(eventStreamColumnDefinitions);
        //    }

        //    switch (model.StreamMode)
        //    {
        //        case StreamMode.EventStream:
        //            var eventModeColumnDefinitions = new ColumnDefinition[]
        //            {
        //                new("EdgeId", SqlColumnHelper.UniqueIdentifier, SqlDbType.UniqueIdentifier, IsPrimaryKey: true),
        //                new("KeyName", SqlColumnHelper.NVarchar256, SqlDbType.NVarChar, IsPrimaryKey: true),
        //                new("Value", SqlColumnHelper.NVarcharMax, SqlDbType.NVarChar),
        //                new("ChangeType", SqlColumnHelper.NVarchar256, SqlDbType.Char),
        //                new("CorrelationId", SqlColumnHelper.UniqueIdentifier, SqlDbType.UniqueIdentifier)
        //            };

        //            var eventModeTableDefinition = new TableDefinition(
        //                eventModeColumnDefinitions,
        //                recordFunc: input =>
        //                {
        //                    var edges = direction == EdgeDirection.Outgoing
        //                        ? input.OutgoingEdges
        //                        : input.IncomingEdges;

        //                    return edges.SelectMany(edge =>
        //                    {
        //                        var edgeId = GetEdgeId(input.EntityId, edge, direction);

        //                        return edge.Properties.Select(property =>
        //                        {
        //                            var sqlMetaData = eventModeColumnDefinitions.Select(column => column.ToSqlMetaData()).ToArray();

        //                            var record = new SqlDataRecord(sqlMetaData);
        //                            record.SetGuid(0, edgeId);
        //                            record.SetString(1, property.Key);
        //                            record.SetString(2, property.Value);
        //                            record.SetString(3, input.ChangeType.ToString());
        //                            record.SetGuid(4, input.CorrelationId.Value);
        //                        });
        //                    });
        //                },
        //                parameterFunc: input =>
        //                {
        //                    var edgeId = GetEdgeId(input.EntityId)
        //                    yield return new SqlParameter("@EdgeId", SqlDbType.UniqueIdentifier) { Value = input.}
        //                });
        //            break;
        //        case StreamMode.Sync:
        //            break;
        //        default:
        //            throw new ArgumentOutOfRangeException();
        //    }
        //}
    }
}
