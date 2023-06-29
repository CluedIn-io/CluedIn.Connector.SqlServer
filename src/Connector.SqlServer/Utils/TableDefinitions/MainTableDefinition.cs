using CluedIn.Connector.SqlServer.Connector;
using CluedIn.Core.Connectors;
using CluedIn.Core.Streams.Models;
using Microsoft.Data.SqlClient;
using Nest;
using System;
using System.Collections.Generic;
using System.Linq;

namespace CluedIn.Connector.SqlServer.Utils.TableDefinitions
{
    internal static class MainTableDefinition
    {
        public static MainTableColumnDefinition[] GetColumnDefinitions(IReadOnlyCreateContainerModelV2 model)
        {
            return GetColumnDefinitions(model.StreamMode, model.Properties.Select(property => (property.Name, property.DataType)).ToList());
        }

        public static MainTableColumnDefinition[] GetColumnDefinitions(IReadOnlyConnectorEntityData model)
        {
            return GetColumnDefinitions(model.StreamMode, model.Properties.Select(property => (property.Name, property.DataType)).ToList());
        }

        public static MainTableColumnDefinition[] GetColumnDefinitions(StreamMode streamMode, IReadOnlyCollection<(string name, ConnectorPropertyDataType dataType)> properties)
        {
            var defaultColumns = streamMode switch
            {
                StreamMode.EventStream => new MainTableColumnDefinition[]
                {
                    new("Id", SqlColumnHelper.UniqueIdentifier, input => input.EntityId, IsPrimaryKey: true),
                    new("PersistVersion", SqlColumnHelper.Int, input => input.PersistInfo != null ? (object)input.PersistInfo.PersistVersion : (object)DBNull.Value, CanBeNull: true),
                    new("PersistHash", SqlColumnHelper.Char24, input => input.PersistInfo != null ? (object)input.PersistInfo.PersistHash : (object)DBNull.Value, CanBeNull: true),
                    new("OriginEntityCode", SqlColumnHelper.NVarchar1024, input => input.OriginEntityCode != null ? (object)input.OriginEntityCode.ToString() : (object)DBNull.Value, CanBeNull: true),
                    new("EntityType", SqlColumnHelper.NVarcharMax, input => input.EntityType != null ? (object)input.EntityType.ToString() : (object)DBNull.Value, CanBeNull: true),
                    new("Timestamp", SqlColumnHelper.DateTimeOffset7, input => input.Timestamp),
                    new("ChangeType", SqlColumnHelper.Int, input => input.ChangeType),
                    new("CorrelationId", SqlColumnHelper.UniqueIdentifier, input => input.CorrelationId, IsPrimaryKey: true)
                },
                StreamMode.Sync => new MainTableColumnDefinition[]
                {
                    new("Id", SqlColumnHelper.UniqueIdentifier, input => input.EntityId, IsPrimaryKey: true),
                    new("PersistVersion", SqlColumnHelper.Int, input => input.PersistInfo!.PersistVersion),
                    new("PersistHash", SqlColumnHelper.Char24, input => input.PersistInfo!.PersistHash),
                    new("OriginEntityCode", SqlColumnHelper.NVarchar1024, input => input.OriginEntityCode.ToString()),
                    new("EntityType", SqlColumnHelper.NVarcharMax, input => input.EntityType.ToString()),
                    new("Timestamp", SqlColumnHelper.DateTimeOffset7, input => input.Timestamp),
                },
                _ => throw new ArgumentOutOfRangeException(nameof(streamMode), streamMode, null)
            };

            var propertyColumns = properties.Select(property =>
            {
                var name = property.name.ToSanitizedSqlName();
                var sqlType = SqlColumnHelper.GetColumnType(property.dataType);
                return new MainTableColumnDefinition(
                    name,
                    sqlType,
                    input =>
                    {
                        var propertyValue = input.Properties.First(x => x.Name == property.name).Value;
                        if (propertyValue == null)
                        {
                            return DBNull.Value;
                        }

                        if (propertyValue is IEnumerable<object> enumerable)
                        {
                            return $"[{string.Join(", ", enumerable)}]";
                        }

                        return propertyValue;
                    },
                    CanBeNull: true);
            });

            var allColumns = defaultColumns.Concat(propertyColumns).ToArray();

            return allColumns;
        }

        public static SqlServerConnectorCommand CreateUpsertCommand(IReadOnlyStreamModel streamModel, SqlConnectorEntityData connectorEntityData, SqlName schema)
        {
            var mainTableName = TableNameUtility.GetMainTableName(streamModel, schema);
            var mainTableDefinitions = GetColumnDefinitions(connectorEntityData);

            var columnNames = string.Join(", ", mainTableDefinitions.Select(definition => definition.Name.ToSanitizedSqlName()));
            var valueParameterNames = string.Join(", ", mainTableDefinitions.Select(definition => $"@{definition.Name.ToSanitizedSqlName()}"));

            var valueParameters = mainTableDefinitions.Select(definition =>
                new SqlParameter($"@{definition.Name}", definition.ConnectorSqlType.SqlType)
                {
                    Value = definition.GetValueFunc(connectorEntityData)
                })
                .ToArray();

            switch (streamModel.Mode)
            {
                case StreamMode.EventStream:
                    var insertText = $"""
                        INSERT INTO {mainTableName.FullyQualifiedName}({columnNames})
                        VALUES({valueParameterNames})
                        """;

                    return new SqlServerConnectorCommand { Text = insertText, Parameters = valueParameters };

                case StreamMode.Sync:
                    var valueAssignmentStrings = mainTableDefinitions.Select(valueDefinition =>
                    {
                        var sanitizedKeyName = valueDefinition.Name.ToSanitizedSqlName();
                        return $"{sanitizedKeyName} = @{sanitizedKeyName}";
                    });
                    var valueAssignmentsString = string.Join(", ", valueAssignmentStrings);

                    var upsertText = $"""
                        IF EXISTS (SELECT * FROM {mainTableName.FullyQualifiedName} WHERE [Id] = @Id)
                        	UPDATE {mainTableName.FullyQualifiedName} SET {valueAssignmentsString} WHERE [Id] = @Id;
                        ELSE
                        	INSERT INTO {mainTableName.FullyQualifiedName}({columnNames}) VALUES({valueParameterNames})
                        """;

                    return new SqlServerConnectorCommand { Text = upsertText, Parameters = valueParameters };

                default:
                    throw new ArgumentOutOfRangeException();
            }
        }
    }
}
