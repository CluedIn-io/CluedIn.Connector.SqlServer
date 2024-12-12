using CluedIn.Connector.SqlServer.Connector;
using CluedIn.Core.Connectors;
using CluedIn.Core.Data;
using CluedIn.Core.Streams.Models;
using Microsoft.Data.SqlClient;
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
                    new("EntityType", SqlColumnHelper.NVarchar1024, input => input.EntityType != null ? (object)input.EntityType.ToString() : (object)DBNull.Value, CanBeNull: true),
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
                    new("EntityType", SqlColumnHelper.NVarchar1024, input => input.EntityType.ToString()),
                    new("Timestamp", SqlColumnHelper.DateTimeOffset7, input => input.Timestamp),
                },
                _ => throw new ArgumentOutOfRangeException(nameof(streamMode), streamMode, null)
            };

            var defaultColumnNamesHashSet = defaultColumns.Select(x => x.Name).ToHashSet();

            var alreadyUsedNames = new HashSet<string>();

            var propertyColumns = properties
                // We need to filter out any properties, that are contained in the default columns.
                .Where(property => !defaultColumnNamesHashSet.Contains(property.name.ToSanitizedSqlName()))
                .OrderBy(property => property.dataType is VocabularyKeyConnectorPropertyDataType x
                    ? $"{x.VocabularyKey.Vocabulary.KeyPrefix}.{x.VocabularyKey.Name}"
                    : property.name)
                .Select(property =>
                {
                    var nameToUse = GetNameToUse(property, alreadyUsedNames);

                    var sqlType = SqlColumnHelper.GetColumnType(property.dataType);
                    return new MainTableColumnDefinition(
                        nameToUse,
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

                            if (propertyValue is DateTime dateTimeValue)
                            {
                                return dateTimeValue.ToString("O");
                            }

                            if (propertyValue is DateTimeOffset dateTimeOffsetValue)
                            {
                                return dateTimeOffsetValue.ToString("O");
                            }

                            if (propertyValue is EntityType entityTypeValue)
                            {
                                return entityTypeValue.ToString();
                            }

                            return propertyValue;
                        },
                        CanBeNull: true);
                });

            var allColumns = defaultColumns.Concat(propertyColumns).ToArray();

            return allColumns;
        }

        private static string GetNameToUse((string name, ConnectorPropertyDataType dataType) property, HashSet<string> alreadyUsedNames)
        {
            string rawName;
            switch (property.dataType)
            {
                case VocabularyKeyConnectorPropertyDataType vocabularyKeyConnectorPropertyDataType:
                    var vocabularyKey = vocabularyKeyConnectorPropertyDataType.VocabularyKey;
                    rawName = $"{vocabularyKey.Vocabulary.KeyPrefix}.{vocabularyKey.Name}";
                    break;
                default:
                    rawName = property.name;
                    break;
            }

            rawName = rawName.ToSanitizedSqlName();

            var number = 0;

            var nameToUse = rawName;
            while (alreadyUsedNames.Contains(nameToUse))
            {
                number++;
                nameToUse = $"{rawName}_{number}";
            }

            alreadyUsedNames.Add(nameToUse);

            // We need to call ToSanitizedSqlName again, in case adding numbers pushed length of name over the maximum
            return nameToUse.ToSanitizedSqlName();
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
