using CluedIn.Connector.SqlServer.Unit.Tests.Customizations;
using CluedIn.Connector.SqlServer.Utils;
using CluedIn.Connector.SqlServer.Utils.TableDefinitions;
using CluedIn.Core.Connectors;
using CluedIn.Core.Data;
using CluedIn.Core.Data.Parts;
using CluedIn.Core.Data.Vocabularies;
using CluedIn.Core.Streams.Models;
using FluentAssertions;
using System;
using System.Data;
using System.Linq;
using Xunit;

namespace CluedIn.Connector.SqlServer.Unit.Tests.Utils.TableDefinitions
{
    public class MainTableDefinitionTests
    {
        [Theory, AutoNData]
        public void GetColumnDefinitions_ShouldIncludeProperties()
        {
            // arrange
            var properties = new (string, ConnectorPropertyDataType)[]
            {
                ("Name", new EntityPropertyConnectorPropertyDataType(typeof(string))),
                ("JobTitle", new VocabularyKeyDataTypeConnectorPropertyDataType(VocabularyKeyDataType.Text))
            };

            // act
            var syncColumnDefinitions = MainTableDefinition.GetColumnDefinitions(StreamMode.Sync, properties);
            var eventColumnDefinitions = MainTableDefinition.GetColumnDefinitions(StreamMode.EventStream, properties);

            // assert
            syncColumnDefinitions.Should().Contain(column => column.Name == "Name" && column.ConnectorSqlType.SqlType == SqlDbType.NVarChar);
            syncColumnDefinitions.Should().Contain(column => column.Name == "JobTitle" && column.ConnectorSqlType.SqlType == SqlDbType.NVarChar);

            eventColumnDefinitions.Should().Contain(column => column.Name == "Name" && column.ConnectorSqlType.SqlType == SqlDbType.NVarChar);
            eventColumnDefinitions.Should().Contain(column => column.Name == "JobTitle" && column.ConnectorSqlType.SqlType == SqlDbType.NVarChar);
        }

        [Theory, AutoNData]
        public void GetColumnDefinitions_ShouldIncludeChangeTypeAndCorrelationId_ForEventMode()
        {
            // arrange
            var properties = Array.Empty<(string name, ConnectorPropertyDataType dataType)>();

            // act
            var eventColumnDefinitions = MainTableDefinition.GetColumnDefinitions(StreamMode.EventStream, properties);

            // assert
            eventColumnDefinitions.Should().Contain(column => column.Name == "ChangeType");
            eventColumnDefinitions.Should().Contain(column => column.Name == "CorrelationId");
        }

        [Theory, AutoNData]
        public void GetColumnDefinitions_ShouldNotIncludePropertiesMultipleTimes()
        {
            // arrange
            var properties = new (string, ConnectorPropertyDataType)[]
            {
                ("PersistVersion", new EntityPropertyConnectorPropertyDataType(typeof(string))),
            };

            // act
            var syncColumnDefinitions = MainTableDefinition.GetColumnDefinitions(StreamMode.Sync, properties);
            var eventColumnDefinitions = MainTableDefinition.GetColumnDefinitions(StreamMode.EventStream, properties);

            // assert
            syncColumnDefinitions.Should().ContainSingle(column => column.Name == "PersistVersion");
            eventColumnDefinitions.Should().ContainSingle(column => column.Name == "PersistVersion");
        }

        [Theory, AutoNData]
        public void DateTimePropertyValues_ShouldBeToISO8601(
            VersionChangeType versionChangeType,
            Guid entityId,
            Guid correlationId)
        {
            // arrange
            var properties = new (string, ConnectorPropertyDataType)[]
            {
                ("DiscoveryDate", new EntityPropertyConnectorPropertyDataType(typeof(DateTime))),
            };
            var dateValue = new DateTime(2000, 1, 1, 1, 1, 1);

            var discoveryDatePropertyDate = new ConnectorPropertyData("DiscoveryDate", dateValue, new EntityPropertyConnectorPropertyDataType(typeof(DateTime)));
            var connectorEntityData = new ConnectorEntityData(versionChangeType, StreamMode.Sync, entityId, null, null, null, null, new[] { discoveryDatePropertyDate }, Array.Empty<IEntityCode>(), Array.Empty<EntityEdge>(), Array.Empty<EntityEdge>());
            var sqlDiscoveryDatePropertyDate = new SqlConnectorEntityData(connectorEntityData, correlationId, timestamp: DateTimeOffset.Now);

            // act
            var syncColumnDefinitions = MainTableDefinition.GetColumnDefinitions(StreamMode.Sync, properties);

            // assert
            var discoveryDateColumnDefinition = syncColumnDefinitions.Where(column => column.Name == "DiscoveryDate").Should().ContainSingle().And.Subject.First();
            var sqlDateValue = discoveryDateColumnDefinition.GetValueFunc(sqlDiscoveryDatePropertyDate);
            sqlDateValue.Should().Be("2000-01-01T01:01:01.0000000");
        }

        [Theory, AutoNData]
        public void DateTimeOffsetPropertyValues_ShouldBeToISO8601(
            VersionChangeType versionChangeType,
            Guid entityId,
            Guid correlationId)
        {
            // arrange
            var properties = new (string, ConnectorPropertyDataType)[]
            {
                ("DiscoveryDate", new EntityPropertyConnectorPropertyDataType(typeof(DateTimeOffset))),
            };
            var dateValue = new DateTimeOffset(2000, 1, 1, 1, 1, 1, 0, offset: TimeSpan.FromHours(1));

            var discoveryDatePropertyDate = new ConnectorPropertyData("DiscoveryDate", dateValue, new EntityPropertyConnectorPropertyDataType(typeof(DateTimeOffset)));
            var connectorEntityData = new ConnectorEntityData(versionChangeType, StreamMode.Sync, entityId, null, null, null, null, new[] { discoveryDatePropertyDate }, Array.Empty<IEntityCode>(), Array.Empty<EntityEdge>(), Array.Empty<EntityEdge>());
            var sqlDiscoveryDatePropertyDate = new SqlConnectorEntityData(connectorEntityData, correlationId, timestamp: DateTimeOffset.Now);

            // act
            var syncColumnDefinitions = MainTableDefinition.GetColumnDefinitions(StreamMode.Sync, properties);

            // assert
            var discoveryDateColumnDefinition = syncColumnDefinitions.Where(column => column.Name == "DiscoveryDate").Should().ContainSingle().And.Subject.First();
            var sqlDateValue = discoveryDateColumnDefinition.GetValueFunc(sqlDiscoveryDatePropertyDate);
            sqlDateValue.Should().Be("2000-01-01T01:01:01.0000000+01:00");
        }

        [Theory, AutoNData]
        public void VocabularyPropertiesBeingSanitizedToTheSameName_ShouldHaveNumbersAddedAtTheEnd(
            IVocabulary vocabulary1,
            IVocabulary vocabulary2)
        {
            // arrange
            vocabulary1.KeyPrefix = "test--vocabulary";
            vocabulary2.KeyPrefix = "test-.vocabulary";
            var vocabularyKey1 = new VocabularyKey("name") { Vocabulary = vocabulary1 };
            var vocabularyKey2 = new VocabularyKey("name") { Vocabulary = vocabulary2 };

            var properties = new (string, ConnectorPropertyDataType)[]
            {
                ("testvocabularyname", new VocabularyKeyConnectorPropertyDataType(vocabularyKey1)),
                ("testvocabularyname", new VocabularyKeyConnectorPropertyDataType(vocabularyKey2)),
            };

            // act
            var syncColumnDefinitions = MainTableDefinition.GetColumnDefinitions(StreamMode.Sync, properties);

            // assert
            var columnDefinitionNames = syncColumnDefinitions.Select(x => x.Name).ToList();

            columnDefinitionNames.Should().Contain("testvocabularyname");
            columnDefinitionNames.Should().Contain("testvocabularyname_1");
        }

        [Theory, AutoNData]
        public void DifferentOrderVocabularyProperties_ShouldNotImpactOrderOfColumnDefinition(
            IVocabulary vocabulary1,
            IVocabulary vocabulary2)
        {
            // arrange
            vocabulary1.KeyPrefix = "test--vocabulary";
            vocabulary2.KeyPrefix = "test-.vocabulary";
            var vocabularyKey1 = new VocabularyKey("name") { Vocabulary = vocabulary1 };
            var vocabularyKey2 = new VocabularyKey("name") { Vocabulary = vocabulary2 };

            var properties1 = new (string, ConnectorPropertyDataType)[]
            {
                ("testvocabularyname", new VocabularyKeyConnectorPropertyDataType(vocabularyKey1)),
                ("testvocabularyname", new VocabularyKeyConnectorPropertyDataType(vocabularyKey2)),
            };

            var properties2 = new (string, ConnectorPropertyDataType)[]
            {
                ("testvocabularyname", new VocabularyKeyConnectorPropertyDataType(vocabularyKey2)),
                ("testvocabularyname", new VocabularyKeyConnectorPropertyDataType(vocabularyKey1)),
            };

            // act
            var syncColumnDefinitions1 = MainTableDefinition.GetColumnDefinitions(StreamMode.Sync, properties1);
            var syncColumnDefinitions2 = MainTableDefinition.GetColumnDefinitions(StreamMode.Sync, properties2);

            // assert
            var syncColumnDefinitions1Names = syncColumnDefinitions1.Select(x => (x.Name));
            var syncColumnDefinitions2Names = syncColumnDefinitions2.Select(x => (x.Name));

            syncColumnDefinitions1Names.Should().BeEquivalentTo(syncColumnDefinitions2Names);
        }

        [Theory, AutoNData]
        public void EntityTypePropertyValues_ShouldBeMadeIntoStrings(
            VersionChangeType versionChangeType,
            Guid entityId,
            Guid correlationId)
        {
            // arrange
            var properties = new (string, ConnectorPropertyDataType)[]
            {
                ("Type", new EntityPropertyConnectorPropertyDataType(typeof(EntityType))),
            };
            var entityTypeValue = EntityType.Person;

            var entityTypePropertyDate = new ConnectorPropertyData("Type", entityTypeValue, new EntityPropertyConnectorPropertyDataType(typeof(EntityType)));
            var connectorEntityData = new ConnectorEntityData(versionChangeType, StreamMode.Sync, entityId, null, null, null, null, new[] { entityTypePropertyDate }, Array.Empty<IEntityCode>(), Array.Empty<EntityEdge>(), Array.Empty<EntityEdge>());
            var sqlEntityTypePropertyDate = new SqlConnectorEntityData(connectorEntityData, correlationId, timestamp: DateTimeOffset.Now);

            // act
            var syncColumnDefinitions = MainTableDefinition.GetColumnDefinitions(StreamMode.Sync, properties);

            // assert
            var entityTypeColumnDefinition = syncColumnDefinitions.Where(column => column.Name == "Type").Should().ContainSingle().And.Subject.First();
            var sqlDateValue = entityTypeColumnDefinition.GetValueFunc(sqlEntityTypePropertyDate);
            sqlDateValue.Should().Be("/Person");
        }

        [Theory, AutoNData]
        public void PersonReferencePropertyValues_ShouldBeMadeIntoStrings(
            VersionChangeType versionChangeType,
            Guid entityId,
            Guid correlationId)
        {
            // arrange
            var properties = new (string, ConnectorPropertyDataType)[]
            {
                ("LastChangedBy", new EntityPropertyConnectorPropertyDataType(typeof(PersonReference))),
            };
            var personReferenceValue = new PersonReference("PersonName");

            var personReferencePropertyData = new ConnectorPropertyData("LastChangedBy", personReferenceValue, new EntityPropertyConnectorPropertyDataType(typeof(EntityType)));
            var connectorEntityData = new ConnectorEntityData(versionChangeType, StreamMode.Sync, entityId, null, null, null, null, new[] { personReferencePropertyData }, Array.Empty<IEntityCode>(), Array.Empty<EntityEdge>(), Array.Empty<EntityEdge>());
            var sqlEntityTypePropertyDate = new SqlConnectorEntityData(connectorEntityData, correlationId, timestamp: DateTimeOffset.Now);

            // act
            var syncColumnDefinitions = MainTableDefinition.GetColumnDefinitions(StreamMode.Sync, properties);

            // assert
            var personReferenceColumnDefinition = syncColumnDefinitions.Where(column => column.Name == "LastChangedBy").Should().ContainSingle().And.Subject.First();
            var sqlDataValue = personReferenceColumnDefinition.GetValueFunc(sqlEntityTypePropertyDate);
            sqlDataValue.Should().Be("PersonName");
        }

        [Theory, AutoNData]
        public void EntityReferencePropertyValues_ShouldBeMadeIntoStrings(
            VersionChangeType versionChangeType,
            Guid entityId,
            Guid correlationId)
        {
            // arrange
            var properties = new (string, ConnectorPropertyDataType)[]
            {
                ("LastChangedBy", new EntityPropertyConnectorPropertyDataType(typeof(EntityReference))),
            };
            var entityCode = new EntityCode(EntityType.Person, CodeOrigin.CluedIn, "PersonName");
            var entityReferenceValue = new EntityReference(entityCode);

            var entityReferencePropertyData = new ConnectorPropertyData("LastChangedBy", entityReferenceValue, new EntityPropertyConnectorPropertyDataType(typeof(EntityType)));
            var connectorEntityData = new ConnectorEntityData(versionChangeType, StreamMode.Sync, entityId, null, null, null, null, new[] { entityReferencePropertyData }, Array.Empty<IEntityCode>(), Array.Empty<EntityEdge>(), Array.Empty<EntityEdge>());
            var sqlEntityTypePropertyDate = new SqlConnectorEntityData(connectorEntityData, correlationId, timestamp: DateTimeOffset.Now);

            // act
            var syncColumnDefinitions = MainTableDefinition.GetColumnDefinitions(StreamMode.Sync, properties);

            // assert
            var entityReferenceColumnDefinition = syncColumnDefinitions.Where(column => column.Name == "LastChangedBy").Should().ContainSingle().And.Subject.First();
            var sqlDataValue = entityReferenceColumnDefinition.GetValueFunc(sqlEntityTypePropertyDate);
            sqlDataValue.Should().Be("/Person#CluedIn:PersonName");
        }
    }
}
