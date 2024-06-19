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
                ("DiscoveryDate", new EntityPropertyConnectorPropertyDataType(typeof(DateTime))),
            };
            var dateValue = new DateTimeOffset(2000, 1, 1, 1, 1, 1, 0, offset: TimeSpan.FromHours(1));

            var discoveryDatePropertyDate = new ConnectorPropertyData("DiscoveryDate", dateValue, new EntityPropertyConnectorPropertyDataType(typeof(DateTime)));
            var connectorEntityData = new ConnectorEntityData(versionChangeType, StreamMode.Sync, entityId, null, null, null, null, new[] { discoveryDatePropertyDate }, Array.Empty<IEntityCode>(), Array.Empty<EntityEdge>(), Array.Empty<EntityEdge>());
            var sqlDiscoveryDatePropertyDate = new SqlConnectorEntityData(connectorEntityData, correlationId, timestamp: DateTimeOffset.Now);

            // act
            var syncColumnDefinitions = MainTableDefinition.GetColumnDefinitions(StreamMode.Sync, properties);

            // assert
            var discoveryDateColumnDefinition = syncColumnDefinitions.Where(column => column.Name == "DiscoveryDate").Should().ContainSingle().And.Subject.First();
            var sqlDateValue = discoveryDateColumnDefinition.GetValueFunc(sqlDiscoveryDatePropertyDate);
            sqlDateValue.Should().Be("2000-01-01T01:01:01.0000000+01:00");
        }
    }
}
