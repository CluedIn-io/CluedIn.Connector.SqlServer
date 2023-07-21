using CluedIn.Connector.SqlServer.Unit.Tests.Customizations;
using CluedIn.Connector.SqlServer.Utils;
using CluedIn.Connector.SqlServer.Utils.TableDefinitions;
using CluedIn.Core.Connectors;
using CluedIn.Core.Data;
using CluedIn.Core.Data.Parts;
using CluedIn.Core.Streams.Models;
using FluentAssertions;
using System;
using System.Linq;
using Xunit;

namespace CluedIn.Connector.SqlServer.Unit.Tests.Utils.TableDefinitions
{
    public class CodeTableDefinitionTests
    {
        [Theory, AutoNData]
        public void GetColumnDefinitions_ShouldIncludeChangeTypeAndCorrelationId_ForEventMode()
        {
            // arrange
            // act
            var eventColumnDefinitions = CodeTableDefinition.GetColumnDefinitions(StreamMode.EventStream);

            // assert
            eventColumnDefinitions.Should().Contain(column => column.Name == "ChangeType");
            eventColumnDefinitions.Should().Contain(column => column.Name == "CorrelationId");
        }

        [Theory, AutoNData]
        public void GetSqlRecord_ShouldYieldCorrectRecords_ForEventMode(Guid entityId, EntityCode originEntityCode, EntityType entityType, EntityCode[] codes, Guid correlationId, DateTimeOffset timestamp)
        {
            // arrange
            var connectorEntityData = new ConnectorEntityData(
                VersionChangeType.Added,
                StreamMode.EventStream,
                entityId,
                persistInfo: null,
                previousPersistInfo: null,
                originEntityCode,
                entityType,
                properties: Array.Empty<ConnectorPropertyData>(),
                codes,
                incomingEdges: Array.Empty<EntityEdge>(),
                outgoingEdges: Array.Empty<EntityEdge>());

            var sqlConnectorEntityData = new SqlConnectorEntityData(connectorEntityData, correlationId, timestamp);

            // act
            var sqlRecords = CodeTableDefinition.GetSqlRecords(StreamMode.EventStream, sqlConnectorEntityData).ToArray();

            // assert
            sqlRecords.Should().NotBeEmpty();
            sqlRecords.Should().HaveCount(codes.Length);

            for (var i = 0; i < codes.Length; i++)
            {
                var sqlRecord = sqlRecords[i];
                var code = codes[i];

                sqlRecord[0].Should().Be(entityId);
                sqlRecord[1].Should().Be(code.Key);
                sqlRecord[2].Should().Be(VersionChangeType.Added);
                sqlRecord[3].Should().Be(correlationId);
            }
        }

        [Theory, AutoNData]
        public void GetSqlRecord_ShouldYieldCorrectRecords_ForSyncMode(Guid entityId, EntityCode originEntityCode, EntityType entityType, EntityCode[] codes, Guid correlationId, DateTimeOffset timestamp)
        {
            // arrange
            var connectorEntityData = new ConnectorEntityData(
                VersionChangeType.Added,
                StreamMode.Sync,
                entityId,
                persistInfo: null,
                previousPersistInfo: null,
                originEntityCode,
                entityType,
                properties: Array.Empty<ConnectorPropertyData>(),
                codes,
                incomingEdges: Array.Empty<EntityEdge>(),
                outgoingEdges: Array.Empty<EntityEdge>());

            var sqlConnectorEntityData = new SqlConnectorEntityData(connectorEntityData, correlationId, timestamp);

            // act
            var sqlRecords = CodeTableDefinition.GetSqlRecords(StreamMode.Sync, sqlConnectorEntityData).ToArray();

            // assert
            sqlRecords.Should().NotBeEmpty();
            sqlRecords.Should().HaveCount(codes.Length);

            for (var i = 0; i < codes.Length; i++)
            {
                var sqlRecord = sqlRecords[i];
                var code = codes[i];

                sqlRecord[0].Should().Be(entityId);
                sqlRecord[1].Should().Be(code.Key);
            }
        }
    }
}
