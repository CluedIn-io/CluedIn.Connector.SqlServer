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
    public class EdgeTableDefinitionTests
    {
        [Theory, AutoNData]
        public void GetColumnDefinitions_ShouldIncludeChangeTypeAndCorrelationId_ForEventMode()
        {
            // arrange
            // act
            var eventColumnDefinitionsOutgoing = EdgeTableDefinition.GetColumnDefinitions(StreamMode.EventStream, EdgeDirection.Outgoing);
            var eventColumnDefinitionsIncoming = EdgeTableDefinition.GetColumnDefinitions(StreamMode.EventStream, EdgeDirection.Incoming);

            // assert
            eventColumnDefinitionsOutgoing.Should().Contain(column => column.Name == "ChangeType");
            eventColumnDefinitionsOutgoing.Should().Contain(column => column.Name == "CorrelationId");

            eventColumnDefinitionsIncoming.Should().Contain(column => column.Name == "ChangeType");
            eventColumnDefinitionsIncoming.Should().Contain(column => column.Name == "CorrelationId");
        }

        [Theory, AutoNData]
        public void GetColumnDefinitions_ShouldIncludeToCode_ForOutgoing()
        {
            // arrange
            // act
            var columnDefinitionsOutgoing = EdgeTableDefinition.GetColumnDefinitions(StreamMode.Sync, EdgeDirection.Outgoing);

            // assert
            columnDefinitionsOutgoing.Should().Contain(column => column.Name == "ToCode");
            columnDefinitionsOutgoing.Should().NotContain(column => column.Name == "FromCode");
        }

        [Theory, AutoNData]
        public void GetColumnDefinitions_ShouldIncludeFromCode_ForIncoming()
        {
            // arrange
            // act
            var columnDefinitionsOutgoing = EdgeTableDefinition.GetColumnDefinitions(StreamMode.Sync, EdgeDirection.Incoming);

            // assert
            columnDefinitionsOutgoing.Should().Contain(column => column.Name == "FromCode");
            columnDefinitionsOutgoing.Should().NotContain(column => column.Name == "ToCode");
        }

        [Theory, AutoNData]
        public void GetEdgeId_ShouldBeStable(Guid entityId, EntityReference fromReference, EntityReference toReference)
        {
            // arrange
            var edge1 = new EntityEdge(fromReference, toReference, EntityEdgeType.For, EntityEdgeCreationOptions.Default);
            edge1.Properties.Add("Key", "Value");

            var edge2 = new EntityEdge(fromReference, toReference, EntityEdgeType.For, EntityEdgeCreationOptions.Default);
            edge2.Properties.Add("Key", "Value");

            // act
            var edge1Id = EdgeTableDefinition.GetEdgeId(entityId, edge1, EdgeDirection.Outgoing);
            var edge2Id = EdgeTableDefinition.GetEdgeId(entityId, edge1, EdgeDirection.Outgoing);

            // assert
            edge1Id.Should().Be(edge2Id);
        }

        [Theory, AutoNData]
        public void GetEdgeId_ShouldBeInfluencedByPropertyValues(Guid entityId, EntityReference fromReference, EntityReference toReference)
        {
            // arrange
            var edge1 = new EntityEdge(fromReference, toReference, EntityEdgeType.For, EntityEdgeCreationOptions.Default);
            edge1.Properties.Add("Key", "Value");

            var edge2 = new EntityEdge(fromReference, toReference, EntityEdgeType.For, EntityEdgeCreationOptions.Default);
            edge2.Properties.Add("Key", "Value1");

            // act
            var edge1Id = EdgeTableDefinition.GetEdgeId(entityId, edge1, EdgeDirection.Outgoing);
            var edge2Id = EdgeTableDefinition.GetEdgeId(entityId, edge2, EdgeDirection.Outgoing);

            // assert
            edge1Id.Should().NotBe(edge2Id);
        }

        [Theory, AutoNData]
        public void GetSqlRecord_ShouldYieldCorrectRecords_ForEventMode(Guid entityId, EntityCode originEntityCode, EntityReference[] outgoingEdgeReferences, Guid correlationId, DateTimeOffset timestamp)
        {
            // arrange
            var entityType = EntityType.Person;
            var fromReference = new EntityReference(entityType, "fromReferenceName");
            var outgoingEdges = outgoingEdgeReferences
                .Select(toReference => new EntityEdge(fromReference, toReference, EntityEdgeType.For))
                .ToArray();

            var connectorEntityData = new ConnectorEntityData(
                VersionChangeType.Added,
                StreamMode.EventStream,
                entityId,
                persistInfo: null,
                previousPersistInfo: null,
                originEntityCode,
                entityType,
                properties: Array.Empty<ConnectorPropertyData>(),
                entityCodes: Array.Empty<IEntityCode>(),
                incomingEdges: Array.Empty<EntityEdge>(),
                outgoingEdges: outgoingEdges);

            var sqlConnectorEntityData = new SqlConnectorEntityData(connectorEntityData, correlationId, timestamp);

            // act
            var sqlRecords = EdgeTableDefinition.GetSqlRecords(StreamMode.EventStream, EdgeDirection.Outgoing, sqlConnectorEntityData).ToArray();

            // assert
            sqlRecords.Should().NotBeEmpty();
            sqlRecords.Should().HaveCount(outgoingEdges.Length);

            for (var i = 0; i < outgoingEdges.Length; i++)
            {
                var sqlRecord = sqlRecords[i];
                var edge = outgoingEdges[i];
                var edgeId = EdgeTableDefinition.GetEdgeId(entityId, edge, EdgeDirection.Outgoing);

                sqlRecord[0].Should().Be(edgeId);
                sqlRecord[1].Should().Be(entityId);
                sqlRecord[2].Should().Be(edge.EdgeType.ToString());
                sqlRecord[3].Should().Be(edge.ToReference.Code.Key);
                sqlRecord[4].Should().Be(VersionChangeType.Added);
                sqlRecord[5].Should().Be(correlationId);
            }
        }

        [Theory, AutoNData]
        public void GetSqlRecord_ShouldYieldCorrectRecords_ForSyncMode(Guid entityId, EntityCode originEntityCode, EntityReference[] outgoingEdgeReferences, Guid correlationId, DateTimeOffset timestamp)
        {
            // arrange
            var entityType = EntityType.Person;
            var fromReference = new EntityReference(entityType, "fromReferenceName");
            var outgoingEdges = outgoingEdgeReferences
                .Select(toReference => new EntityEdge(fromReference, toReference, EntityEdgeType.For))
                .ToArray();

            var connectorEntityData = new ConnectorEntityData(
                VersionChangeType.Added,
                StreamMode.Sync,
                entityId,
                persistInfo: null,
                previousPersistInfo: null,
                originEntityCode,
                entityType,
                properties: Array.Empty<ConnectorPropertyData>(),
                entityCodes: Array.Empty<IEntityCode>(),
                incomingEdges: Array.Empty<EntityEdge>(),
                outgoingEdges: outgoingEdges);

            var sqlConnectorEntityData = new SqlConnectorEntityData(connectorEntityData, correlationId, timestamp);

            // act
            var sqlRecords = EdgeTableDefinition.GetSqlRecords(StreamMode.Sync, EdgeDirection.Outgoing, sqlConnectorEntityData).ToArray();

            // assert
            sqlRecords.Should().NotBeEmpty();
            sqlRecords.Should().HaveCount(outgoingEdges.Length);

            for (var i = 0; i < outgoingEdges.Length; i++)
            {
                var sqlRecord = sqlRecords[i];
                var edge = outgoingEdges[i];
                var edgeId = EdgeTableDefinition.GetEdgeId(entityId, edge, EdgeDirection.Outgoing);

                sqlRecord[0].Should().Be(edgeId);
                sqlRecord[1].Should().Be(entityId);
                sqlRecord[2].Should().Be(edge.EdgeType.ToString());
                sqlRecord[3].Should().Be(edge.ToReference.Code.Key);
            }
        }
    }
}
