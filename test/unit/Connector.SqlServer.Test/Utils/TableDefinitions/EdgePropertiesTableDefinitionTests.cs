using CluedIn.Connector.SqlServer.Unit.Tests.Customizations;
using CluedIn.Connector.SqlServer.Utils.TableDefinitions;
using CluedIn.Connector.SqlServer.Utils;
using CluedIn.Core.Streams.Models;
using FluentAssertions;
using Xunit;
using CluedIn.Core.Connectors;
using CluedIn.Core.Data.Parts;
using CluedIn.Core.Data;
using System.Linq;
using System;

namespace CluedIn.Connector.SqlServer.Unit.Tests.Utils.TableDefinitions
{
    public class EdgePropertiesTableDefinitionTests
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
        public void GetSqlRecord_ShouldYieldCorrectRecords_ForEventMode(Guid entityId, EntityCode originEntityCode, EntityReference[] outgoingEdgeReferences, Guid correlationId, DateTimeOffset timestamp)
        {
            // arrange
            var entityType = EntityType.Person;
            var fromReference = new EntityReference(entityType, "fromReferenceName");
            var outgoingEdges = outgoingEdgeReferences
                .Select((toReference, i) =>
                {
                    var edge = new EntityEdge(fromReference, toReference, EntityEdgeType.For);
                    edge.Properties.Add(i.ToString(), i.ToString());
                    return edge;
                })
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
            var sqlRecords = EdgePropertiesTableDefinition.GetSqlDataRecords(StreamMode.EventStream, sqlConnectorEntityData, EdgeDirection.Outgoing).ToArray();

            // assert
            sqlRecords.Should().NotBeEmpty();
            sqlRecords.Should().HaveCount(outgoingEdges.SelectMany(edge => edge.Properties).Count());

            for (var i = 0; i < outgoingEdges.Length; i++)
            {
                var sqlRecord = sqlRecords[i];
                var edge = outgoingEdges[i];
                var edgeId = EdgeTableDefinition.GetEdgeId(entityId, edge, EdgeDirection.Outgoing);

                foreach (var edgeProperty in edge.Properties)
                {
                    sqlRecord[0].Should().Be(edgeId);
                    sqlRecord[1].Should().Be(edgeProperty.Key);
                    sqlRecord[2].Should().Be(edgeProperty.Value);
                    sqlRecord[3].Should().Be(VersionChangeType.Added);
                    sqlRecord[4].Should().Be(correlationId);
                }
            }
        }

        [Theory, AutoNData]
        public void GetSqlRecord_ShouldYieldCorrectRecords_ForSyncMode(Guid entityId, EntityCode originEntityCode, EntityReference[] outgoingEdgeReferences, Guid correlationId, DateTimeOffset timestamp)
        {
            // arrange
            var entityType = EntityType.Person;
            var fromReference = new EntityReference(entityType, "fromReferenceName");
            var outgoingEdges = outgoingEdgeReferences
                .Select((toReference, i) =>
                {
                    var edge = new EntityEdge(fromReference, toReference, EntityEdgeType.For);
                    edge.Properties.Add(i.ToString(), i.ToString());
                    return edge;
                })
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
            var sqlRecords = EdgePropertiesTableDefinition.GetSqlDataRecords(StreamMode.Sync, sqlConnectorEntityData, EdgeDirection.Outgoing).ToArray();

            // assert
            sqlRecords.Should().NotBeEmpty();
            sqlRecords.Should().HaveCount(outgoingEdges.SelectMany(edge => edge.Properties).Count());

            for (var i = 0; i < outgoingEdges.Length; i++)
            {
                var sqlRecord = sqlRecords[i];
                var edge = outgoingEdges[i];
                var edgeId = EdgeTableDefinition.GetEdgeId(entityId, edge, EdgeDirection.Outgoing);

                foreach (var edgeProperty in edge.Properties)
                {
                    sqlRecord[0].Should().Be(edgeId);
                    sqlRecord[1].Should().Be(edgeProperty.Key);
                    sqlRecord[2].Should().Be(edgeProperty.Value);
                }
            }
        }
    }
}
