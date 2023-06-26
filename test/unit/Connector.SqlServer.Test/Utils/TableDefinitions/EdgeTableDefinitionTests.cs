using CluedIn.Connector.SqlServer.Unit.Tests.Customizations;
using CluedIn.Connector.SqlServer.Utils;
using CluedIn.Connector.SqlServer.Utils.TableDefinitions;
using CluedIn.Core.Data;
using CluedIn.Core.Streams.Models;
using FluentAssertions;
using System;
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
    }
}
