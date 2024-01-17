using AutoFixture.Xunit2;
using CluedIn.Connector.SqlServer.Unit.Tests.Customizations;
using CluedIn.Connector.SqlServer.Utils;
using CluedIn.Core.Connectors;
using CluedIn.Core.Streams.Models;
using FluentAssertions;
using Xunit;
using NSubstitute;

namespace CluedIn.Connector.SqlServer.Unit.Tests.Utils
{
    public class TableNameUtilityTests
    {
        [Theory]
        [InlineAutoData("TestTableName", "TestTableName", "TestTableNameCodes", "TestTableNameOutgoingEdges", "TestTableNameOutgoingEdgeProperties", "TestTableNameIncomingEdges", "TestTableNameIncomingEdgeProperties")]
        [InlineAutoData(
            "TestTableName127CharactersAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
            "TestTableName127CharactersAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA_3b2a4bbe",
            "TestTableName127CharactersAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA_3b2a4bbeCodes",
            "TestTableName127CharactersAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA_3b2a4bbeOutgoingEdges",
            "TestTableName127CharactersAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA_3b2a4bbeOutgoingEdgeProperties",
            "TestTableName127CharactersAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA_3b2a4bbeIncomingEdges",
            "TestTableName127CharactersAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA_3b2a4bbeIncomingEdgeProperties")]
        internal void TableNamesShouldBeExpectedNames(
            string containerName,
            string expectedMainTableName, string expectedCodeTableName, string expectedOutgoingEdgeTableName, string expectedOutgoingEdgePropertiesTableName, string expectedIncomingEdgeTableName, string expectedIncomingEdgePropertiesTableName)
        {
            // arrange
            var schema = new SqlName();
            var streamModel = Substitute.For<IReadOnlyStreamModel>();
            streamModel.ContainerName.Returns(containerName);

            // act
            var mainTableName                   = TableNameUtility.GetMainTableName(streamModel, schema);
            var codeTableName                   = TableNameUtility.GetCodeTableName(streamModel, schema);
            var outgoingEdgeTableName           = TableNameUtility.GetEdgesTableName(streamModel, EdgeDirection.Outgoing, schema);
            var outgoingEdgePropertiesTableName = TableNameUtility.GetEdgePropertiesTableName(streamModel, EdgeDirection.Outgoing, schema);
            var incomingEdgeTableName           = TableNameUtility.GetEdgesTableName(streamModel, EdgeDirection.Incoming, schema);
            var incomingEdgePropertiesTableName = TableNameUtility.GetEdgePropertiesTableName(streamModel, EdgeDirection.Incoming, schema);

            // assert
            mainTableName.LocalName.Value.Should().BeEquivalentTo(expectedMainTableName);
            codeTableName.LocalName.Value.Should().BeEquivalentTo(expectedCodeTableName);
            outgoingEdgeTableName.LocalName.Value.Should().BeEquivalentTo(expectedOutgoingEdgeTableName);
            outgoingEdgePropertiesTableName.LocalName.Value.Should().BeEquivalentTo(expectedOutgoingEdgePropertiesTableName);
            incomingEdgeTableName.LocalName.Value.Should().BeEquivalentTo(expectedIncomingEdgeTableName);
            incomingEdgePropertiesTableName.LocalName.Value.Should().BeEquivalentTo(expectedIncomingEdgePropertiesTableName);
        }

        [Theory, AutoNData]
        internal void AllOverloadsShouldGiveSameMainTableName(IReadOnlyStreamModel streamModel, IReadOnlyCreateContainerModelV2 createContainerModel)
        {
            // arrange
            var schema = new SqlName();
            var containerName = "test";
            streamModel.ContainerName.Returns(containerName);
            createContainerModel.Name.Returns(containerName);

            // act
            var stringOverloadName = TableNameUtility.GetMainTableName(containerName, schema);
            var streamModelOverloadName = TableNameUtility.GetMainTableName(streamModel, schema);
            var createContainerOverloadName = TableNameUtility.GetMainTableName(createContainerModel, schema);

            // assert
            stringOverloadName.Should().BeEquivalentTo(streamModelOverloadName);
            stringOverloadName.Should().BeEquivalentTo(createContainerOverloadName);

            streamModelOverloadName.Should().BeEquivalentTo(stringOverloadName);
            streamModelOverloadName.Should().BeEquivalentTo(createContainerOverloadName);

            createContainerOverloadName.Should().BeEquivalentTo(stringOverloadName);
            createContainerOverloadName.Should().BeEquivalentTo(streamModelOverloadName);
        }

        [Theory, AutoNData]
        internal void AllOverloadsShouldGiveSameCodeTableName(IReadOnlyStreamModel streamModel, IReadOnlyCreateContainerModelV2 createContainerModel)
        {
            // arrange
            var schema = new SqlName();
            var containerName = "Test";
            streamModel.ContainerName.Returns(containerName);
            createContainerModel.Name.Returns(containerName);

            var stringOverloadName = TableNameUtility.GetMainTableName(containerName, schema);

            // act
            var stringOverloadCodeTableName = TableNameUtility.GetCodeTableName(stringOverloadName, schema);
            var streamModelOverloadCodeTableName = TableNameUtility.GetCodeTableName(streamModel, schema);
            var createContainerOverloadCodeTableName = TableNameUtility.GetCodeTableName(createContainerModel, schema);

            // assert
            stringOverloadCodeTableName.Should().BeEquivalentTo(streamModelOverloadCodeTableName);
            stringOverloadCodeTableName.Should().BeEquivalentTo(createContainerOverloadCodeTableName);

            streamModelOverloadCodeTableName.Should().BeEquivalentTo(stringOverloadCodeTableName);
            streamModelOverloadCodeTableName.Should().BeEquivalentTo(createContainerOverloadCodeTableName);

            createContainerOverloadCodeTableName.Should().BeEquivalentTo(stringOverloadCodeTableName);
            createContainerOverloadCodeTableName.Should().BeEquivalentTo(streamModelOverloadCodeTableName);
        }

        [Theory, AutoNData]
        internal void AllOverloadsShouldGiveSameEdgeTableName(IReadOnlyStreamModel streamModel, IReadOnlyCreateContainerModelV2 createContainerModel)
        {
            // arrange
            var schema = new SqlName();
            var containerName = "Test";
            streamModel.ContainerName.Returns(containerName);
            createContainerModel.Name.Returns(containerName);

            var stringOverloadName = TableNameUtility.GetMainTableName(containerName, schema);

            // act
            var stringOverloadEdgeTableName = TableNameUtility.GetEdgesTableName(stringOverloadName, EdgeDirection.Incoming, schema);
            var streamModelOverloadEdgeTableName = TableNameUtility.GetEdgesTableName(streamModel, EdgeDirection.Incoming, schema);
            var createContainerOverloadEdgeTableName = TableNameUtility.GetEdgesTableName(createContainerModel, EdgeDirection.Incoming, schema);

            // assert
            stringOverloadEdgeTableName.Should().BeEquivalentTo(streamModelOverloadEdgeTableName);
            stringOverloadEdgeTableName.Should().BeEquivalentTo(createContainerOverloadEdgeTableName);

            streamModelOverloadEdgeTableName.Should().BeEquivalentTo(stringOverloadEdgeTableName);
            streamModelOverloadEdgeTableName.Should().BeEquivalentTo(createContainerOverloadEdgeTableName);

            createContainerOverloadEdgeTableName.Should().BeEquivalentTo(stringOverloadEdgeTableName);
            createContainerOverloadEdgeTableName.Should().BeEquivalentTo(streamModelOverloadEdgeTableName);
        }

        [Theory, AutoNData]
        internal void AllOverloadsShouldGiveSameEdgePropertiesTableName(IReadOnlyStreamModel streamModel, IReadOnlyCreateContainerModelV2 createContainerModel)
        {
            // arrange
            var schema = new SqlName();
            var containerName = "Test";
            streamModel.ContainerName.Returns(containerName);
            createContainerModel.Name.Returns(containerName);

            var stringOverloadName = TableNameUtility.GetMainTableName(containerName, schema);

            // act
            var stringOverloadEdgePropertiesTableName = TableNameUtility.GetEdgePropertiesTableName(stringOverloadName, EdgeDirection.Incoming, schema);
            var streamModelOverloadEdgePropertiesTableName = TableNameUtility.GetEdgePropertiesTableName(streamModel, EdgeDirection.Incoming, schema);
            var createContainerOverloadEdgePropertiesTableName = TableNameUtility.GetEdgePropertiesTableName(createContainerModel, EdgeDirection.Incoming, schema);

            // assert
            stringOverloadEdgePropertiesTableName.Should().BeEquivalentTo(streamModelOverloadEdgePropertiesTableName);
            stringOverloadEdgePropertiesTableName.Should().BeEquivalentTo(createContainerOverloadEdgePropertiesTableName);

            streamModelOverloadEdgePropertiesTableName.Should().BeEquivalentTo(stringOverloadEdgePropertiesTableName);
            streamModelOverloadEdgePropertiesTableName.Should().BeEquivalentTo(createContainerOverloadEdgePropertiesTableName);

            createContainerOverloadEdgePropertiesTableName.Should().BeEquivalentTo(stringOverloadEdgePropertiesTableName);
            createContainerOverloadEdgePropertiesTableName.Should().BeEquivalentTo(streamModelOverloadEdgePropertiesTableName);
        }
    }
}
