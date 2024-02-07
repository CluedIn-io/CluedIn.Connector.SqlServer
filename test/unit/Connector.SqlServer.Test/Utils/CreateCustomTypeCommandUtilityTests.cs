using AutoFixture.Xunit2;
using CluedIn.Connector.SqlServer.Unit.Tests.Customizations;
using CluedIn.Connector.SqlServer.Utils;
using CluedIn.Core.Connectors;
using CluedIn.Core.Streams.Models;
using FluentAssertions;
using NSubstitute;
using Xunit;

namespace CluedIn.Connector.SqlServer.Unit.Tests.Utils;

public class CreateCustomTypeCommandUtilityTests
{
    [Theory]
    [InlineAutoData("TestTableName", "TestTableNameCodesType", "TestTableNameOutgoingEdgesType", "TestTableNameOutgoingEdgePropertiesType", "TestTableNameIncomingEdgesType", "TestTableNameIncomingEdgePropertiesType")]
    [InlineAutoData(
            "TestTableName127CharactersAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
            "TestTableName127CharactersAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA_3b2a4bbeCodesType",
            "TestTableName127CharactersAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA_3b2a4bbeOutgoingEdgesType",
            "TestTableName127CharactersAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA_3b2a4bbeOutgoingEdgePropertiesType",
            "TestTableName127CharactersAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA_3b2a4bbeIncomingEdgesType",
            "TestTableName127CharactersAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA_3b2a4bbeIncomingEdgePropertiesType")]
    internal void CustomTypeNamesShouldBeExpectedNames(
            string containerName,
            string expectedCodeCustomTableTypeName, string expectedOutgoingEdgeCustomTableTypeName, string expectedOutgoingEdgePropertiesCustomTableTypeName, string expectedIncomingEdgeCustomTableTypeName, string expectedIncomingEdgePropertiesCustomTableTypeName)
    {
        // arrange
        var schema = new SqlName();
        var streamModel = Substitute.For<IReadOnlyStreamModel>();
        var createContainerModel = Substitute.For<IReadOnlyCreateContainerModelV2>();
        streamModel.ContainerName.Returns(containerName);
        createContainerModel.Name.Returns(containerName);

        // act
        var codeCustomTableTypeName = CreateCustomTypeCommandUtility.GetCodeTableCustomTypeName(streamModel, schema);
        var outgoingEdgeCustomTableTypeName = CreateCustomTypeCommandUtility.GetEdgeTableCustomTypeName(streamModel, EdgeDirection.Outgoing, schema);
        var outgoingEdgePropertiesCustomTableTypeName = CreateCustomTypeCommandUtility.GetEdgePropertiesTableCustomTypeName(streamModel, EdgeDirection.Outgoing, schema);
        var incomingEdgeCustomTableTypeName = CreateCustomTypeCommandUtility.GetEdgeTableCustomTypeName(streamModel, EdgeDirection.Incoming, schema);
        var incomingEdgePropertiesCustomTableTypeName = CreateCustomTypeCommandUtility.GetEdgePropertiesTableCustomTypeName(streamModel, EdgeDirection.Incoming, schema);

        // assert
        codeCustomTableTypeName.LocalName.Value.Should().BeEquivalentTo(expectedCodeCustomTableTypeName);
        outgoingEdgeCustomTableTypeName.LocalName.Value.Should().BeEquivalentTo(expectedOutgoingEdgeCustomTableTypeName);
        outgoingEdgePropertiesCustomTableTypeName.LocalName.Value.Should().BeEquivalentTo(expectedOutgoingEdgePropertiesCustomTableTypeName);
        incomingEdgeCustomTableTypeName.LocalName.Value.Should().BeEquivalentTo(expectedIncomingEdgeCustomTableTypeName);
        incomingEdgePropertiesCustomTableTypeName.LocalName.Value.Should().BeEquivalentTo(expectedIncomingEdgePropertiesCustomTableTypeName);
    }

    [Theory, AutoNData]
    internal void AllOverloadsShouldGiveSameCodeTableCustomTypeName(IReadOnlyStreamModel streamModel, IReadOnlyCreateContainerModelV2 createContainerModel)
    {
        // arrange
        var schema = new SqlName();
        var containerName = "test";
        streamModel.ContainerName.Returns(containerName);
        createContainerModel.Name.Returns(containerName);

        var mainTableName = TableNameUtility.GetMainTableName(containerName, schema);

        // act
        var stringOverloadName = CreateCustomTypeCommandUtility.GetCodeTableCustomTypeName(mainTableName, schema);
        var streamModelOverloadName = CreateCustomTypeCommandUtility.GetCodeTableCustomTypeName(streamModel, schema);
        var createContainerOverloadName = CreateCustomTypeCommandUtility.GetCodeTableCustomTypeName(createContainerModel, schema);

        // assert
        stringOverloadName.Should().BeEquivalentTo(streamModelOverloadName);
        stringOverloadName.Should().BeEquivalentTo(createContainerOverloadName);

        streamModelOverloadName.Should().BeEquivalentTo(stringOverloadName);
        streamModelOverloadName.Should().BeEquivalentTo(createContainerOverloadName);

        createContainerOverloadName.Should().BeEquivalentTo(stringOverloadName);
        createContainerOverloadName.Should().BeEquivalentTo(streamModelOverloadName);
    }

    [Theory, AutoNData]
    internal void AllOverloadsShouldGiveSameEdgeTableCustomTypeName(IReadOnlyStreamModel streamModel, IReadOnlyCreateContainerModelV2 createContainerModel)
    {
        // arrange
        var schema = new SqlName();
        var containerName = "test";
        streamModel.ContainerName.Returns(containerName);
        createContainerModel.Name.Returns(containerName);

        var mainTableName = TableNameUtility.GetMainTableName(containerName, schema);

        // act
        var stringOverloadName = CreateCustomTypeCommandUtility.GetEdgeTableCustomTypeName(mainTableName, EdgeDirection.Outgoing, schema);
        var streamModelOverloadName = CreateCustomTypeCommandUtility.GetEdgeTableCustomTypeName(streamModel, EdgeDirection.Outgoing, schema);
        var createContainerOverloadName = CreateCustomTypeCommandUtility.GetEdgeTableCustomTypeName(createContainerModel, EdgeDirection.Outgoing, schema);

        // assert
        stringOverloadName.Should().BeEquivalentTo(streamModelOverloadName);
        stringOverloadName.Should().BeEquivalentTo(createContainerOverloadName);

        streamModelOverloadName.Should().BeEquivalentTo(stringOverloadName);
        streamModelOverloadName.Should().BeEquivalentTo(createContainerOverloadName);

        createContainerOverloadName.Should().BeEquivalentTo(stringOverloadName);
        createContainerOverloadName.Should().BeEquivalentTo(streamModelOverloadName);
    }

    [Theory, AutoNData]
    internal void AllOverloadsShouldGiveSameEdgePropertiesTableCustomTypeName(IReadOnlyStreamModel streamModel, IReadOnlyCreateContainerModelV2 createContainerModel)
    {
        // arrange
        var schema = new SqlName();
        var containerName = "test";
        streamModel.ContainerName.Returns(containerName);
        createContainerModel.Name.Returns(containerName);

        var mainTableName = TableNameUtility.GetMainTableName(containerName, schema);

        // act
        var stringOverloadName = CreateCustomTypeCommandUtility.GetEdgePropertiesTableCustomTypeName(mainTableName, EdgeDirection.Outgoing, schema);
        var streamModelOverloadName = CreateCustomTypeCommandUtility.GetEdgePropertiesTableCustomTypeName(streamModel, EdgeDirection.Outgoing, schema);
        var createContainerOverloadName = CreateCustomTypeCommandUtility.GetEdgePropertiesTableCustomTypeName(createContainerModel, EdgeDirection.Outgoing, schema);

        // assert
        stringOverloadName.Should().BeEquivalentTo(streamModelOverloadName);
        stringOverloadName.Should().BeEquivalentTo(createContainerOverloadName);

        streamModelOverloadName.Should().BeEquivalentTo(stringOverloadName);
        streamModelOverloadName.Should().BeEquivalentTo(createContainerOverloadName);

        createContainerOverloadName.Should().BeEquivalentTo(stringOverloadName);
        createContainerOverloadName.Should().BeEquivalentTo(streamModelOverloadName);
    }
}
