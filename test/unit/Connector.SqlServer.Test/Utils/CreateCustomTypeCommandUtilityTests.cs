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
