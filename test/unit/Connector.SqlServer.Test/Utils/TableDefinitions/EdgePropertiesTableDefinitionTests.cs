using CluedIn.Connector.SqlServer.Unit.Tests.Customizations;
using CluedIn.Connector.SqlServer.Utils.TableDefinitions;
using CluedIn.Connector.SqlServer.Utils;
using CluedIn.Core.Streams.Models;
using FluentAssertions;
using Xunit;

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
    }
}
