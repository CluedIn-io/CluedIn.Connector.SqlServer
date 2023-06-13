using CluedIn.Connector.SqlServer.Unit.Tests.Customizations;
using CluedIn.Connector.SqlServer.Utils.TableDefinitions;
using CluedIn.Core.Streams.Models;
using FluentAssertions;
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
    }
}
