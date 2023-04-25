using CluedIn.Connector.SqlServer.Unit.Tests.Customizations;
using CluedIn.Connector.SqlServer.Utils;
using CluedIn.Connector.SqlServer.Utils.TableDefinitions;
using CluedIn.Core.Connectors;
using CluedIn.Core.Data.Vocabularies;
using CluedIn.Core.Data.Vocabularies.CluedIn;
using FluentAssertions;
using NSubstitute;
using System.Data;
using Xunit;

namespace CluedIn.Connector.SqlServer.Unit.Tests.Utils
{
    public class ColumnDefinitionsUtilityTests
    {
        [Theory, AutoNData]
        internal void GetMainTableDefinitions_AddsDefinitionsForProperties(IReadOnlyCreateContainerModelV2 createContainerModel)
        {
            // arrange
            createContainerModel.Properties.Returns(new[]
            {
                new ConnectorProperty("Name", new EntityPropertyConnectorPropertyDataType(typeof(string))),
                new ConnectorProperty("FirstName", new VocabularyKeyConnectorPropertyDataType(new CluedInUserVocabulary().FirstName)),
                new ConnectorProperty("LastName", new VocabularyKeyDataTypeConnectorPropertyDataType(VocabularyKeyDataType.Text))
            });

            // act
            var columns = MainTableDefinition.GetColumnDefinitions(createContainerModel);

            // assert
            columns.Should().Contain(column => column.Name.Equals("Name".ToSanitizedSqlName()) && column.ConnectorSqlType.SqlType == SqlDbType.NVarChar);
            columns.Should().Contain(column => column.Name.Equals("FirstName".ToSanitizedSqlName()) && column.ConnectorSqlType.SqlType == SqlDbType.NVarChar);
            columns.Should().Contain(column => column.Name.Equals("LastName".ToSanitizedSqlName()) && column.ConnectorSqlType.SqlType == SqlDbType.NVarChar);
        }
    }
}
