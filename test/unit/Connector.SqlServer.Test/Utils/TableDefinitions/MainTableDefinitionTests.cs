using CluedIn.Connector.SqlServer.Unit.Tests.Customizations;
using CluedIn.Connector.SqlServer.Utils.TableDefinitions;
using CluedIn.Core.Connectors;
using CluedIn.Core.Data.Vocabularies;
using CluedIn.Core.Streams.Models;
using FluentAssertions;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using Xunit;

namespace CluedIn.Connector.SqlServer.Unit.Tests.Utils.TableDefinitions
{
    public class MainTableDefinitionTests
    {
        [Theory, AutoNData]
        public void GetColumnDefinitions_ShouldIncludeProperties()
        {
            // arrange
            var properties = new (string, ConnectorPropertyDataType)[]
            {
                ("Name", new EntityPropertyConnectorPropertyDataType(typeof(string))),
                ("JobTitle", new VocabularyKeyDataTypeConnectorPropertyDataType(VocabularyKeyDataType.Text))
            };

            // act
            var syncColumnDefinitions = MainTableDefinition.GetColumnDefinitions(StreamMode.Sync, properties);
            var eventColumnDefinitions = MainTableDefinition.GetColumnDefinitions(StreamMode.EventStream, properties);

            // assert
            syncColumnDefinitions.Should().Contain(column => column.Name == "Name" && column.ConnectorSqlType.SqlType == SqlDbType.NVarChar);
            syncColumnDefinitions.Should().Contain(column => column.Name == "JobTitle" && column.ConnectorSqlType.SqlType == SqlDbType.NVarChar);

            eventColumnDefinitions.Should().Contain(column => column.Name == "Name" && column.ConnectorSqlType.SqlType == SqlDbType.NVarChar);
            eventColumnDefinitions.Should().Contain(column => column.Name == "JobTitle" && column.ConnectorSqlType.SqlType == SqlDbType.NVarChar);
        }

        [Theory, AutoNData]
        public void GetColumnDefinitions_ShouldIncludeChangeTypeAndCorrelationId_ForEventMode()
        {
            // arrange
            var properties = Array.Empty<(string name, ConnectorPropertyDataType dataType)>();

            // act
            var eventColumnDefinitions = MainTableDefinition.GetColumnDefinitions(StreamMode.EventStream, properties);

            // assert
            eventColumnDefinitions.Should().Contain(column => column.Name == "ChangeType");
            eventColumnDefinitions.Should().Contain(column => column.Name == "CorrelationId");
        }

        [Theory, AutoNData]
        public void GetColumnDefinitions_ShouldNotIncludePropertiesMultipleTimes()
        {
            // arrange
            var properties = new (string, ConnectorPropertyDataType)[]
            {
                ("PersistVersion", new EntityPropertyConnectorPropertyDataType(typeof(string))),
            };

            // act
            var syncColumnDefinitions = MainTableDefinition.GetColumnDefinitions(StreamMode.Sync, properties);
            var eventColumnDefinitions = MainTableDefinition.GetColumnDefinitions(StreamMode.EventStream, properties);

            // assert
            syncColumnDefinitions.Should().ContainSingle(column => column.Name == "PersistVersion");
            eventColumnDefinitions.Should().ContainSingle(column => column.Name == "PersistVersion");
        }
    }
}
