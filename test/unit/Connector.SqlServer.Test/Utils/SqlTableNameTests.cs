using AutoFixture.Xunit2;
using CluedIn.Connector.SqlServer.Utils;
using CluedIn.Core.Connectors;
using FluentAssertions;
using System;
using Xunit;

namespace CluedIn.Connector.SqlServer.Unit.Tests.Utils
{
    public class SqlTableNameTests
    {
        [Theory]
        [InlineAutoData(null)]
        [InlineAutoData("")]
        [InlineAutoData("\t\t   ")]
        public void FromUnsafeName_ShouldThrowForInvalidName_WhenUseStringOverload(string tableName,
            SqlName schema)
        {
            // arrange
            Action action = () => SqlTableName.FromUnsafeName(tableName, schema);

            // act
            // assert
            action.Should().Throw<ArgumentException>();
        }

        [Theory]
        [InlineAutoData(null)]
        [InlineAutoData("")]
        [InlineAutoData("\t\t   ")]
        public void FromUnsafeName_ShouldThrowForInvalidName_WhenUseConfigOverload(string tableName,
            string schema)
        {
            // arrange
            var config = new ConnectorConnectionBase { Authentication = { [SqlServerConstants.KeyName.Schema] = schema } };

            Action action = () => SqlTableName.FromUnsafeName(tableName, config);

            // act
            // assert
            action.Should().Throw<ArgumentException>();
        }
    }
}
