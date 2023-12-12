using AutoFixture.Xunit2;
using CluedIn.Connector.SqlServer.Utils;
using FluentAssertions;
using Xunit;

namespace CluedIn.Connector.SqlServer.Unit.Tests.Utils;

public class StringExtensionsTests
{
    [Theory]
    [InlineAutoData("TableName", "TableName")]
    [InlineAutoData("TableName1", "TableName1")]
    [InlineAutoData("TableName+", "TableName")]
    [InlineAutoData("+TableName", "TableName")]
    [InlineAutoData("1", "Table1")]
    [InlineAutoData("123", "Table123")]
    [InlineAutoData("1Table", "Table1Table")]
    public void ToSanitizedTableName_ShouldYieldName(string input, string expectedOutput)
    {
        // arrange
        // act
        var result = input.ToSanitizedSqlName();

        // assert
        result.Should().Be(expectedOutput);
    }
}
