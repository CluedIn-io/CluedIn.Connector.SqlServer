using AutoFixture.Xunit2;
using CluedIn.Connector.SqlServer.Utils;
using FluentAssertions;
using System;
using Xunit;

namespace CluedIn.Connector.SqlServer.Unit.Tests.Utils;

public class StringExtensionsTests
{
    [Theory]
    [InlineAutoData("TableName", "TableName")]
    [InlineAutoData("TableName1", "TableName1")]
    [InlineAutoData("TableName+", "TableName")]
    [InlineAutoData("+TableName", "TableName")]
    [InlineAutoData("1", "_1")]
    [InlineAutoData("123", "_123")]
    [InlineAutoData("1Table", "_1Table")]
    [InlineAutoData("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa_0392220648")] // Expected result should not be changed, since we need to ensure that hash is stable
    public void ToSanitizedTableName_ShouldYieldName(string input, string expectedOutput)
    {
        // arrange
        // act
        var result = input.ToSanitizedSqlName();

        // assert
        result.Should().Be(expectedOutput);
    }

    [Theory]
    [InlineAutoData("!@#")]
    [InlineAutoData("")]
    public void ToSanitizedTableName_ShouldThrow(string input)
    {
        // Arrange
        // Act
        var action = () => input.ToSanitizedSqlName();

        // Assert
        action.Should().Throw<ArgumentException>();
    }

    [Theory]
    [InlineAutoData("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")]
    [InlineAutoData("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")]
    [InlineAutoData("cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc")]
    public void ToSanitizedTableName_ShouldLimitSize(string input)
    {
        // arrange
        // act
        var result = input.ToSanitizedSqlName();

        // assert
        result.Length.Should().Be(127);
    }
}
