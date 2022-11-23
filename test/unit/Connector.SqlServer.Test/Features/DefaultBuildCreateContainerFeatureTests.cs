﻿using AutoFixture.Xunit2;
using CluedIn.Connector.SqlServer.Features;
using CluedIn.Connector.SqlServer.Utils;
using CluedIn.Core.Connectors;
using Microsoft.Extensions.Logging;
using Moq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Xunit;

namespace CluedIn.Connector.SqlServer.Unit.Tests.Features
{
    public class DefaultBuildCreateContainerFeatureTests
    {
        private readonly TestContext _testContext;
        private readonly Mock<ILogger> _logger;
        private readonly DefaultBuildCreateContainerFeature _sut;
        private readonly IList<string> _defaultKeyFields = new List<string> { "OriginEntityCode" };

        public DefaultBuildCreateContainerFeatureTests()
        {
            _testContext = new TestContext();
            _logger = new Mock<ILogger>();
            _sut = new DefaultBuildCreateContainerFeature();
        }

        [Theory, InlineAutoData]
        public void BuildCreateContainerSql_NullContext_Throws(
            Guid providerDefinitionId,
            SqlTableName name,
            List<ConnectionDataType> columns)
        {
            Assert.Throws<ArgumentNullException>("executionContext", () => _sut.BuildCreateContainerSql(null, providerDefinitionId, name, columns, _defaultKeyFields, _logger.Object));
        }

        [Theory, InlineAutoData]
        public void BuildCreateContainerSql_NullColumns_Throws(
            Guid providerDefinitionId,
            SqlTableName name)
        {
            Assert.Throws<InvalidOperationException>(() => _sut.BuildCreateContainerSql(_testContext.Context, providerDefinitionId, name, null, _defaultKeyFields, _logger.Object));
        }

        [Theory, InlineAutoData]
        public void BuildCreateContainerSql_EmptyColumns_Throws(
            Guid providerDefinitionId,
            SqlTableName name)
        {
            Assert.Throws<InvalidOperationException>(() => _sut.BuildCreateContainerSql(_testContext.Context, providerDefinitionId, name, new List<ConnectionDataType>(), _defaultKeyFields, _logger.Object));
        }

        [Theory, InlineAutoData]
        public void BuildCreateContainerSql_NullLogger_Throws(
            Guid providerDefinitionId,
            SqlTableName name,
            List<ConnectionDataType> columns)
        {
            Assert.Throws<ArgumentNullException>("logger", () => _sut.BuildCreateContainerSql(_testContext.Context, providerDefinitionId, name, columns, _defaultKeyFields, null));
        }

        [Theory, InlineAutoData]
        public void BuildCreateContainerSql_ValidColumns_IsSuccessful(
            Guid providerDefinitionId,
            SqlTableName tableName,
            List<ConnectionDataType> columns)
        {
            var expected = new StringBuilder();
            expected.AppendLine($"CREATE TABLE [{tableName.Schema}].[{tableName.LocalName}](");
            expected.AppendJoin(", ", columns.Select(c => $"[{c.Name.ToSanitizedSqlName()}] nvarchar(1024) NULL"));
            expected.AppendLine(") ON[PRIMARY]");

            var execContext = _testContext.Context;
            var result = _sut.BuildCreateContainerSql(execContext, providerDefinitionId, tableName, columns, _defaultKeyFields, _logger.Object);
            var command = result.Single();
            Assert.Equal(expected.ToString(), command.Text);
            Assert.Null(command.Parameters);
        }
    }
}
