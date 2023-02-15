﻿using System;
using System.Collections.Generic;
using System.Linq;
using AutoFixture.Xunit2;
using CluedIn.Connector.Common.Helpers;
using CluedIn.Connector.SqlServer.Features;
using CluedIn.Connector.SqlServer.Utils;
using CluedIn.Core.Data;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace CluedIn.Connector.SqlServer.Unit.Tests.Features
{
    public class DefaultBuildDeleteDataFeatureTests
    {
        private readonly TestContext _testContext;
        private readonly Mock<ILogger> _logger;
        private readonly DefaultBuildDeleteDataFeature _sut;
        private readonly IList<IEntityCode> _codes = new List<IEntityCode>
        {
            EntityCode.FromKey("/Person#CluedIn(email):admin@foobar.com"),
            EntityCode.FromKey("/Organization#Acceptance:495aa299-f13c-42f9-807f-3060f3d4e218"),
            EntityCode.FromKey("/Organization#Acceptance:8e8cd067-ad4a-42f9-8fee-2d3028d08d39")
        };

        public DefaultBuildDeleteDataFeatureTests()
        {
            _testContext = new TestContext();
            _logger = new Mock<ILogger>();
            _sut = new DefaultBuildDeleteDataFeature();
        }

        [Theory, InlineAutoData]
        public void BuildDeleteDataSql_NullContext_Throws(
            Guid providerDefinitionId,
            SqlTableName tableName,
            string originEntityCode,
            Guid entityId)
        {
            Assert.Throws<ArgumentNullException>("executionContext", () => _sut.BuildDeleteDataSql(null, providerDefinitionId, tableName, originEntityCode, _codes, entityId, _logger.Object));
        }

        [Theory, InlineAutoData]
        public void BuildDeleteDataSql_NullLogger_Throws(
            Guid providerDefinitionId,
            SqlTableName tableName,
            string originEntityCode,
            Guid entityId)
        {
            Assert.Throws<ArgumentNullException>("logger", () => _sut.BuildDeleteDataSql(_testContext.Context, providerDefinitionId, tableName, originEntityCode, _codes, entityId, null));
        }

        [Theory, InlineAutoData]
        public void BuildDeleteDataSql_WithOriginEntityCode_IsSuccessful(
            Guid providerDefinitionId,
            SqlTableName tableName,
            string originEntityCode)
        {
            var execContext = _testContext.Context;
            var result = _sut.BuildDeleteDataSql(execContext, providerDefinitionId, tableName, originEntityCode, null, null, _logger.Object);
            var command = result.Single();

            Assert.Equal($"DELETE FROM [{tableName.Schema}].[{tableName.LocalName}] WHERE [OriginEntityCode] = @OriginEntityCode;", command.Text.Trim());
            Assert.Single(command.Parameters);

            var parameter = command.Parameters.First();
            Assert.Equal(originEntityCode, parameter.Value);
            Assert.Equal("OriginEntityCode", parameter.ParameterName);
        }

        [Theory, InlineAutoData]
        public void BuildDeleteDataSql_WithEntityId_IsSuccessful(
            Guid providerDefinitionId,
            SqlTableName tableName,
            Guid entityId)
        {
            var execContext = _testContext.Context;
            var result = _sut.BuildDeleteDataSql(execContext, providerDefinitionId, tableName, null, null, entityId, _logger.Object);
            var command = result.Single();

            Assert.Equal($"DELETE FROM [{tableName.Schema}].[{tableName.LocalName}] WHERE [Id] = @Id;", command.Text.Trim());
            Assert.Single(command.Parameters);

            var parameter = command.Parameters.First();
            Assert.Equal(entityId, parameter.Value);
            Assert.Equal("Id", parameter.ParameterName);
        }

        [Theory, InlineAutoData]
        public void BuildDeleteDataSql_WithCodes_IsSuccessful(
            Guid providerDefinitionId,
            SqlTableName tableName)
        {
            var execContext = _testContext.Context;
            var result = _sut.BuildDeleteDataSql(execContext, providerDefinitionId, tableName, null, _codes, null, _logger.Object).ToList();
            Assert.Equal(result.Count(), _codes.Count);

            for(var x = 0; x < result.Count(); x++)
            {
                var command = result[x];
                Assert.Equal($"DELETE FROM [{tableName.Schema}].[{tableName.LocalName}] WHERE [Code] = @Code;", command.Text.Trim());
                Assert.Single(command.Parameters);
                var parameter = command.Parameters.First();
                Assert.Equal(_codes[x], parameter.Value);
                Assert.Equal("Code", parameter.ParameterName);
            }
        }
    }
}
