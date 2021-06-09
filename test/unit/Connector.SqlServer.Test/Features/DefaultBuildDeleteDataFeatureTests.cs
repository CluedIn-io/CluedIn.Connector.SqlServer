using System;
using System.Linq;
using AutoFixture.Xunit2;
using CluedIn.Connector.SqlServer.Features;
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

        public DefaultBuildDeleteDataFeatureTests()
        {
            _testContext = new TestContext();
            _logger = new Mock<ILogger>();
            _sut = new DefaultBuildDeleteDataFeature();
        }

        [Theory, InlineAutoData]
        public void BuildDeleteDataSql_NullContext_Throws(
            Guid providerDefinitionId,
            string containerName,
            string data)
        {
            Assert.Throws<ArgumentNullException>("executionContext", () => _sut.BuildDeleteDataSql(null, providerDefinitionId, containerName, data, _logger.Object));
        }

        [Theory]
        [InlineAutoData(null)]
        [InlineAutoData("")]
        [InlineAutoData("\t\t   ")]
        public void BuildDeleteDataSql_InvalidContainerName_Throws(
            string containerName,
            Guid providerDefinitionId,
            string data)
        {
            Assert.Throws<InvalidOperationException>(() => _sut.BuildDeleteDataSql(_testContext.Context, providerDefinitionId, containerName, data, _logger.Object));
        }

        [Theory, InlineAutoData]
        public void BuildDeleteDataSql_NullData_Throws(
            Guid providerDefinitionId,
            string containerName)
        {
            Assert.Throws<InvalidOperationException>(() => _sut.BuildDeleteDataSql(_testContext.Context, providerDefinitionId, containerName, null, _logger.Object));
        }


        [Theory, InlineAutoData]
        public void BuildDeleteDataSql_NullLogger_Throws(
            Guid providerDefinitionId,
            string containerName,
            string data)
        {
            Assert.Throws<ArgumentNullException>("logger", () => _sut.BuildDeleteDataSql(_testContext.Context, providerDefinitionId, containerName, data, null));
        }

        [Theory, InlineAutoData]
        public void BuildDeleteDataSql_ValidData_IsSuccessful(
            string name,
            string data,
            Guid providerDefinitionId)
        {
            var execContext = _testContext.Context;
            var result = _sut.BuildDeleteDataSql(execContext, providerDefinitionId, name, data, _logger.Object);
            var command = result.Single();
            Assert.Equal($"DELETE FROM {name} WHERE {DefaultBuildDeleteDataFeature.DefaultKeyField} = @KeyValue", command.Text.Trim());
            Assert.Single(command.Parameters);

            var paramsList = command.Parameters.ToList();
            var parameter = paramsList[0];
            Assert.Equal(data, parameter.Value);
        }
    }
}
