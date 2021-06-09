using System;
using System.Collections.Generic;
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
            IDictionary<string, object> data)
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
            IDictionary<string, object> data)
        {
            Assert.Throws<InvalidOperationException>(() => _sut.BuildDeleteDataSql(_testContext.Context, providerDefinitionId, containerName, data, _logger.Object));
        }

        [Theory]
        [InlineAutoData(null)]
        [InlineAutoData("")]
        [InlineAutoData("\t\t   ")]
        public void BuildDeleteDataSql_EmptyData_Throws(
            string containerName,
            Guid providerDefinitionId)
        {
            Assert.Throws<InvalidOperationException>(() => _sut.BuildDeleteDataSql(_testContext.Context, providerDefinitionId, containerName, new Dictionary<string, object>(), _logger.Object));
        }

        [Theory, InlineAutoData]
        public void BuildDeleteDataSql_NullLogger_Throws(
            Guid providerDefinitionId,
            string containerName,
            IDictionary<string, object> data)
        {
            Assert.Throws<ArgumentNullException>("logger", () => _sut.BuildDeleteDataSql(_testContext.Context, providerDefinitionId, containerName, data, null));
        }

        [Theory, InlineAutoData]
        public void BuildDeleteDataSql_ValidData_IsSuccessful(
            string name,
            int field1,
            string field2,
            DateTime field3,
            decimal field4,
            bool field5,
            Guid providerDefinitionId)
        {

            var data = new Dictionary<string, object>
                        {
                             { "Field1", field1   },
                             { "Field2", field2   },
                             { "Field3", field3  },
                             { "Field4", field4   },
                             { "Field5", field5   }
                        };

            var execContext = _testContext.Context;
            var result = _sut.BuildDeleteDataSql(execContext, providerDefinitionId, name, data, _logger.Object);
            var command = result.Single();
            Assert.Equal($"DELETE FROM {name} WHERE [Field1] = @Field1 AND [Field2] = @Field2 AND [Field3] = @Field3 AND [Field4] = @Field4 AND [Field5] = @Field5;", command.Text.Trim());
            Assert.Equal(data.Count, command.Parameters.Count());

            var paramsList = command.Parameters.ToList();
            for (var index = 0; index < data.Count; index++)
            {
                var parameter = paramsList[index];
                var val = data[$"Field{index + 1}"];
                Assert.Equal(val, parameter.Value);
            }
        }
    }
}
