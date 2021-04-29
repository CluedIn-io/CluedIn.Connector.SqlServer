using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using AutoFixture.Xunit2;
using CluedIn.Connector.SqlServer.Features;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;
using Xunit.Abstractions;

namespace CluedIn.Connector.SqlServer.Unit.Tests.Features
{
    public class DefaultBuildStoreDataFeatureTests
    {
        private readonly TestContext _testContext;
        private readonly Mock<ILogger> _logger;
        private readonly DefaultBuildStoreDataFeature _sut;

        public DefaultBuildStoreDataFeatureTests()
        {
            _testContext = new TestContext();
            _logger = new Mock<ILogger>();
            _sut = new DefaultBuildStoreDataFeature();
        }

        [Theory, InlineAutoData]
        public void BuildStoreDataSql_NullContext_Throws(
            Guid providerDefinitionId,
            string containerName,
            IDictionary<string, object> data)
        {
            Assert.Throws<ArgumentNullException>("executionContext", () => _sut.BuildStoreDataSql(null, providerDefinitionId, containerName, data, _logger.Object));
        }

        [Theory]
        [InlineAutoData(null)]
        [InlineAutoData("")]
        [InlineAutoData("\t\t   ")]
        public void BuildStoreDataSql_InvalidContainerName_Throws(
            string containerName,
            Guid providerDefinitionId,
            IDictionary<string, object> data)
        {
            Assert.Throws<InvalidOperationException>(() => _sut.BuildStoreDataSql(_testContext.Context, providerDefinitionId, containerName, data, _logger.Object));
        }

        [Theory, InlineAutoData]
        public void BuildStoreDataSql_NullData_Throws(
            Guid providerDefinitionId,
            string containerName)
        {
            Assert.Throws<InvalidOperationException>(() => _sut.BuildStoreDataSql(_testContext.Context, providerDefinitionId, containerName, null, _logger.Object));
        }

        [Theory, InlineAutoData]
        public void BuildStoreDataSql_EmptyData_Throws(
            Guid providerDefinitionId,
            string containerName)
        {
            Assert.Throws<InvalidOperationException>(() => _sut.BuildStoreDataSql(_testContext.Context, providerDefinitionId, containerName, new Dictionary<string, object>(), _logger.Object));
        }

        [Theory, InlineAutoData]
        public void BuildStoreDataSql_NullLogger_Throws(
            Guid providerDefinitionId,
            string containerName,
            IDictionary<string, object> data)
        {
            Assert.Throws<ArgumentNullException>("logger", () => _sut.BuildStoreDataSql(_testContext.Context, providerDefinitionId, containerName, data, null));
        }

        [Theory, InlineAutoData]
        public void BuildStoreDataSql_ValidData_IsSuccessful(
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
            var result = _sut.BuildStoreDataSql(execContext, providerDefinitionId, name, data, _logger.Object);
            var command = result.Single();
            Assert.Equal($"MERGE [{name}] AS target" + Environment.NewLine +
                         "USING (SELECT @Field1, @Field2, @Field3, @Field4, @Field5) AS source ([Field1], [Field2], [Field3], [Field4], [Field5])" + Environment.NewLine +
                         "  ON (target.[OriginEntityCode] = source.[OriginEntityCode])" + Environment.NewLine +
                         "WHEN MATCHED THEN" + Environment.NewLine +
                         "  UPDATE SET target.[Field1] = source.[Field1], target.[Field2] = source.[Field2], target.[Field3] = source.[Field3], target.[Field4] = source.[Field4], target.[Field5] = source.[Field5]" + Environment.NewLine +
                         "WHEN NOT MATCHED THEN" + Environment.NewLine +
                         "  INSERT ([Field1], [Field2], [Field3], [Field4], [Field5])" + Environment.NewLine +
                         "  VALUES (source.[Field1], source.[Field2], source.[Field3], source.[Field4], source.[Field5]);", command.Text.Trim());            
            Assert.Equal(data.Count, command.Parameters.Count());

            var paramsList = command.Parameters.ToList();
            for (var index = 0; index < data.Count; index++)
            {
                var parameter = paramsList[index];
                var val = data[$"Field{index + 1}"];
                Assert.Equal(val, parameter.Value);
            }
        }

        [Theory, InlineAutoData]
        public void BuildStoreDataSql_InvalidData_SetsValueAsString(
           string name,
           int field1,
           string field2,
           string[] invalidField,
           Guid providerDefinitionId)
        {
            var data = new Dictionary<string, object>
                        {
                             { "Field1", field1   },
                             { "Field2", field2   },
                             { "InvalidField", invalidField  }
                        };
            
            var execContext = _testContext.Context;
            var result = _sut.BuildStoreDataSql(execContext, providerDefinitionId, name, data, _logger.Object);
            var command = result.Single();
            Assert.Equal($"MERGE [{name}] AS target" + Environment.NewLine +
                         "USING (SELECT @Field1, @Field2, @InvalidField) AS source ([Field1], [Field2], [InvalidField])" + Environment.NewLine +
                         "  ON (target.[OriginEntityCode] = source.[OriginEntityCode])" + Environment.NewLine +
                         "WHEN MATCHED THEN" + Environment.NewLine +
                         "  UPDATE SET target.[Field1] = source.[Field1], target.[Field2] = source.[Field2], target.[InvalidField] = source.[InvalidField]" + Environment.NewLine +
                         "WHEN NOT MATCHED THEN" + Environment.NewLine +
                         "  INSERT ([Field1], [Field2], [InvalidField])" + Environment.NewLine +
                         "  VALUES (source.[Field1], source.[Field2], source.[InvalidField]);", command.Text.Trim());
            Assert.Equal(3, command.Parameters.Count());
            var paramsList = command.Parameters.ToList();

            Assert.Equal(paramsList[0].Value, data["Field1"]);
            Assert.Equal(paramsList[1].Value, data["Field2"]);
            Assert.Equal(paramsList[2].Value, data["InvalidField"].ToString());
        }

    }
}
