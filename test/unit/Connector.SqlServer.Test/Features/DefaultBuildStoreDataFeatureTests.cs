using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using AutoFixture.Xunit2;
using CluedIn.Connector.SqlServer.Features;
using CluedIn.Core;
using CluedIn.Core.Data.Parts;
using CluedIn.Core.Streams.Models;
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
        private readonly IList<string> _defaultKeyFields = new List<string> { "OriginEntityCode" };

        public DefaultBuildStoreDataFeatureTests()
        {
            _testContext = new TestContext();
            _logger = new Mock<ILogger>();
            _sut = new DefaultBuildStoreDataFeature();
        }

        [Theory, InlineAutoData]
        public void BuildStoreDataSql_NullContext_Throws(Guid providerDefinitionId,
            string containerName,
            IDictionary<string, object> data,
            string correlationId,
            DateTimeOffset timestamp,
            VersionChangeType changeType)
        {
            Assert.Throws<ArgumentNullException>("executionContext", () => _sut.BuildStoreDataSql(null, providerDefinitionId, containerName, data, _defaultKeyFields, StreamMode.Sync, correlationId, timestamp, changeType, _logger.Object).ToList());
        }

        [Theory]
        [InlineAutoData(null)]
        [InlineAutoData("")]
        [InlineAutoData("\t\t   ")]
        public void BuildStoreDataSql_InvalidContainerName_Throws(
            string containerName,
            Guid providerDefinitionId,
            IDictionary<string, object> data,
            string correlationId,
            DateTimeOffset timestamp,
            VersionChangeType changeType)
        {
            Assert.Throws<InvalidOperationException>(() => _sut.BuildStoreDataSql(_testContext.Context, providerDefinitionId, containerName, data, _defaultKeyFields, StreamMode.Sync, correlationId, timestamp, changeType, _logger.Object).ToList());
        }

        [Theory, InlineAutoData]
        public void BuildStoreDataSql_NullData_Throws(
            Guid providerDefinitionId,
            string containerName,
            string correlationId,
            DateTimeOffset timestamp,
            VersionChangeType changeType)
        {

            _sut.BuildStoreDataSql(_testContext.Context, providerDefinitionId, containerName, null, _defaultKeyFields, StreamMode.Sync, correlationId, timestamp, changeType, _logger.Object);
            Assert.Throws<InvalidOperationException>(() => _sut.BuildStoreDataSql(_testContext.Context, providerDefinitionId, containerName, null, _defaultKeyFields, StreamMode.Sync, correlationId, timestamp, changeType, _logger.Object).ToList());
        }

        [Theory, InlineAutoData]
        public void BuildStoreDataSql_EmptyData_Throws(
            Guid providerDefinitionId,
            string containerName,
            string correlationId,
            DateTimeOffset timestamp,
            VersionChangeType changeType)
        {
            Assert.Throws<InvalidOperationException>(() => _sut.BuildStoreDataSql(_testContext.Context, providerDefinitionId, containerName, new Dictionary<string, object>(), _defaultKeyFields, StreamMode.Sync, correlationId, timestamp, changeType, _logger.Object).ToList());
        }

        [Theory, InlineAutoData]
        public void BuildStoreDataSql_NullLogger_Throws(
            Guid providerDefinitionId,
            string containerName,
            IDictionary<string, object> data,
            string correlationId,
            DateTimeOffset timestamp,
            VersionChangeType changeType)
        {
            Assert.Throws<ArgumentNullException>("logger", () => _sut.BuildStoreDataSql(_testContext.Context, providerDefinitionId, containerName, data, _defaultKeyFields, StreamMode.Sync, correlationId, timestamp, changeType, null).ToList());
        }

        [Theory, InlineAutoData]
        public void BuildStoreDataSql_ValidData_IsSuccessful(
            string name,
            int field1,
            string field2,
            DateTime field3,
            decimal field4,
            bool field5,
            Guid providerDefinitionId,
            string correlationId,
            DateTimeOffset timestamp,
            VersionChangeType changeType)
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
            var result = _sut.BuildStoreDataSql(execContext, providerDefinitionId, name, data, _defaultKeyFields, StreamMode.Sync, correlationId, timestamp, changeType, _logger.Object);
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
           Guid providerDefinitionId,
           string correlationId,
           DateTimeOffset timestamp,
           VersionChangeType changeType)
        {
            var data = new Dictionary<string, object>
                        {
                             { "Field1", field1   },
                             { "Field2", field2   },
                             { "InvalidField", invalidField  }
                        };

            var execContext = _testContext.Context;
            var result = _sut.BuildStoreDataSql(execContext, providerDefinitionId, name, data, _defaultKeyFields, StreamMode.Sync, correlationId, timestamp, changeType, _logger.Object);
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
            Assert.Equal(paramsList[2].Value, JsonUtility.Serialize(data["InvalidField"]));
        }

        [Theory, InlineAutoData]
        public void BuildStoreDataSql_ValidDataWithCodes_IsSuccessful(
           string name,
           string originEntityCode,
           string additionalField,
           Guid providerDefinitionId,
           string correlationId,
           DateTimeOffset timestamp,
           VersionChangeType changeType)
        {
            var codes = new[] { "alpha", "beta", "gamma", "delta" };

            var data = new Dictionary<string, object>
            {
                    { "Codes", codes },
                    { "OriginEntityCode", originEntityCode },
                    { "AdditionalField", additionalField }
            };

            var keys = _defaultKeyFields;

            var execContext = _testContext.Context;
            var result = _sut.BuildStoreDataSql(execContext, providerDefinitionId, name, data, keys, StreamMode.Sync, correlationId, timestamp, changeType, _logger.Object).ToList();

            // codes inserts will delete from table first
            // then insert into codes
            // then main table insert
            var expectedCount = codes.Length + 2;
            Assert.Equal(expectedCount, result.Count());

            var deleteCodesCommand = result.First();
            Assert.Equal($"DELETE FROM {name}Codes WHERE [OriginEntityCode] = @OriginEntityCode;", deleteCodesCommand.Text.Trim());

            var deleteCodesParameters = deleteCodesCommand.Parameters.ToList();
            Assert.Single(deleteCodesParameters);
            Assert.Contains(deleteCodesParameters, p => p.ParameterName == "@OriginEntityCode" && (string)p.Value == originEntityCode);

            for (var x = 0; x < codes.Length; x++)
            {
                var code = codes[x];
                var codesCommand = result[x+1];
                Assert.Equal($"INSERT INTO [{name}Codes] ([OriginEntityCode],[Code]) values (@OriginEntityCode,@Code);", codesCommand.Text.Trim());

                var codesParameters = codesCommand.Parameters.ToList();
                Assert.Equal(2, codesParameters.Count());
                Assert.Contains(codesParameters, p => p.ParameterName == "@OriginEntityCode" && (string)p.Value == originEntityCode);
                Assert.Contains(codesParameters, p => p.ParameterName == "@Code" && (string)p.Value == code);
            }

            var mainTableCommand = result.Last();
            Assert.Equal($"MERGE [{name}] AS target" + Environment.NewLine +
                         "USING (SELECT @OriginEntityCode, @AdditionalField) AS source ([OriginEntityCode], [AdditionalField])" + Environment.NewLine +
                         "  ON (target.[OriginEntityCode] = source.[OriginEntityCode])" + Environment.NewLine +
                         "WHEN MATCHED THEN" + Environment.NewLine +
                         "  UPDATE SET target.[OriginEntityCode] = source.[OriginEntityCode], target.[AdditionalField] = source.[AdditionalField]" + Environment.NewLine +
                         "WHEN NOT MATCHED THEN" + Environment.NewLine +
                         "  INSERT ([OriginEntityCode], [AdditionalField])" + Environment.NewLine +
                         "  VALUES (source.[OriginEntityCode], source.[AdditionalField]);", mainTableCommand.Text.Trim());
            Assert.Equal(data.Count, mainTableCommand.Parameters.Count());

            var mainTableParameters = mainTableCommand.Parameters.ToList();
            Assert.Equal(2, mainTableParameters.Count());
            Assert.Contains(mainTableParameters, p => p.ParameterName == "@OriginEntityCode" && (string)p.Value == originEntityCode);
            Assert.Contains(mainTableParameters, p => p.ParameterName == "@AdditionalField" && (string)p.Value == additionalField);
        }

    }
}
