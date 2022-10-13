using AutoFixture.Xunit2;
using CluedIn.Connector.SqlServer.Features;
using CluedIn.Core.Connectors;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Xunit;

namespace CluedIn.Connector.SqlServer.Unit.Tests.Features
{
    public class DefaultBuildCreateContainerFeatureTests : FeatureTestsBase
    {
        private readonly DefaultBuildCreateContainerFeature _sut;
        private readonly IList<string> _defaultKeyFields;

        public DefaultBuildCreateContainerFeatureTests()
        {
            _sut = new DefaultBuildCreateContainerFeature();
            _defaultKeyFields = new List<string> { "OriginEntityCode" };
        }

        [Theory, InlineAutoData]
        public void BuildCreateContainerSql_NullContext_Throws(
            Guid providerDefinitionId,
            string schema,
            string name,
            List<ConnectionDataType> columns)
        {
            Assert.Throws<ArgumentNullException>("executionContext", () => _sut.BuildCreateContainerSql(null, providerDefinitionId, ToSanitized(schema), ToSanitized(name), columns, _defaultKeyFields, _logger.Object));
        }

        [Theory]
        [InlineAutoData("")]
        [InlineAutoData("\t\t   ")]
        public void BuildCreateContainerSql_InvalidContainerName_Throws(
            string name,
            string schema,
            Guid providerDefinitionId,
            List<ConnectionDataType> columns)
        {
            Assert.Throws<InvalidOperationException>(() => _sut.BuildCreateContainerSql(_testContext.Context, providerDefinitionId, ToSanitized(schema), ToSanitized(name), columns, _defaultKeyFields, _logger.Object));
        }

        [Theory, InlineAutoData]
        public void BuildCreateContainerSql_NullColumns_Throws(
            Guid providerDefinitionId,
            string schema,
            string name)
        {
            Assert.Throws<InvalidOperationException>(() => _sut.BuildCreateContainerSql(_testContext.Context, providerDefinitionId, ToSanitized(schema), ToSanitized(name), null, _defaultKeyFields, _logger.Object));
        }

        [Theory, InlineAutoData]
        public void BuildCreateContainerSql_EmptyColumns_Throws(
            Guid providerDefinitionId,
            string schema,
            string name)
        {
            Assert.Throws<InvalidOperationException>(() => _sut.BuildCreateContainerSql(_testContext.Context, providerDefinitionId, ToSanitized(schema), ToSanitized(name), new List<ConnectionDataType>(), _defaultKeyFields, _logger.Object));
        }

        [Theory, InlineAutoData]
        public void BuildCreateContainerSql_NullLogger_Throws(
            Guid providerDefinitionId,
            string schema,
            string name,
            List<ConnectionDataType> columns)
        {
            Assert.Throws<ArgumentNullException>("logger", () => _sut.BuildCreateContainerSql(_testContext.Context, providerDefinitionId, ToSanitized(schema), ToSanitized(name), columns, _defaultKeyFields, null));
        }

        [Theory, InlineAutoData]
        public void BuildCreateContainerSql_ValidColumns_IsSuccessful(
            Guid providerDefinitionId,
            string schema,
            string name,
            List<ConnectionDataType> columns)
        {
            var sanitizedSchema = ToSanitized(schema);
            var sanitizedName = ToSanitized(name);
            var expected = new StringBuilder();
            expected.AppendLine($"CREATE TABLE [{sanitizedSchema}].[{sanitizedName}] (");
            expected.AppendJoin(", ", columns.Select(c => $"[{ToSanitized(c.Name)}] nvarchar(max) NULL"));
            expected.AppendLine(") ON[PRIMARY]");

            var execContext = _testContext.Context;
            var result = _sut.BuildCreateContainerSql(execContext, providerDefinitionId, sanitizedSchema, sanitizedName, columns, _defaultKeyFields, _logger.Object);
            var command = result.Single();
            Assert.Equal(expected.ToString(), command.Text);
            Assert.Null(command.Parameters);
        }
    }
}
