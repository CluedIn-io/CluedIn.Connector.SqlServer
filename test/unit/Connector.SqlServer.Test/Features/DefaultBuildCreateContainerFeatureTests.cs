using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using AutoFixture.Xunit2;
using CluedIn.Connector.SqlServer.Features;
using CluedIn.Core.Connectors;
using CluedIn.Core.Data.Vocabularies;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;
using Xunit.Abstractions;

namespace CluedIn.Connector.SqlServer.Unit.Tests.Features
{
    public class DefaultBuildCreateContainerFeatureTests
    {
        private readonly TestContext _testContext;
        private readonly Mock<ILogger> _logger;
        private readonly DefaultBuildCreateContainerFeature _sut;
        private readonly ConnectionDataType[] _defaultColumns;
        private readonly IList<string> _defaultKeyFields = new List<string> { "OriginEntityCode" };

        public DefaultBuildCreateContainerFeatureTests()
        {
            _testContext = new TestContext();
            _logger = new Mock<ILogger>();
            _sut = new DefaultBuildCreateContainerFeature();
            _defaultColumns = new[]
            {
                new ConnectionDataType
                {
                    Name = "TEST",
                    Type = VocabularyKeyDataType.Text
                }
            };
        }

        [Theory, InlineAutoData]
        public void BuildCreateContainerSql_NullContext_Throws(
            Guid providerDefinitionId,
            string name,
            List<ConnectionDataType> columns)
        {
            Assert.Throws<ArgumentNullException>("executionContext", () => _sut.BuildCreateContainerSql(null, providerDefinitionId, name, columns, _defaultKeyFields, _logger.Object));
        }

        [Theory]
        [InlineAutoData(null)]
        [InlineAutoData("")]
        [InlineAutoData("\t\t   ")]
        public void BuildCreateContainerSql_InvalidContainerName_Throws(
            string name,
            Guid providerDefinitionId,
            List<ConnectionDataType> columns)
        {
            Assert.Throws<InvalidOperationException>(() => _sut.BuildCreateContainerSql(_testContext.Context, providerDefinitionId, name, columns, _defaultKeyFields, _logger.Object));
        }

        [Theory, InlineAutoData]
        public void BuildCreateContainerSql_NullColumns_Throws(
            Guid providerDefinitionId,
            string name)
        {
            Assert.Throws<InvalidOperationException>(() => _sut.BuildCreateContainerSql(_testContext.Context, providerDefinitionId, name, null, _defaultKeyFields, _logger.Object));
        }

        [Theory, InlineAutoData]
        public void BuildCreateContainerSql_EmptyColumns_Throws(
            Guid providerDefinitionId,
            string name)
        {
            Assert.Throws<InvalidOperationException>(() => _sut.BuildCreateContainerSql(_testContext.Context, providerDefinitionId, name, new List<ConnectionDataType>(), _defaultKeyFields, _logger.Object));
        }

        [Theory, InlineAutoData]
        public void BuildCreateContainerSql_NullLogger_Throws(
            Guid providerDefinitionId,
            string name,
            List<ConnectionDataType> columns)
        {
            Assert.Throws<ArgumentNullException>("logger", () => _sut.BuildCreateContainerSql(_testContext.Context, providerDefinitionId, name, columns, _defaultKeyFields, null));
        }

        [Theory, InlineAutoData]
        public void BuildCreateContainerSql_ValidColumns_IsSuccessful(
            Guid providerDefinitionId,
            string name,
            List<ConnectionDataType> columns)
        {
            var expected = new StringBuilder();
            expected.AppendLine($"CREATE TABLE [{name.SqlSanitize()}](");
            expected.AppendJoin(", ", columns.Select(c => $"[{c.Name.SqlSanitize()}] nvarchar(100) NULL"));
            expected.AppendLine(") ON[PRIMARY]");

            var execContext = _testContext.Context;
            var result = _sut.BuildCreateContainerSql(execContext, providerDefinitionId, name, columns, _defaultKeyFields, _logger.Object);
            var command = result.Single();
            Assert.Equal(expected.ToString(), command.Text);
            Assert.Null(command.Parameters);
        }
    }
}
