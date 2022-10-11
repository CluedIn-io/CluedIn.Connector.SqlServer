using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using AutoFixture;
using AutoFixture.Xunit2;
using CluedIn.Connector.Common.Configurations;
using CluedIn.Core.Crawling;
using CluedIn.Core.Webhooks;
using FluentAssertions;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace CluedIn.Connector.SqlServer.Unit.Tests
{
    public class SqlServerConnectorProviderTests
    {
        private readonly TestContext _testContext;
        private readonly SqlServerConstants _constants;
        private readonly ILogger<SqlServerConnectorProvider> _logger;

        public SqlServerConnectorProviderTests()
        {
            _testContext = new TestContext();
            _constants = new SqlServerConstants();
            _logger = _testContext.Container.Resolve<ILogger<SqlServerConnectorProvider>>();
        }

        [Fact]
        public void Ctor_NullContext_Throws()
        {
            Action action = () => new SqlServerConnectorProvider(null, _constants, _logger);

            action.Should().Throw<ArgumentNullException>()
                .And.ParamName.Should().Be("appContext");
        }

        [Theory, AutoData]
        public async void GetCrawlJobData_NullContext_ReturnsGiven(Guid orgId, Guid userId, Guid providerDefId)
        {
            var sut = new SqlServerConnectorProvider(_testContext.AppContext.Object, _constants, _logger);

            var result = await sut.GetCrawlJobData(null, new Dictionary<string, object>()
            {
                { "alpha", "one" },
                { "beta", 2 }
            }, orgId, userId, providerDefId);

            result.Should().BeOfType<CrawlJobDataWrapper>();
            var typedResult = result as CrawlJobDataWrapper;
            typedResult.Configurations.Should().Equal(
                new Dictionary<string, object>
                {
                    { "alpha", "one" },
                    { "beta", 2 }
                });
        }

        [Theory, AutoData]
        public async void GetCrawlJobData_NullConfiguration_SetsNull(Guid orgId, Guid userId, Guid providerDefId)
        {
            var sut = new SqlServerConnectorProvider(_testContext.AppContext.Object, _constants, _logger);

            var result = await sut.GetCrawlJobData(_testContext.ProviderUpdateContext, null, orgId, userId, providerDefId);

            result.Should().BeOfType<CrawlJobDataWrapper>();
            var typedResult = result as CrawlJobDataWrapper;
            typedResult.Configurations.Should().BeNull();
        }

        [Theory, AutoData]
        public async void GetCrawlJobData_CamelCaseKeys_MatchesConstantsAndReturnsValues(Guid orgId, Guid userId, Guid providerDefId)
        {
            var sut = new SqlServerConnectorProvider(_testContext.AppContext.Object, _constants, _logger);
            var values = new Dictionary<string, object>
            {
                { "username", "user" },
                { "databaseName", "database" },
                { "host", "host" },
                { "password", "password" },
                { "portNumber", 1234 }
            };


            var result = await sut.GetCrawlJobData(_testContext.ProviderUpdateContext,
               values, orgId, userId, providerDefId);

            result.Should().BeOfType<CrawlJobDataWrapper>();
            var typedResult = result as CrawlJobDataWrapper;
            typedResult.Configurations.Should().Equal(
                new Dictionary<string, object>
                {
                    { KeyName.Username, "user" },
                    { KeyName.DatabaseName, "database" },
                    { KeyName.Host, "host" },
                    { KeyName.Password, "password" },
                    { KeyName.PortNumber, 1234 }
                });
        }

        [Theory, AutoData]
        public void TestAuthentication_Throws(Guid orgId, Guid userId, Guid providerDefId)
        {
            var sut = new SqlServerConnectorProvider(_testContext.AppContext.Object, _constants, _logger);

            Func<Task> action = () => sut.TestAuthentication(_testContext.ProviderUpdateContext, null, orgId, userId, providerDefId);

            action.Should().Throw<NotImplementedException>();
        }

        [Theory, AutoData]
        public void FetchUnSyncedEntityStatistics_Throws(Guid orgId, Guid userId, Guid providerDefId)
        {
            var sut = new SqlServerConnectorProvider(_testContext.AppContext.Object, _constants, _logger);

            Func<Task> action = () => sut.FetchUnSyncedEntityStatistics(_testContext.ProviderUpdateContext, null, orgId, userId, providerDefId);

            action.Should().Throw<NotImplementedException>();
        }

        [Theory, AutoData]
        public async void GetHelperConfiguration_NullContext_ReturnsEmpty(Guid orgId, Guid userId, Guid providerDefId)
        {
            var sut = new SqlServerConnectorProvider(_testContext.AppContext.Object, _constants, _logger);
            var data = new CrawlJobDataWrapper(new Dictionary<string, object>());


            var result = await sut.GetHelperConfiguration(null, data, orgId, userId, providerDefId);

            result.Should().BeEmpty();
        }

        [Theory, AutoData]
        public async void GetHelperConfiguration_NullData_Throws(Guid orgId, Guid userId, Guid providerDefId)
        {
            var sut = new SqlServerConnectorProvider(_testContext.AppContext.Object, _constants, _logger);

            var result = await sut.GetHelperConfiguration(_testContext.ProviderUpdateContext, null, orgId, userId, providerDefId);            

            result.Should().BeEmpty();
        }

        [Theory, AutoData]
        public async void GetHelperConfiguration_CamelCaseKeys_MatchesConstantsAndReturnsValues(Guid orgId, Guid userId, Guid providerDefId)
        {
            var sut = new SqlServerConnectorProvider(_testContext.AppContext.Object, _constants, _logger);
            var data = new CrawlJobDataWrapper(new Dictionary<string, object> {
                { "username", "user" },
                { "databaseName", "database" },
                { "host", "host" },
                { "password", "password" },
                { "portNumber", 1234 }
            });



            var result = await sut.GetHelperConfiguration(_testContext.ProviderUpdateContext, data, orgId, userId, providerDefId);

            result.Should().Equal(
                new Dictionary<string, object>
                {
                    { KeyName.Username, "user" },
                    { KeyName.DatabaseName, "database" },
                    { KeyName.Host, "host" },
                    { KeyName.Password, "password" },
                    { KeyName.PortNumber, 1234 }
                });
        }

        [Theory, AutoData]
        public void GetHelperConfigurationWithFolder_Throws_NotImplemented(Guid orgId, Guid userId, Guid providerDefId, string folderId)
        {
            var sut = new SqlServerConnectorProvider(_testContext.AppContext.Object, _constants, _logger);

            Func<Task> action = () => sut.GetHelperConfiguration(null, null, orgId, userId, providerDefId, folderId);

            action.Should().Throw<NotImplementedException>();
        }


        [Theory, AutoData]
        public void GetAccountInformation_NullData_Throws(Guid orgId, Guid userId, Guid providerDefId)
        {
            var sut = new SqlServerConnectorProvider(_testContext.AppContext.Object, _constants, _logger);

            Func<Task> action = () => sut.GetAccountInformation(_testContext.ProviderUpdateContext, null, orgId, userId, providerDefId);

            action.Should().Throw<ArgumentNullException>()
                .And.ParamName.Should().Be("jobData");
        }

        [Theory, AutoData]
        public void GetAccountInformation_InvalidJobDataType_Throws(Guid orgId, Guid userId, Guid providerDefId)
        {
            var sut = new SqlServerConnectorProvider(_testContext.AppContext.Object, _constants, _logger);

            Func<Task> action = () => sut.GetAccountInformation(
                _testContext.ProviderUpdateContext, new CrawlJobData(), orgId, userId, providerDefId);

            action.Should().Throw<ArgumentException>()
                .And.ParamName.Should().Be("jobData");

        }

        [Theory, AutoData]
        public async void GetAccountInformation_EmptyJobData_ReturnsEmpty(Guid orgId, Guid userId, Guid providerDefId)
        {
            var sut = new SqlServerConnectorProvider(_testContext.AppContext.Object, _constants, _logger);
            var data = new CrawlJobDataWrapper(new Dictionary<string, object>());

            var result = await sut.GetAccountInformation(
                _testContext.ProviderUpdateContext, data, orgId, userId, providerDefId);

            result.AccountId.Should().Be(".");
            result.AccountId.Should().Be(".");
        }

        [Theory, AutoData]
        public async void GetAccountInformation_WithJobData_ReturnsValue(Guid orgId, Guid userId, Guid providerDefId)
        {
            var sut = new SqlServerConnectorProvider(_testContext.AppContext.Object, _constants, _logger);
            var data = new CrawlJobDataWrapper(new Dictionary<string, object> {
                { "username", "user" },
                { "databaseName", "database" },
                { "host", "host" },
                { "password", "password" },
                { "portNumber", 1234 }
            });

            var result = await sut.GetAccountInformation(
                _testContext.ProviderUpdateContext, data, orgId, userId, providerDefId);

            result.AccountId.Should().Be("host.database");
            result.AccountId.Should().Be("host.database");
        }

        [Theory]
        [InlineAutoData(true)]
        [InlineAutoData(false)]
        public void Schedule_Returns_CronFormat(bool webhooksEnabled, DateTimeOffset dateTime)
        {
            var sut = new SqlServerConnectorProvider(_testContext.AppContext.Object, _constants, _logger);

            var result = sut.Schedule(dateTime, webhooksEnabled);

            result.Should().Be($"{dateTime.Minute} 0/23 * * *");
        }

        [Fact]
        public void CreateWebHook_Throws_NotImplemented()
        {
            var sut = new SqlServerConnectorProvider(_testContext.AppContext.Object, _constants, _logger);

            Func<Task> action = () => sut.CreateWebHook(null, new CrawlJobData(), Mock.Of<IWebhookDefinition>(), new Dictionary<string, object>());

            action.Should().Throw<NotImplementedException>();
        }

        [Fact]
        public void GetWebHooks_Throws_NotImplemented()
        {
            var sut = new SqlServerConnectorProvider(_testContext.AppContext.Object, _constants, _logger);

            Func<Task> action = () => sut.GetWebHooks(_testContext.ProviderUpdateContext);

            action.Should().Throw<NotImplementedException>();
        }

        [Fact]
        public void DeleteWebHook_Throws_NotImplemented()
        {
            var sut = new SqlServerConnectorProvider(_testContext.AppContext.Object, _constants, _logger);

            Func<Task> action = () => sut.DeleteWebHook(null, new CrawlJobData(), Mock.Of<IWebhookDefinition>());

            action.Should().Throw<NotImplementedException>();
        }

        [Fact]
        public void WebhookManagementEndpoints_Throws_NotImplemented()
        {
            var sut = new SqlServerConnectorProvider(_testContext.AppContext.Object, _constants, _logger);

            Action action = () => sut.WebhookManagementEndpoints(Array.Empty<string>());

            action.Should().Throw<NotImplementedException>();
        }

        [Theory, AutoData]
        public async void GetRemainingApiAllowance_WithJobData_Throws(Guid orgId, Guid userId, Guid providerDefId)
        {
            var sut = new SqlServerConnectorProvider(_testContext.AppContext.Object, _constants, _logger);

            var result = await sut.GetRemainingApiAllowance(null, new CrawlJobData(), orgId, userId, providerDefId);

            result.RemainingApiCalls.Should().Be(-1);
            result.TimeUntilNextAvailableCalls.Should().Be(TimeSpan.Zero);
        }

    }
}
