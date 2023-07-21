using CluedIn.Core;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using CluedIn.Connector.SqlServer.Utils;
using System;
using System.Threading.Tasks;
using CluedIn.Core.Crawling;
using CluedIn.Core.Data.Relational;
using CluedIn.Core.Providers;
using CluedIn.Core.Webhooks;
using CluedIn.Providers.Models;
using Newtonsoft.Json;

namespace CluedIn.Connector.SqlServer
{
    public class SqlServerConnectorProvider : ProviderBase, IExtendedProviderMetadata
    {
        private readonly ISqlServerConstants _configuration;
        private readonly ILogger<SqlServerConnectorProvider> _logger;

        public SqlServerConnectorProvider([NotNull] ApplicationContext appContext, ISqlServerConstants configuration,
            ILogger<SqlServerConnectorProvider> logger)
            : base(appContext, configuration.CreateProviderMetadata())
        {
            _configuration = configuration;
            _logger = logger;
        }

        protected IEnumerable<string> ProviderNameParts => new[] { SqlServerConstants.KeyName.Host, SqlServerConstants.KeyName.Schema, SqlServerConstants.KeyName.DatabaseName };

        public string Icon => _configuration.Icon;
        public string Domain => _configuration.Domain;
        public string About => _configuration.About;
        public AuthMethods AuthMethods => _configuration.AuthMethods;
        public IEnumerable<Control> Properties => _configuration.Properties;
        public Guide Guide => _configuration.Guide;
        public new IntegrationType Type => _configuration.Type;

        public override string Schedule(DateTimeOffset relativeDateTime, bool webHooksEnabled)
            => $"{relativeDateTime.Minute} 0/23 * * *";

        public override Task<IEnumerable<WebHookSignature>> CreateWebHook(ExecutionContext context, CrawlJobData jobData, IWebhookDefinition webhookDefinition,
            IDictionary<string, object> config)
        {
            throw new NotImplementedException();
        }

        public override Task<IEnumerable<WebhookDefinition>> GetWebHooks(ExecutionContext context)
        {
            throw new NotImplementedException();
        }

        public override Task DeleteWebHook(ExecutionContext context, CrawlJobData jobData, IWebhookDefinition webhookDefinition)
        {
            throw new NotImplementedException();
        }

        public override Task<CrawlLimit> GetRemainingApiAllowance(ExecutionContext context, CrawlJobData jobData, Guid organizationId, Guid userId, Guid providerDefinitionId)
            => Task.FromResult(new CrawlLimit(-1, TimeSpan.Zero));

        public override IEnumerable<string> WebhookManagementEndpoints(IEnumerable<string> ids)
        {
            throw new NotImplementedException();
        }

        public override Task<CrawlJobData> GetCrawlJobData(ProviderUpdateContext context, IDictionary<string, object> configuration, Guid organizationId, Guid userId,
            Guid providerDefinitionId)
        {
            // WARNING: The log output can contain sensitive information
            _logger.LogDebug($"GetCrawlJobData config input: {JsonConvert.SerializeObject(configuration)}");
            return Task.FromResult<CrawlJobData>(new CrawlJobDataWrapper(configuration));
        }

        public override Task<bool> TestAuthentication(ProviderUpdateContext context, IDictionary<string, object> configuration, Guid organizationId, Guid userId,
            Guid providerDefinitionId)
        {
            throw new NotImplementedException();
        }

        public override Task<ExpectedStatistics> FetchUnSyncedEntityStatistics(ExecutionContext context, IDictionary<string, object> configuration, Guid organizationId,
            Guid userId, Guid providerDefinitionId)
        {
            throw new NotImplementedException();
        }

        public override Task<IDictionary<string, object>> GetHelperConfiguration(ProviderUpdateContext context, CrawlJobData jobData, Guid organizationId, Guid userId, Guid providerDefinitionId)
        {
            _logger.LogDebug($"GetHelperConfiguration CrawlJobData input: {JsonConvert.SerializeObject(jobData)}");

            if (jobData is CrawlJobDataWrapper dataWrapper)
                return Task.FromResult(dataWrapper.Configurations);

            return Task.FromResult<IDictionary<string, object>>(new Dictionary<string, object>());
        }

        public override Task<IDictionary<string, object>> GetHelperConfiguration(ProviderUpdateContext context, CrawlJobData jobData, Guid organizationId, Guid userId,
            Guid providerDefinitionId, string folderId)
        {
            throw new NotImplementedException();
        }

        public override Task<AccountInformation> GetAccountInformation(ExecutionContext context, CrawlJobData jobData, Guid organizationId, Guid userId, Guid providerDefinitionId)
        {
            // base class does not map to original spec of this connector with dot-separated values

            if (jobData == null) throw new ArgumentNullException(nameof(jobData));

            if (!(jobData is CrawlJobDataWrapper dataWrapper))
            {
                throw new ArgumentException("Wrong CrawlJobData type", nameof(jobData));
            }

            var partsFound = new List<string>();
            foreach (var key in ProviderNameParts)
            {
                if (dataWrapper.Configurations.TryGetValue(key, out var value) && value != null)
                {
                    // Do not add schema name if it's the default. So account would look `localhost.ExportTarget2` instead of `localhost.dbo.ExportTarget2`
                    // 
                    // The primary reason is backward compatibility. CluedIn does not create multiple accounts pointing to the same object.
                    // Before schema support introduction all accounts did not contain schema, so if we change the rule, it will be possible to
                    // create new export targets pointing to same database as the existing ones (as the name will differ due to a presence of the schema in it).
                    // We solve that if we don't embed default schema, so it will be reduced to the legacy behavior for the default schema.
                    //
                    // Also it looks nicer in majority of cases when you don't specify the schema :)
                    if (SqlServerConstants.KeyName.Schema.Equals(key, StringComparison.Ordinal) && SqlTableName.DefaultSchema.Equals(value))
                    {
                        continue;
                    }

                    partsFound.Add(value.ToString());
                }
            }

            var account = string.Join('.', partsFound);
            if (string.IsNullOrEmpty(account))
            {
                account = ".";
            }

            return Task.FromResult(new AccountInformation(account, account));
        }
    }
}
