using CluedIn.Core;
using CluedIn.Connector.Common;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using CluedIn.Connector.Common.Configurations;
using System;
using System.Linq;
using System.Threading.Tasks;
using CluedIn.Core.Crawling;
using CluedIn.Core.Providers;

namespace CluedIn.Connector.SqlServer
{
    public class SqlServerConnectorProvider : ConnectorProviderBase<SqlServerConnectorProvider>
    {
        public SqlServerConnectorProvider([NotNull] ApplicationContext appContext, ISqlServerConstants configuration,
            ILogger<SqlServerConnectorProvider> logger)
            : base(appContext, configuration, logger)
        {
        }

        protected override IEnumerable<string> ProviderNameParts =>
            new[] { SqlServerConstants.KeyName.Host, SqlServerConstants.KeyName.DatabaseName };

        public override string Schedule(DateTimeOffset relativeDateTime, bool webHooksEnabled)
            => $"{relativeDateTime.Minute} 0/23 * * *";

        public override Task<CrawlLimit> GetRemainingApiAllowance(ExecutionContext context, CrawlJobData jobData, Guid organizationId, Guid userId, Guid providerDefinitionId)
            => Task.FromResult(new CrawlLimit(-1, TimeSpan.Zero));

        public override Task<AccountInformation> GetAccountInformation(ExecutionContext context, CrawlJobData jobData, Guid organizationId, Guid userId, Guid providerDefinitionId)
        {
            // base class does not map to original spec of this connector with dot-seperated values

            if (jobData == null)
            {
                throw new ArgumentNullException(nameof(jobData));
            }

            if (!(jobData is CrawlJobDataWrapper dataWrapper))
            {
                throw new ArgumentException(
                    "Wrong CrawlJobData type", nameof(jobData));
            }

            var partsFound = new List<string>();
            foreach (var key in ProviderNameParts)
                if (dataWrapper.Configurations.TryGetValue(key, out var value) && value != null)
                    partsFound.Add(value.ToString());

            var account = string.Join('.', partsFound);
            if (string.IsNullOrEmpty(account))
                account = ".";

            return Task.FromResult(new AccountInformation(account, account));


        }
    }
}
