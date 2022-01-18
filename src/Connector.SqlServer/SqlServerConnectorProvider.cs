using CluedIn.Core;
using CluedIn.Connector.Common;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using CluedIn.Connector.Common.Configurations;
using System;
using System.Threading.Tasks;
using CluedIn.Core.Crawling;

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
            new[] { CommonConfigurationNames.Host, CommonConfigurationNames.DatabaseName };

        public override string Schedule(DateTimeOffset relativeDateTime, bool webHooksEnabled)
            => $"{relativeDateTime.Minute} 0/23 * * *";

        public override Task<CrawlLimit> GetRemainingApiAllowance(ExecutionContext context, CrawlJobData jobData, Guid organizationId, Guid userId, Guid providerDefinitionId)
            => Task.FromResult(new CrawlLimit(-1, TimeSpan.Zero));
    }
}
