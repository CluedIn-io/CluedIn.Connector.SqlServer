using CluedIn.Core;
using CluedIn.Connector.Common;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using CluedIn.Connector.Common.Configurations;
using CluedIn.Connector.SqlServer.Utils;
using System;
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
            new[] { SqlServerConstants.KeyName.Host, SqlServerConstants.KeyName.Schema, SqlServerConstants.KeyName.DatabaseName };

        public override string Schedule(DateTimeOffset relativeDateTime, bool webHooksEnabled)
            => $"{relativeDateTime.Minute} 0/23 * * *";

        public override Task<CrawlLimit> GetRemainingApiAllowance(ExecutionContext context, CrawlJobData jobData, Guid organizationId, Guid userId, Guid providerDefinitionId)
            => Task.FromResult(new CrawlLimit(-1, TimeSpan.Zero));

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
