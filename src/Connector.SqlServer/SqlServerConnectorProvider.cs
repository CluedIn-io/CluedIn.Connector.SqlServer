using CluedIn.Core;
using CluedIn.Connector.Common;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using CluedIn.Connector.Common.Configurations;

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
    }
}
