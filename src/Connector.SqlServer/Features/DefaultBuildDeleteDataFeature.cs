using System;
using System.Collections.Generic;
using CluedIn.Connector.SqlServer.Connector;
using CluedIn.Core;
using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.SqlServer.Features
{
    public class DefaultBuildDeleteDataFeature : IBuildDeleteDataFeature
    {
        public IEnumerable<SqlServerConnectorCommand> BuildDeleteDataSql(ExecutionContext executionContext, Guid providerDefinitionId, string containerName, string originEntityCode, ILogger logger)
        {
            return default;
        }
    }
}
