using System;
using System.Collections.Generic;
using CluedIn.Connector.SqlServer.Connector;
using CluedIn.Core;
using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.SqlServer.Features
{
    public interface IBuildDeleteDataFeature
    {
        IEnumerable<SqlServerConnectorCommand> BuildDeleteDataSql(ExecutionContext executionContext,
            Guid providerDefinitionId,
            string containerName,
            Guid entityId,
            ILogger logger);
    }
}
