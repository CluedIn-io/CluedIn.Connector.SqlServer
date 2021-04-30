using System;
using System.Collections.Generic;
using CluedIn.Connector.SqlServer.Connector;
using CluedIn.Core;
using CluedIn.Core.Connectors;
using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.SqlServer.Features
{
    public interface IBuildCreateContainerFeature
    {
        IEnumerable<SqlServerConnectorCommand> BuildCreateContainerSql(
            ExecutionContext executionContext,
            Guid providerDefinitionId,
            string name,
            IEnumerable<ConnectionDataType> columns,
            ILogger logger);
    }
}
