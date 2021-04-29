using System;
using System.Collections.Generic;
using CluedIn.Connector.SqlServer.Connector;
using CluedIn.Core;

namespace CluedIn.Connector.SqlServer.Features
{
    public interface IBuildStoreDataFeature
    {
        IEnumerable<SqlServerConnectorCommand> BuildStoreDataSql(
            ExecutionContext executionContext,
            Guid providerDefinitionId,
            string containerName,
            IDictionary<string, object> data);
    }
}
