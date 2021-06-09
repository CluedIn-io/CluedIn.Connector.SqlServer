using System;
using System.Threading.Tasks;
using CluedIn.Connector.SqlServer.Connector;
using CluedIn.Core;
using CluedIn.Core.Connectors;
using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.SqlServer.Features
{
    public interface IBulkDeleteDataFeature
    {
        Task BulkTableDelete(ExecutionContext executionContext,
            Guid providerDefinitionId,
            string containerName,
            Guid entityId,
            int threshold,
            IBulkSqlClient client,
            Func<Task<IConnectorConnection>> connectionFactory,
            ILogger logger);
    }
}
