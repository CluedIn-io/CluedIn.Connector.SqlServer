using CluedIn.Connector.SqlServer.Connector;
using CluedIn.Connector.SqlServer.Utility;
using CluedIn.Core;
using CluedIn.Core.Connectors;
using CluedIn.Core.Data;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace CluedIn.Connector.SqlServer.Features
{
    public class DefaultBulkDeleteDataFeature : IBulkDeleteDataFeature
    {
        public Task BulkTableDelete(
            ExecutionContext executionContext,
            Guid providerDefinitionId,
            SanitizedSqlString tableName,
            string originEntityCode,
            IList<IEntityCode> codes,
            Guid entityId,
            int threshold,
            IBulkSqlClient client,
            Func<Task<IConnectorConnection>> connectionFactory,
            ILogger logger)
        {
            return Task.CompletedTask;
        }
    }
}
