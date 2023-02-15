using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using CluedIn.Connector.SqlServer.Connector;
using CluedIn.Connector.SqlServer.Utils;
using CluedIn.Core;
using CluedIn.Core.Connectors;
using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.SqlServer.Features
{
    public interface IBulkStoreDataFeature
    {
        Task BulkTableUpdate(
            ExecutionContext executionContext,
            Guid providerDefinitionId,
            SqlTableName tableName,
            IDictionary<string, object> data,
            int threshold,
            IBulkSqlClient client,
            IConnectorConnection config,
            ILogger logger);
    }
}
