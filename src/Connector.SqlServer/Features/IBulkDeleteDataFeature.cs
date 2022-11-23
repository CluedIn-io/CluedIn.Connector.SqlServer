using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using CluedIn.Connector.SqlServer.Connector;
using CluedIn.Connector.SqlServer.Utils;
using CluedIn.Core;
using CluedIn.Core.Connectors;
using CluedIn.Core.Data;
using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.SqlServer.Features
{
    public interface IBulkDeleteDataFeature
    {
        Task BulkTableDelete(ExecutionContext executionContext,
            Guid providerDefinitionId,
            SqlTableName tableName,
            string originEntityCode,
            IList<IEntityCode> codes,
            Guid entityId,
            int threshold,
            IBulkSqlClient client,
            IConnectorConnection config,
            ILogger logger);
    }
}
