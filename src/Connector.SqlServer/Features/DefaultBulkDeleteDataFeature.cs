﻿using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using CluedIn.Connector.SqlServer.Connector;
using CluedIn.Core;
using CluedIn.Core.Connectors;
using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.SqlServer.Features
{
    public class DefaultBulkDeleteDataFeature : IBulkDeleteDataFeature
    {
        public Task BulkTableDelete(
            ExecutionContext executionContext,
            Guid providerDefinitionId,
            string containerName,
            IDictionary<string, object> data,
            int threshold,
            IBulkSqlClient client,
            Func<Task<IConnectorConnection>> connectionFactory,
            ILogger logger)
        {
            return Task.CompletedTask;
        }
    }
}
