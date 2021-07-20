using System;
using System.Collections.Generic;
using CluedIn.Connector.SqlServer.Connector;
using CluedIn.Core;
using CluedIn.Core.Data.Parts;
using CluedIn.Core.Streams.Models;
using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.SqlServer.Features
{
    public interface IBuildStoreDataForMode
    {
        IEnumerable<SqlServerConnectorCommand> BuildStoreDataSql(ExecutionContext executionContext,
            Guid providerDefinitionId,
            string containerName,
            IDictionary<string, object> data,
            IList<string> keys,
            StreamMode mode,
            string correlationId,
            DateTimeOffset timestamp,
            VersionChangeType changeType,
            ILogger logger);
    }
}
