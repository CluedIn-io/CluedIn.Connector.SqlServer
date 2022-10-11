using CluedIn.Connector.SqlServer.Connector;
using CluedIn.Connector.SqlServer.Utility;
using CluedIn.Core;
using CluedIn.Core.Data.Parts;
using CluedIn.Core.Streams.Models;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;

namespace CluedIn.Connector.SqlServer.Features
{
    public interface IBuildStoreDataForMode
    {
        IEnumerable<SqlServerConnectorCommand> BuildStoreDataSql(ExecutionContext executionContext,
            Guid providerDefinitionId,
            SanitizedSqlString schema,
            SanitizedSqlString tableName,
            IDictionary<string, object> data,
            IList<string> keys,
            StreamMode mode,
            string correlationId,
            DateTimeOffset timestamp,
            VersionChangeType changeType,
            ILogger logger);
    }
}
