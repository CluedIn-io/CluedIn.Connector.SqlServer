using System;
using System.Collections.Generic;
using CluedIn.Connector.SqlServer.Connector;
using CluedIn.Connector.SqlServer.Utility;
using CluedIn.Core;
using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.SqlServer.Features
{
    public interface IBuildCreateIndexFeature
    {
        IEnumerable<SqlServerConnectorCommand> BuildCreateIndexSql(ExecutionContext executionContext,
            Guid providerDefinitionId,
            SanitizedSqlString tableName,
            IEnumerable<string> keys,
            ILogger logger);
    }
}
