using CluedIn.Connector.SqlServer.Connector;
using CluedIn.Connector.SqlServer.Utility;
using CluedIn.Core;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;

namespace CluedIn.Connector.SqlServer.Features
{
    public interface IBuildStoreDataFeature
    {
        IEnumerable<SqlServerConnectorCommand> BuildStoreDataSql(ExecutionContext executionContext,
            Guid providerDefinitionId,
            SanitizedSqlName schema,
            SanitizedSqlName tableName,
            IDictionary<string, object> data,
            IList<string> keys,
            ILogger logger);
    }
}
