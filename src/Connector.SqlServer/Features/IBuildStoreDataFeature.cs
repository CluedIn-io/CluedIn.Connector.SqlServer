using System;
using System.Collections.Generic;
using CluedIn.Connector.SqlServer.Connector;
using CluedIn.Connector.SqlServer.Utils;
using CluedIn.Core;
using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.SqlServer.Features
{
    public interface IBuildStoreDataFeature
    {
        IEnumerable<SqlServerConnectorCommand> BuildStoreDataSql(ExecutionContext executionContext,
            Guid providerDefinitionId,
            SqlTableName tableName,
            IDictionary<string, object> data,
            IList<string> uniqueColumns,
            ILogger logger);
    }
}
