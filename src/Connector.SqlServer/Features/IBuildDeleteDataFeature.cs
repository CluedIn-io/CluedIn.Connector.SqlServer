using System;
using System.Collections.Generic;
using CluedIn.Connector.SqlServer.Connector;
using CluedIn.Connector.SqlServer.Utility;
using CluedIn.Core;
using CluedIn.Core.Data;
using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.SqlServer.Features
{
    public interface IBuildDeleteDataFeature
    {
        IEnumerable<SqlServerConnectorCommand> BuildDeleteDataSql(ExecutionContext executionContext,
            Guid providerDefinitionId,
            SanitizedSqlString tableName,
            string originEntityCode,
            IList<IEntityCode> codes,
            Guid? entityId,
            ILogger logger);
    }
}
