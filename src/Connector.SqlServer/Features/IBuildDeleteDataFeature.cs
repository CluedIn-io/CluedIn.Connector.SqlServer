using System;
using System.Collections.Generic;
using CluedIn.Connector.SqlServer.Connector;
using CluedIn.Connector.SqlServer.Utils;
using CluedIn.Core;
using CluedIn.Core.Data;
using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.SqlServer.Features
{
    public interface IBuildDeleteDataFeature
    {
        IEnumerable<SqlServerConnectorCommand> BuildDeleteDataSql(ExecutionContext executionContext,
            Guid providerDefinitionId,
            SqlTableName tableName,
            string originEntityCode,
            IList<IEntityCode> codes,
            Guid? entityId,
            ILogger logger);
    }
}
