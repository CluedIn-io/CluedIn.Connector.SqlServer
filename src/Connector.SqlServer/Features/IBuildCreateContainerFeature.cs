using System;
using System.Collections.Generic;
using CluedIn.Connector.SqlServer.Connector;
using CluedIn.Connector.SqlServer.Utility;
using CluedIn.Core;
using CluedIn.Core.Connectors;
using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.SqlServer.Features
{
    public interface IBuildCreateContainerFeature
    {
        IEnumerable<SqlServerConnectorCommand> BuildCreateContainerSql(ExecutionContext executionContext,
            Guid providerDefinitionId,
            SanitizedSqlName schema,
            SanitizedSqlName tableName,
            IEnumerable<ConnectionDataType> columns,
            IEnumerable<string> keys,
            ILogger logger);
    }
}
