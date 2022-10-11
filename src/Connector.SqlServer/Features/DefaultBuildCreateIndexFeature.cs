using CluedIn.Connector.Common.Helpers;
using CluedIn.Connector.SqlServer.Connector;
using CluedIn.Connector.SqlServer.Utility;
using CluedIn.Core;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;

namespace CluedIn.Connector.SqlServer.Features
{
    public class DefaultBuildCreateIndexFeature : IBuildCreateIndexFeature
    {
        public virtual IEnumerable<SqlServerConnectorCommand> BuildCreateIndexSql(
            ExecutionContext executionContext,
            Guid providerDefinitionId,
            SanitizedSqlString schema,
            SanitizedSqlString tableName,
            IEnumerable<string> keys,
            ILogger logger)
        {
            if (executionContext == null)
                throw new ArgumentNullException(nameof(executionContext));

            if (string.IsNullOrWhiteSpace(tableName.GetValue()))
                throw new InvalidOperationException("The Container Name must be provided.");

            if (logger == null)
                throw new ArgumentNullException(nameof(logger));
            
            var createIndexCommandText = $"CREATE INDEX [idx_{tableName}] ON {schema}.{tableName} ({string.Join(", ", keys)}); ";

            return new[] { new SqlServerConnectorCommand { Text = createIndexCommandText } };
        }
    }
}
