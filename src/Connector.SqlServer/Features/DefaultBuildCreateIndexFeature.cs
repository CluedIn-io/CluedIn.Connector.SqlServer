using CluedIn.Connector.SqlServer.Connector;
using CluedIn.Connector.SqlServer.Utils;
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
            SqlTableName tableName,
            IEnumerable<string> keys,
            ILogger logger,
            bool useUniqueIndex)
        {
            if (executionContext == null)
                throw new ArgumentNullException(nameof(executionContext));

            if (logger == null)
                throw new ArgumentNullException(nameof(logger));

            var createIndexCommandText = $"CREATE {(useUniqueIndex ? "UNIQUE" : string.Empty)} INDEX [idx_{tableName.Schema}_{tableName.LocalName}] ON {tableName.FullyQualifiedName}({string.Join(", ", keys)}); ";

            return new[] { new SqlServerConnectorCommand { Text = createIndexCommandText } };
        }
    }
}
