using CluedIn.Connector.Common.Helpers;
using CluedIn.Connector.SqlServer.Connector;
using CluedIn.Core;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;

namespace CluedIn.Connector.SqlServer.Features
{
    public class DefaultBuildCreateIndexFeature : IBuildCreateIndexFeature
    {
        public virtual IEnumerable<SqlServerConnectorCommand> BuildCreateIndexSql(
            ExecutionContext executionContext,
            Guid providerDefinitionId,
            string containerName,
            IEnumerable<string> keys,
            ILogger logger)
        {
            if (executionContext == null)
                throw new ArgumentNullException(nameof(executionContext));

            if (string.IsNullOrWhiteSpace(containerName))
                throw new InvalidOperationException("The Container Name must be provided.");

            if (logger == null)
                throw new ArgumentNullException(nameof(logger));

            var sanitizedName = SqlStringSanitizer.Sanitize(containerName);
            var createIndexCommandText = $"CREATE INDEX [idx_{sanitizedName}] ON [{sanitizedName}]({string.Join(", ", keys)}); ";


            return new[] { new SqlServerConnectorCommand { Text = createIndexCommandText } };
        }
    }
}
