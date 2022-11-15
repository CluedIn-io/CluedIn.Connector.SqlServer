using CluedIn.Connector.Common.Helpers;
using CluedIn.Connector.SqlServer.Connector;
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

            //LDM. Added here for fast track. But properly would be to manage it as method parameter.
            var createNonUniqueIndex = sanitizedName.EndsWith("Codes") || sanitizedName.EndsWith("Edges");

            var createIndexCommandText = $"CREATE {(createNonUniqueIndex ? string.Empty : "UNIQUE")} INDEX [idx_{sanitizedName}] ON [{sanitizedName}]({string.Join(", ", keys)}); ";

            return new[] { new SqlServerConnectorCommand { Text = createIndexCommandText } };
        }
    }
}
