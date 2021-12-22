using CluedIn.Connector.SqlServer.Connector;
using CluedIn.Core;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Text;

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

            var builder = new StringBuilder();
            var sanitizedName = containerName.SqlSanitize();
            var indexName = $"idx_{sanitizedName}".SqlSanitize();

            builder.AppendLine($"CREATE INDEX [{indexName}] ON [{sanitizedName}]({string.Join(", ", keys)}); ");

            return new[] { new SqlServerConnectorCommand { Text = builder.ToString() } };
        }
    }
}
