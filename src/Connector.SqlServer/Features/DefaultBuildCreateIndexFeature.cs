using CluedIn.Connector.Common.Helpers;
using CluedIn.Connector.SqlServer.Connector;
using CluedIn.Core;
using Microsoft.EntityFrameworkCore.Internal;
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
            IEnumerable<(string[] keys, string[] includes)> surrogateKeys,
            ILogger logger)
        {
            if (executionContext == null)
                throw new ArgumentNullException(nameof(executionContext));

            if (string.IsNullOrWhiteSpace(containerName))
                throw new InvalidOperationException("The Container Name must be provided.");

            if (logger == null)
                throw new ArgumentNullException(nameof(logger));

            var commands = new List<SqlServerConnectorCommand>();

            var sanitizedName = SqlStringSanitizer.Sanitize(containerName);
            var createIndexCommandText = $"CREATE INDEX [idx_{sanitizedName}] ON [{sanitizedName}]({string.Join(", ", keys)}); ";

            commands.Add(new SqlServerConnectorCommand { Text = createIndexCommandText });

            foreach (var surrogateKey in surrogateKeys.SafeEnumerate())
            {
                var sql = $"CREATE INDEX [idx_{sanitizedName}2] ON [{sanitizedName}]({string.Join(", ", surrogateKey.keys)}) ";
            
                if (surrogateKey.includes.SafeEnumerate().Any())
                {
                    sql += $" INCLUDE({string.Join(", ", surrogateKey.includes)})";
                }
            
                commands.Add(new SqlServerConnectorCommand { Text = sql });
            }

            return commands.ToArray();
        }
    }
}
