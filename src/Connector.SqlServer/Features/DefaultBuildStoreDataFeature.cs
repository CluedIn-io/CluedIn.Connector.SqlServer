using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using CluedIn.Connector.SqlServer.Connector;
using CluedIn.Core;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.SqlServer.Features
{
    public class DefaultBuildStoreDataFeature : IBuildStoreDataFeature
    {
        public virtual IEnumerable<SqlServerConnectorCommand> BuildStoreDataSql(
            ExecutionContext executionContext,
            Guid providerDefinitionId,
            string containerName,
            IDictionary<string, object> data,
            ILogger logger)
        {
            if (executionContext == null)
                throw new ArgumentNullException(nameof(executionContext));

            if (string.IsNullOrWhiteSpace(containerName))
                throw new InvalidOperationException("The containerName must be provided.");

            if (data == null || data.Count == 0)
                throw new InvalidOperationException("The data to specify columns must be provided.");

            if (logger == null)
                throw new ArgumentNullException(nameof(logger));

            var builder = new StringBuilder();
            var parameters = new List<SqlParameter>();
            var fields = new List<string>();
            var inserts = new List<string>();
            var updates = new List<string>();
            foreach (var entry in data)
            {
                var name = entry.Key.SqlSanitize();
                var param = new SqlParameter($"@{name}", entry.Value ?? string.Empty);
                try
                {
                    var dbType = param.DbType;
                    logger.LogDebug("Adding [{field}] as a sql type [{sqlType}].", name, dbType);
                    parameters.Add(param);
                    fields.Add($"[{name}]");
                    inserts.Add($"source.[{name}]");
                    updates.Add($"target.[{name}] = source.[{name}]");
                }
                catch (Exception ex)
                {
                    logger.LogWarning(ex, "Could not store [{field}] as a sql type could not be mapped.", name);
                }
            }

            var fieldsString = string.Join(", ", fields);
            if (string.IsNullOrWhiteSpace(fieldsString))
                throw new InvalidOperationException($"No fields could be mapped to sql types for table [{containerName}]");
            
            builder.AppendLine($"MERGE [{containerName.SqlSanitize()}] AS target");
            builder.AppendLine($"USING (SELECT {string.Join(", ", parameters.Select(x => x.ParameterName))}) AS source ({fieldsString})");
            builder.AppendLine("  ON (target.[OriginEntityCode] = source.[OriginEntityCode])");
            builder.AppendLine("WHEN MATCHED THEN");
            builder.AppendLine($"  UPDATE SET {string.Join(", ", updates)}");
            builder.AppendLine("WHEN NOT MATCHED THEN");
            builder.AppendLine($"  INSERT ({fieldsString})");
            builder.AppendLine($"  VALUES ({string.Join(", ", inserts)});");

            return new[]
            {
                new SqlServerConnectorCommand
                {
                    Text = builder.ToString(),
                    Parameters = parameters
                }
            };            
        }
    }
}
