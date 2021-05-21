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
            IList<string> keys,
            ILogger logger)
        {
            if (executionContext == null)
                throw new ArgumentNullException(nameof(executionContext));

            if (string.IsNullOrWhiteSpace(containerName))
                throw new InvalidOperationException("The containerName must be provided.");

            if (data == null || data.Count == 0)
                throw new InvalidOperationException("The data to specify columns must be provided.");

            if (keys == null || ! keys.Any())
                throw new InvalidOperationException("No Key Fields have been specified");

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
                var param = new SqlParameter($"@{name}", entry.Value ?? DBNull.Value);
                try
                {
                    var dbType = param.DbType;
                    logger.LogDebug("Adding [{field}] as sql type [{sqlType}].", name, dbType);                    
                }
                catch (Exception ex)
                {
                    logger.LogWarning(ex, "[{field}] does not map to a known sql type - will be persisted as a string.", name);
                    param.Value = entry.Value == null ? string.Empty : entry.Value.ToString();
                }

                parameters.Add(param);
                fields.Add($"{name}");
                inserts.Add($"@{name}");
                updates.Add($"target.[{name}] = source.[{name}]");
            }

            var fieldsString = string.Join(", ", fields);
            var mergeOnList = keys.Select(n => $"target.[{n}] = source.[{n}]");
            var mergeOn = string.Join(" AND ", mergeOnList);

            //builder.AppendLine($"MERGE [{containerName.SqlSanitize()}] AS target");
            //builder.AppendLine($"USING (SELECT {string.Join(", ", parameters.Select(x => x.ParameterName))}) AS source ({fieldsString})");
            //builder.AppendLine($"  ON ({mergeOn})");
            //builder.AppendLine("WHEN MATCHED THEN");
            //builder.AppendLine($"  UPDATE SET {string.Join(", ", updates)}");
            //builder.AppendLine("WHEN NOT MATCHED THEN");
            builder.AppendLine($"  INSERT INTO {containerName.SqlSanitize()} ({string.Join(", ", fields)})");
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
