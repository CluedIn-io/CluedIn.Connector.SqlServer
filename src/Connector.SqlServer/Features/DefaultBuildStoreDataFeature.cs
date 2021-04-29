using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using CluedIn.Connector.SqlServer.Connector;
using CluedIn.Core;
using Microsoft.Data.SqlClient;

namespace CluedIn.Connector.SqlServer.Features
{
    public class DefaultBuildStoreDataFeature : IBuildStoreDataFeature
    {
        public virtual IEnumerable<SqlServerConnectorCommand> BuildStoreDataSql(
            ExecutionContext executionContext,
            Guid providerDefinitionId,
            string containerName,
            IDictionary<string, object> data)
        {
            if (executionContext == null)
                throw new ArgumentNullException(nameof(executionContext));

            if (string.IsNullOrWhiteSpace(containerName))
                throw new InvalidOperationException("The containerName must be provided.");

            if (data == null || data.Count == 0)
                throw new InvalidOperationException("The data to specify columns must be provided.");

            var builder = new StringBuilder();

            var nameList = data.Select(n => n.Key.SqlSanitize()).ToList();
            var fieldList = string.Join(", ", nameList.Select(n => $"[{n}]"));
            var paramList = string.Join(", ", nameList.Select(n => $"@{n}"));
            var insertList = string.Join(", ", nameList.Select(n => $"source.[{n}]"));
            var updateList = string.Join(", ", nameList.Select(n => $"target.[{n}] = source.[{n}]"));

            builder.AppendLine($"MERGE [{containerName.SqlSanitize()}] AS target");
            builder.AppendLine($"USING (SELECT {paramList}) AS source ({fieldList})");
            builder.AppendLine("  ON (target.[OriginEntityCode] = source.[OriginEntityCode])");
            builder.AppendLine("WHEN MATCHED THEN");
            builder.AppendLine($"  UPDATE SET {updateList}");
            builder.AppendLine("WHEN NOT MATCHED THEN");
            builder.AppendLine($"  INSERT ({fieldList})");
            builder.AppendLine($"  VALUES ({insertList});");

            return new []
            {
                new SqlServerConnectorCommand
                {
                    Text = builder.ToString(),
                    Parameters = data.Select(x => new SqlParameter
                    {
                        ParameterName = $"@{x.Key.SqlSanitize()}",
                        Value = x.Value ?? string.Empty
                    })
                }
            };
        }
    }
}
