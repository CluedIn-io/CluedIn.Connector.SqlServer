using System;
using System.Collections.Generic;
using System.Text;
using CluedIn.Connector.SqlServer.Connector;
using CluedIn.Core;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.SqlServer.Features
{
    public class DefaultBuildDeleteDataFeature : IBuildDeleteDataFeature
    {
        public const string DefaultKeyField = "Id";

        public IEnumerable<SqlServerConnectorCommand> BuildDeleteDataSql(
            ExecutionContext executionContext,
            Guid providerDefinitionId,
            string containerName,
            IDictionary<string, object> data,
            ILogger logger)
        {
            if (executionContext == null)
                throw new ArgumentNullException(nameof(executionContext));

            if (logger == null)
                throw new ArgumentNullException(nameof(logger));

            if (string.IsNullOrWhiteSpace(containerName))
                throw new InvalidOperationException("The containerName must be provided.");

            if (data == null || data.Count == 0)
                throw new InvalidOperationException("The data to specify columns must be provided.");

            var sqlBuilder = new StringBuilder($"DELETE FROM {containerName.SqlSanitize()} WHERE ");
            var clauses = new List<string>();
            var parameters = new List<SqlParameter>();

            foreach (var entry in data)
            {
                var key = entry.Key.SqlSanitize();
                clauses.Add($"[{key}] = @{key}");
                parameters.Add(new SqlParameter(key, entry.Value));
            }

            sqlBuilder.AppendJoin(" AND ", clauses);
            sqlBuilder.Append(";");

            return new[]
            {
                new SqlServerConnectorCommand
                {
                    Text = sqlBuilder.ToString(),
                    Parameters = parameters
                }
            };
        }
    }
}
