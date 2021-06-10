using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using CluedIn.Connector.SqlServer.Connector;
using CluedIn.Core;
using CluedIn.Core.Data;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.SqlServer.Features
{
    public class DefaultBuildDeleteDataFeature : IBuildDeleteDataFeature
    {
        public IEnumerable<SqlServerConnectorCommand> BuildDeleteDataSql(
            ExecutionContext executionContext,
            Guid providerDefinitionId,
            string containerName,
            string originEntityCode,
            IList<IEntityCode> codes,
            Guid? entityId,
            ILogger logger)
        {
            if (executionContext == null)
                throw new ArgumentNullException(nameof(executionContext));

            if (logger == null)
                throw new ArgumentNullException(nameof(logger));

            if (string.IsNullOrWhiteSpace(containerName))
                throw new InvalidOperationException("The containerName must be provided.");

            if (!string.IsNullOrWhiteSpace(originEntityCode))
            {
                return ComposeDelete(containerName, new Dictionary<string, object>
                {
                    ["OriginEntityCode"] = originEntityCode
                });
            }
            else if(entityId.HasValue)
            {
                return ComposeDelete(containerName, new Dictionary<string, object>
                {
                    ["Id"] = entityId.Value
                });
            }
            else if(codes != null)
            {
                return codes.SelectMany(x => ComposeDelete(containerName, new Dictionary<string, object>
                {
                    ["Code"] = x
                }));
            }

            return Enumerable.Empty<SqlServerConnectorCommand>();
        }

        protected virtual IEnumerable<SqlServerConnectorCommand> ComposeDelete(string tableName, IDictionary<string, object> fields)
        {
            var sqlBuilder = new StringBuilder($"DELETE FROM {tableName.SqlSanitize()} WHERE ");
            var clauses = new List<string>();
            var parameters = new List<SqlParameter>();

            foreach (var entry in fields)
            {
                var key = entry.Key.SqlSanitize();
                clauses.Add($"[{key}] = @{key}");
                parameters.Add(new SqlParameter(key, entry.Value));
            }

            sqlBuilder.AppendJoin(" AND ", clauses);
            sqlBuilder.Append(";");

            yield return new SqlServerConnectorCommand
            {
                Text = sqlBuilder.ToString(),
                Parameters = parameters
            };
        }
    }
}
