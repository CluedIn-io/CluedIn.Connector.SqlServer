using CluedIn.Connector.Common.Helpers;
using CluedIn.Connector.SqlServer.Connector;
using CluedIn.Connector.SqlServer.Utility;
using CluedIn.Core;
using CluedIn.Core.Data;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace CluedIn.Connector.SqlServer.Features
{
    public class DefaultBuildDeleteDataFeature : IBuildDeleteDataFeature
    {
        public IEnumerable<SqlServerConnectorCommand> BuildDeleteDataSql(
            ExecutionContext executionContext,
            Guid providerDefinitionId,
            SanitizedSqlString schema,
            SanitizedSqlString tableName,
            string originEntityCode,
            IList<IEntityCode> codes,
            Guid? entityId,
            ILogger logger)
        {
            if (executionContext == null)
                throw new ArgumentNullException(nameof(executionContext));

            if (logger == null)
                throw new ArgumentNullException(nameof(logger));

            if (string.IsNullOrWhiteSpace(tableName.GetValue()))
                throw new InvalidOperationException("The containerName must be provided.");

            if (!string.IsNullOrWhiteSpace(originEntityCode))
                return ComposeDelete(schema, tableName,
                    new Dictionary<string, object> { ["OriginEntityCode"] = originEntityCode });
            if (entityId.HasValue)
                return ComposeDelete(schema, tableName, new Dictionary<string, object> { ["Id"] = entityId.Value });
            if (codes != null)
                return codes.SelectMany(
                    x => ComposeDelete(schema, tableName, new Dictionary<string, object> { ["Code"] = x }));

            return Enumerable.Empty<SqlServerConnectorCommand>();
        }

        protected virtual IEnumerable<SqlServerConnectorCommand> ComposeDelete(SanitizedSqlString schema, SanitizedSqlString tableName,
            IDictionary<string, object> fields)
        {
            var sqlBuilder = new StringBuilder($"DELETE FROM [{schema}].[{tableName}] WHERE ");
            var clauses = new List<string>();
            var parameters = new List<SqlParameter>();

            foreach (var entry in fields)
            {
                var key = SqlStringSanitizer.Sanitize(entry.Key);
                clauses.Add($"[{key}] = @{key}");
                parameters.Add(new SqlParameter(key, entry.Value));
            }

            sqlBuilder.AppendJoin(" AND ", clauses);
            sqlBuilder.Append(";");

            yield return new SqlServerConnectorCommand { Text = sqlBuilder.ToString(), Parameters = parameters };
        }
    }
}
