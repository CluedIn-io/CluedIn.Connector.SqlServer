using CluedIn.Connector.Common.Helpers;
using CluedIn.Connector.SqlServer.Connector;
using CluedIn.Connector.SqlServer.Utils;
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
            SqlTableName tableName,
            string originEntityCode,
            IList<IEntityCode> codes,
            Guid? entityId,
            ILogger logger)
        {
            if (executionContext == null)
                throw new ArgumentNullException(nameof(executionContext));

            if (logger == null)
                throw new ArgumentNullException(nameof(logger));

            if (!string.IsNullOrWhiteSpace(originEntityCode))
                return ComposeDelete(tableName,
                    new Dictionary<string, object> { ["OriginEntityCode"] = originEntityCode });
            if (entityId.HasValue)
                return ComposeDelete(tableName, new Dictionary<string, object> { ["Id"] = entityId.Value });
            if (codes != null)
                return codes.SelectMany(
                    x => ComposeDelete(tableName, new Dictionary<string, object> { ["Code"] = x }));

            return Enumerable.Empty<SqlServerConnectorCommand>();
        }

        protected virtual IEnumerable<SqlServerConnectorCommand> ComposeDelete(SqlTableName tableName, IDictionary<string, object> fields)
        {
            var sqlBuilder = new StringBuilder($"DELETE FROM {tableName.FullyQualifiedName} WHERE ");
            var clauses = new List<string>();
            var parameters = new List<SqlParameter>();

            foreach (var entry in fields)
            {
                var key = entry.Key.ToSanitizedSqlName();
                clauses.Add($"[{key}] = @{key}");
                parameters.Add(new SqlParameter(key, entry.Value));
            }

            sqlBuilder.AppendJoin(" AND ", clauses);
            sqlBuilder.Append(";");

            yield return new SqlServerConnectorCommand { Text = sqlBuilder.ToString(), Parameters = parameters };
        }
    }
}
