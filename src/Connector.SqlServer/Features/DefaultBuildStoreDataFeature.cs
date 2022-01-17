using CluedIn.Connector.Common.Helpers;
using CluedIn.Connector.SqlServer.Connector;
using CluedIn.Core;
using CluedIn.Core.Data.Parts;
using CluedIn.Core.Streams.Models;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace CluedIn.Connector.SqlServer.Features
{
    public class DefaultBuildStoreDataFeature : IBuildStoreDataFeature, IBuildStoreDataForMode
    {
        public IEnumerable<SqlServerConnectorCommand> BuildStoreDataSql(ExecutionContext executionContext,
            Guid providerDefinitionId, string containerName, IDictionary<string, object> data, IList<string> keys,
            ILogger logger)
        {
            return BuildStoreDataSql(executionContext, providerDefinitionId, containerName, data, keys, StreamMode.Sync,
                null, DateTimeOffset.Now, VersionChangeType.NotSet, logger);
        }

        public virtual IEnumerable<SqlServerConnectorCommand> BuildStoreDataSql(
            ExecutionContext executionContext,
            Guid providerDefinitionId,
            string containerName,
            IDictionary<string, object> data,
            IList<string> keys,
            StreamMode mode,
            string correlationId,
            DateTimeOffset timestamp,
            VersionChangeType changeType,
            ILogger logger)
        {
            if (executionContext == null)
                throw new ArgumentNullException(nameof(executionContext));

            if (string.IsNullOrWhiteSpace(containerName))
                throw new InvalidOperationException("The containerName must be provided.");

            if (data == null || data.Count == 0)
                throw new InvalidOperationException("The data to specify columns must be provided.");

            if (keys == null || !keys.Any())
                throw new InvalidOperationException("No Key Fields have been specified");

            if (logger == null)
                throw new ArgumentNullException(nameof(logger));

            //HACK: we need to pull out Codes into a separate table
            var container = new Container(containerName, mode);
            if (data.TryGetValue("Codes", out var codes) && codes is IEnumerable codesEnumerable)
            {
                data.Remove("Codes");
                keys.Remove("Codes");

                // HACK need a better way to source origin entity code
                var codesTable = container.Tables["Codes"];

                if (mode == StreamMode.Sync)
                    // need to delete from Codes table
                    yield return ComposeDelete(codesTable.Name,
                        new Dictionary<string, object> { ["OriginEntityCode"] = data["OriginEntityCode"] });

                // need to insert into Codes table
                var enumerator = codesEnumerable.GetEnumerator();
                while (enumerator.MoveNext())
                {
                    var dictionary = new Dictionary<string, object>
                    {
                        ["OriginEntityCode"] = data["OriginEntityCode"],
                        ["Code"] = enumerator.Current
                    };

                    if (mode == StreamMode.EventStream)
                        dictionary["CorrelationId"] = correlationId;

                    yield return ComposeInsert(codesTable.Name, dictionary);
                }
            }

            // Primary table
            if (mode == StreamMode.Sync)
                yield return ComposeUpsert(container.PrimaryTable, data, keys, logger);
            else
                yield return ComposeInsert(container.PrimaryTable, data);
        }

        protected virtual SqlServerConnectorCommand ComposeUpsert(string tableName, IDictionary<string, object> data,
            IList<string> keys, ILogger logger)
        {
            var builder = new StringBuilder();
            var parameters = new List<SqlParameter>();
            var fields = new List<string>();
            var inserts = new List<string>();
            var updates = new List<string>();
            foreach (var entry in data)
            {
                var name = SqlStringSanitizer.Sanitize(entry.Key);
                var param = new SqlParameter($"@{name}", entry.Value ?? DBNull.Value);
                try
                {
                    var dbType = param.DbType;
                }
                catch (Exception ex)
                {
                    logger.LogWarning(ex, "[{field}] does not map to a known sql type - will be persisted as a string.",
                        name);
                    param.Value = JsonUtility.Serialize(entry.Value);
                }

                parameters.Add(param);
                fields.Add($"[{name}]");
                inserts.Add($"source.[{name}]");
                updates.Add($"target.[{name}] = source.[{name}]");
            }

            var fieldsString = string.Join(", ", fields);
            var mergeOnList = keys.Select(n => $"target.[{n}] = source.[{n}]");
            var mergeOn = string.Join(" AND ", mergeOnList);

            builder.AppendLine($"MERGE [{SqlStringSanitizer.Sanitize(tableName)}] AS target");
            builder.AppendLine(
                $"USING (SELECT {string.Join(", ", parameters.Select(x => x.ParameterName))}) AS source ({fieldsString})");
            builder.AppendLine($"  ON ({mergeOn})");
            builder.AppendLine("WHEN MATCHED THEN");
            builder.AppendLine($"  UPDATE SET {string.Join(", ", updates)}");
            builder.AppendLine("WHEN NOT MATCHED THEN");
            builder.AppendLine($"  INSERT ({fieldsString})");
            builder.AppendLine($"  VALUES ({string.Join(", ", inserts)});");

            return new SqlServerConnectorCommand { Text = builder.ToString(), Parameters = parameters };
        }

        protected virtual SqlServerConnectorCommand ComposeDelete(string tableName, IDictionary<string, object> fields)
        {
            var sqlBuilder = new StringBuilder($"DELETE FROM {SqlStringSanitizer.Sanitize(tableName)} WHERE ");
            var clauses = new List<string>();
            var parameters = new List<SqlParameter>();

            foreach (var entry in fields)
            {
                var key = SqlStringSanitizer.Sanitize(entry.Key);
                clauses.Add($"[{key}] = @{key}");
                parameters.Add(new SqlParameter($"@{key}", entry.Value));
            }

            sqlBuilder.AppendJoin(" AND ", clauses);
            sqlBuilder.Append(";");

            return new SqlServerConnectorCommand { Text = sqlBuilder.ToString(), Parameters = parameters };
        }

        protected virtual SqlServerConnectorCommand ComposeInsert(string tableName, IDictionary<string, object> fields)
        {
            var columns = new List<string>();
            var parameters = new List<SqlParameter>();

            foreach (var entry in fields)
            {
                columns.Add($"[{entry.Key}]");
                parameters.Add(new SqlParameter($"@{entry.Key}", entry.Value));
            }

            var sqlBuilder = new StringBuilder($"INSERT INTO [{SqlStringSanitizer.Sanitize(tableName)}] (");
            sqlBuilder.AppendJoin(",", columns);
            sqlBuilder.Append(") values (");
            sqlBuilder.AppendJoin(",", parameters.Select(x => $"{x.ParameterName}"));
            sqlBuilder.Append(");");

            return new SqlServerConnectorCommand { Text = sqlBuilder.ToString(), Parameters = parameters };
        }
    }
}
