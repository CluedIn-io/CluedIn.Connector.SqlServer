﻿using CluedIn.Connector.SqlServer.Connector;
using CluedIn.Connector.SqlServer.Utils;
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
            Guid providerDefinitionId, SqlTableName tableName, IDictionary<string, object> data, IList<string> uniqueColumns,
            ILogger logger)
        {
            return BuildStoreDataSql(executionContext, providerDefinitionId, tableName, data, uniqueColumns, StreamMode.Sync,
                null, DateTimeOffset.Now, VersionChangeType.NotSet, logger);
        }

        public virtual IEnumerable<SqlServerConnectorCommand> BuildStoreDataSql(
            ExecutionContext executionContext,
            Guid providerDefinitionId,
            SqlTableName tableName,
            IDictionary<string, object> data,
            IList<string> uniqueColumns,
            StreamMode mode,
            string correlationId,
            DateTimeOffset timestamp,
            VersionChangeType changeType,
            ILogger logger)
        {
            if (executionContext == null)
                throw new ArgumentNullException(nameof(executionContext));

            if (data == null || data.Count == 0)
                throw new InvalidOperationException("The data to specify columns must be provided.");

            if (logger == null)
                throw new ArgumentNullException(nameof(logger));

            var schema = tableName.Schema;

            //HACK: we need to pull out Codes into a separate table
            var container = new Container(tableName.LocalName, mode);
            if (data.TryGetValue("Codes", out var codes) && codes is IEnumerable codesEnumerable)
            {
                data.Remove("Codes");

                // HACK need a better way to source origin entity code
                var codesTable = container.Tables["Codes"];

                if (mode == StreamMode.Sync)
                    // need to delete from Codes table
                    yield return ComposeDelete(codesTable.Name.ToTableName(schema),
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

                    yield return ComposeInsert(codesTable.Name.ToTableName(schema), dictionary);
                }
            }

            // Primary table
            if (mode == StreamMode.Sync)
                yield return ComposeUpsert(container.PrimaryTable.ToTableName(schema), data, uniqueColumns, logger);
            else
                yield return ComposeInsert(container.PrimaryTable.ToTableName(schema), data);
        }

        protected virtual SqlServerConnectorCommand ComposeUpsert(SqlTableName tableName, IDictionary<string, object> data,
            IList<string> columnsToMergeOn, ILogger logger)
        {
            var builder = new StringBuilder();
            var parameters = new List<SqlParameter>();
            var fields = new List<string>();
            var inserts = new List<string>();
            var updates = new List<string>();
            foreach (var entry in data)
            {
                var name = entry.Key.ToSanitizedSqlName();
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
            var mergeOnList = columnsToMergeOn.Select(n => $"target.[{n}] = source.[{n}]");
            var mergeOn = string.Join(" AND ", mergeOnList);

            builder.AppendLine($"MERGE {tableName.FullyQualifiedName} AS target");
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

        protected virtual SqlServerConnectorCommand ComposeDelete(SqlTableName tableName, IDictionary<string, object> fields)
        {
            var sqlBuilder = new StringBuilder($"DELETE FROM {tableName.FullyQualifiedName} WHERE ");
            var clauses = new List<string>();
            var parameters = new List<SqlParameter>();

            foreach (var entry in fields)
            {
                var key = entry.Key.ToSanitizedSqlName();
                clauses.Add($"[{key}] = @{key}");
                parameters.Add(new SqlParameter($"@{key}", entry.Value));
            }

            sqlBuilder.AppendJoin(" AND ", clauses);
            sqlBuilder.Append(";");

            return new SqlServerConnectorCommand { Text = sqlBuilder.ToString(), Parameters = parameters };
        }

        protected virtual SqlServerConnectorCommand ComposeInsert(SqlTableName tableName, IDictionary<string, object> fields)
        {
            var columns = new List<string>();
            var parameters = new List<SqlParameter>();

            foreach (var entry in fields)
            {
                var key = entry.Key.ToSanitizedSqlName();
                columns.Add($"[{key}]");
                parameters.Add(new SqlParameter($"@{key}", entry.Value));
            }

            var sqlBuilder = new StringBuilder($"INSERT INTO {tableName.FullyQualifiedName} (");
            sqlBuilder.AppendJoin(",", columns);
            sqlBuilder.Append(") values (");
            sqlBuilder.AppendJoin(",", parameters.Select(x => $"{x.ParameterName}"));
            sqlBuilder.Append(");");

            return new SqlServerConnectorCommand { Text = sqlBuilder.ToString(), Parameters = parameters };
        }
    }
}
