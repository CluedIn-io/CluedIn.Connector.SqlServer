using CluedIn.Connector.Common.Helpers;
using CluedIn.Connector.SqlServer.Connector;
using CluedIn.Connector.SqlServer.Utils;
using CluedIn.Core;
using CluedIn.Core.Connectors;
using CluedIn.Core.Data.Vocabularies;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace CluedIn.Connector.SqlServer.Features
{
    public class DefaultBuildCreateContainerFeature : IBuildCreateContainerFeature
    {
        public virtual IEnumerable<SqlServerConnectorCommand> BuildCreateContainerSql(
            ExecutionContext executionContext,
            Guid providerDefinitionId,
            SqlTableName tableName,
            IEnumerable<ConnectionDataType> columns,
            IEnumerable<string> keys,
            ILogger logger)
        {
            // TODO: Columns should define if they are collections so we can handle creating additional tables

            if (executionContext == null)
                throw new ArgumentNullException(nameof(executionContext));

            var enumeratedColumns = columns as ConnectionDataType[] ?? columns?.ToArray();
            if (columns == null || !enumeratedColumns.Any())
                throw new InvalidOperationException("The data to specify columns must be provided.");

            if (logger == null)
                throw new ArgumentNullException(nameof(logger));

            // HACK: Remove 'Codes' column as it will be pushed to a separate table
            var trimmedColumns = enumeratedColumns.Where(x => x.Name != "Codes");

            var builder = new StringBuilder();
            builder.AppendLine($"CREATE TABLE {tableName.FullyQualifiedName}(");
            builder.AppendJoin(", ",
                trimmedColumns.Select(c => $"[{c.Name.ToSanitizedSqlName()}] {GetDbType(c.Type, c.Name)} NULL"));
            builder.AppendLine(") ON[PRIMARY]");

            return new[] { new SqlServerConnectorCommand { Text = builder.ToString() } };
        }

        protected virtual string GetDbType(VocabularyKeyDataType type, string columnName)
        {
            return SqlColumnHelper.GetColumnType(type, columnName);
        }
    }
}
