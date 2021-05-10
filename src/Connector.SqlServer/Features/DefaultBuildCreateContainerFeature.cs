using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using CluedIn.Connector.SqlServer.Connector;
using CluedIn.Core;
using CluedIn.Core.Connectors;
using CluedIn.Core.Data.Vocabularies;
using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.SqlServer.Features
{
    public class DefaultBuildCreateContainerFeature : IBuildCreateContainerFeature
    {
        public virtual IEnumerable<SqlServerConnectorCommand> BuildCreateContainerSql(
            ExecutionContext executionContext,
            Guid providerDefinitionId,
            string name,
            IEnumerable<ConnectionDataType> columns,
            IList<string> keys,
            ILogger logger)
        {
            if (executionContext == null)
                throw new ArgumentNullException(nameof(executionContext));

            if (string.IsNullOrWhiteSpace(name))
                throw new InvalidOperationException("The name must be provided.");

            if (columns == null || !columns.Any())
                throw new InvalidOperationException("The data to specify columns must be provided.");

            if (logger == null)
                throw new ArgumentNullException(nameof(logger));

            var builder = new StringBuilder();
            var sanitizedName = name.SqlSanitize();
            builder.AppendLine($"CREATE TABLE [{sanitizedName}](");
            builder.AppendJoin(", ", columns.Select(c => $"[{c.Name.SqlSanitize()}] {GetDbType(c.Type)} NULL"));
            builder.AppendLine(") ON[PRIMARY]");

            var indexName = $"idx_{sanitizedName}".SqlSanitize();
            builder.AppendLine($"CREATE INDEX [{indexName}] ON [{sanitizedName}]({string.Join(", ", keys)}); ");

            return new[]
            {
                new SqlServerConnectorCommand
                {
                    Text = builder.ToString()
                }
            };
        }

        protected virtual string GetDbType(VocabularyKeyDataType type)
        {
            // return type switch //TODO: @LJU: Disabled as it needs reviewing; Breaks streams;
            // {
            //     VocabularyKeyDataType.Integer => "bigint",
            //     VocabularyKeyDataType.Number => "decimal(18,4)",
            //     VocabularyKeyDataType.Money => "money",
            //     VocabularyKeyDataType.DateTime => "datetime2",
            //     VocabularyKeyDataType.Time => "time",
            //     VocabularyKeyDataType.Xml => "XML",
            //     VocabularyKeyDataType.Guid => "uniqueidentifier",
            //     VocabularyKeyDataType.GeographyLocation => "geography",
            //     _ => "nvarchar(max)"
            // };

            return "nvarchar(1024)";
        }
    }
}
