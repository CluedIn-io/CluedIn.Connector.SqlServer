using System.Collections.Generic;
using System.Linq;
using System.Text;
using CluedIn.Core.Connectors;
using CluedIn.Core.Data.Vocabularies;

namespace CluedIn.Connector.SqlServer.Features
{
    public class DefaultBuildCreateContainerFeature : IBuildCreateContainerFeature
    {
        public virtual string BuildCreateContainerSql(string name, IEnumerable<ConnectionDataType> columns)
        {
            var builder = new StringBuilder();
            builder.AppendLine($"CREATE TABLE [{name.SqlSanitize()}](");
            builder.AppendJoin(", ", columns.Select(c => $"[{c.Name.SqlSanitize()}] {GetDbType(c.Type)} NULL"));
            builder.AppendLine(") ON[PRIMARY]");

            var sql = builder.ToString();
            return sql;
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

            return "nvarchar(max)";
        }
    }
}
