using System;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using CluedIn.Connector.SqlServer.Connector;
using CluedIn.Core.Connectors;
using CluedIn.Core.Data.Vocabularies;
using CluedIn.Core.Streams.Models;

namespace CluedIn.Connector.SqlServer.Features
{
    public class DefaultUpgradeExistingSchemaFeature : IUpgradeExistingSchemaFeature
    {
        public virtual async Task VerifyExistingContainer(ISqlClient client, IConnectorConnection config, StreamModel stream)
        {
            var tableName = stream.ContainerName.SqlSanitize();
            var tables = await client.GetTableColumns(config.Authentication, tableName);
            var result = (from DataRow row in tables.Rows
                let name = row["COLUMN_NAME"] as string
                let rawType = row["DATA_TYPE"] as string
                select new 
                {
                    Name = name,
                    RawDataType = rawType
                }).ToList();

            if (result.Any() && !result.Any(n => n.Name.Equals("TimeStamp", StringComparison.OrdinalIgnoreCase)))
            {
                var columnName = "TimeStamp";
                var addTimeStampSql = $"alter table [{tableName}] add [{columnName}] {SqlColumnHelper.GetColumnType(VocabularyKeyDataType.DateTime, columnName)}";
                await client.ExecuteCommandAsync(config, addTimeStampSql);
            }
        }
    }
}
