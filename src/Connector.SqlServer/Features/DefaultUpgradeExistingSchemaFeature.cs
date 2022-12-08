using CluedIn.Connector.Common.Helpers;
using CluedIn.Connector.SqlServer.Connector;
using CluedIn.Core.Connectors;
using CluedIn.Core.Data.Vocabularies;
using CluedIn.Core.Streams.Models;
using System;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

namespace CluedIn.Connector.SqlServer.Features
{
    public class DefaultUpgradeExistingSchemaFeature : IUpgradeExistingSchemaFeature
    {
        public virtual async Task VerifyExistingContainer(ISqlClient client, IConnectorConnection config,
            StreamModel stream)
        {
            //TODO. Check if ContainerName already sanitized.
            var tableName = SqlStringSanitizer.Sanitize(stream.ContainerName);
            var tables = await client.GetTableColumns(config.Authentication, tableName);
            var result = (from DataRow row in tables.Rows
                          let name = row["COLUMN_NAME"] as string
                          let rawType = row["DATA_TYPE"] as string
                          select new { Name = name, RawDataType = rawType }).ToList();

            if (result.Any() && !result.Any(n => n.Name.Equals("TimeStamp", StringComparison.OrdinalIgnoreCase)))
            {
                var columnName = "TimeStamp";
                var addTimeStampSql =
                    $"alter table [{tableName}] add [{columnName}] {SqlColumnHelper.GetColumnType(VocabularyKeyDataType.DateTime, columnName)}";
                await client.ExecuteCommandAsync(config, addTimeStampSql);
            }

            if (result.Any(x => x.Name.Equals("Id", StringComparison.OrdinalIgnoreCase)))
            {
                var sql =
$@"IF ((SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{tableName}' AND  COLUMN_NAME = 'Id') <> 'uniqueidentifier')
    ALTER TABLE [{tableName}] ALTER COLUMN Id UNIQUEIDENTIFIER

";
                

                if (result.Any(x => x.Name.Equals("OriginEntityCode", StringComparison.OrdinalIgnoreCase)))
                {
                    sql +=
@$"IF NOT EXISTS(SELECT * FROM sys.indexes WHERE object_id = object_id('{tableName}') AND NAME ='idx_{tableName}2')
    CREATE INDEX [idx_{tableName}2] ON [{tableName}] (Id) INCLUDE (OriginEntityCode)";
                }

                await client.ExecuteCommandAsync(config, sql);
            }
        }
    }
}
