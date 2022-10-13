using CluedIn.Connector.Common.Helpers;
using CluedIn.Connector.SqlServer.Connector;
using CluedIn.Connector.SqlServer.Utility;
using CluedIn.Core.Connectors;
using CluedIn.Core.Data.Vocabularies;
using CluedIn.Core.Streams.Models;
using System;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

namespace CluedIn.Connector.SqlServer.Features
{
    public class DefaultTimeStampingFeature : ITimeStampingFeature
    {
        public virtual async Task VerifyTimeStampColumnExist(ISqlClient client, IConnectorConnection config,
            StreamModel stream)
        {
            //TODO. Check if ContainerName already sanitized.
            var tableName = new SanitizedSqlName(stream.ContainerName);
            var schema = config.GetSchema();
            var tables = await client.GetTableColumns(config.Authentication, tableName.GetValue());
            var result = (from DataRow row in tables.Rows
                          let name = row["COLUMN_NAME"] as string
                          let rawType = row["DATA_TYPE"] as string
                          select new { Name = name, RawDataType = rawType }).ToList();

            if (result.Any() && !result.Any(n => n.Name.Equals("TimeStamp", StringComparison.OrdinalIgnoreCase)))
            {
                var columnName = "TimeStamp";
                var addTimeStampSql =
                    $"ALTER TABLE [{schema}].[{tableName}] ADD [{columnName}] {SqlColumnHelper.GetColumnType(VocabularyKeyDataType.DateTime, columnName)}";
                await client.ExecuteCommandAsync(config, addTimeStampSql);
            }
        }
    }
}
