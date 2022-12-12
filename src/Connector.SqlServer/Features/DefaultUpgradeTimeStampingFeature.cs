﻿using CluedIn.Connector.SqlServer.Connector;
using CluedIn.Connector.SqlServer.Utils;
using CluedIn.Core.Connectors;
using CluedIn.Core.Data.Vocabularies;
using CluedIn.Core.Streams.Models;
using Microsoft.Data.SqlClient;
using System;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

namespace CluedIn.Connector.SqlServer.Features
{
    public class DefaultUpgradeTimeStampingFeature : IUpgradeTimeStampingFeature
    {
        public virtual async Task VerifyTimeStampColumnExist(ISqlClient client, IConnectorConnection config, SqlTransaction transaction, StreamModel stream)
        {
            var tableName = SqlTableName.FromUnsafeName(stream.ContainerName, config);
            var tables = await client.GetTableColumns(transaction, tableName: tableName.LocalName, schema: tableName.Schema);
            var result = (from DataRow row in tables.Rows
                          let name = row["COLUMN_NAME"] as string
                          let rawType = row["DATA_TYPE"] as string
                          select new { Name = name, RawDataType = rawType }).ToList();

            if (result.Any() && !result.Any(n => n.Name.Equals("TimeStamp", StringComparison.OrdinalIgnoreCase)))
            {
                var columnName = "TimeStamp";
                var addTimeStampSql =
                    $"alter table {tableName.FullyQualifiedName} add [{columnName}] {SqlColumnHelper.GetColumnType(VocabularyKeyDataType.DateTime, columnName)}";
                await client.ExecuteCommandInTransactionAsync(transaction, addTimeStampSql);
            }
        }
    }
}
