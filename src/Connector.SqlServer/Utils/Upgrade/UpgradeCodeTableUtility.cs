using CluedIn.Connector.SqlServer.Connector;
using CluedIn.Connector.SqlServer.Utils.TableDefinitions;
using CluedIn.Core.Streams.Models;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CluedIn.Connector.SqlServer.Utils.Upgrade
{
    internal class UpgradeCodeTableUtility
    {
        public static async Task AddIsDataPartOriginEntityCodeToTable(IReadOnlyStreamModel streamModel, SqlName schema, SqlTransaction transaction, ILogger logger)
        {
            var codeTableName = TableNameUtility.GetCodeTableName(streamModel, schema);
            var codeTableColumnsSqlQueryText = $"""
                SELECT [COLUMN_NAME]
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = N'{codeTableName.LocalName}'
                """;

            var codeTableColumnsSqlQuery = new SqlServerConnectorCommand { Text = codeTableColumnsSqlQueryText, Parameters = Array.Empty<SqlParameter>() };
            var reader = await codeTableColumnsSqlQuery.ToSqlCommand(transaction).ExecuteReaderAsync();

            var actualColumns = new List<string>();
            while (await reader.ReadAsync())
            {
                actualColumns.Add(reader[0].ToString());
            }

            // TODO
            var streamMode = streamModel.Mode ?? StreamMode.Sync;
            var expectedColumns = CodeTableDefinition.GetColumnDefinitions(streamMode).Select(c => c.Name).ToList();
            var missingColumns = expectedColumns.Except(actualColumns);
            var isDataPartOriginEntityCodeColumnDefinition = CodeTableDefinition.IsDataPartOriginEntityCodeColumnDefinition;

            if (missingColumns.Contains(isDataPartOriginEntityCodeColumnDefinition.Name))
            {
                // Upgrade table
                {
                    var nullString = isDataPartOriginEntityCodeColumnDefinition.CanBeNull ? "NULL" : "NOT NULL";
                    var alterCommandText = $"""
                    ALTER TABLE {codeTableName.LocalName}
                    ADD {isDataPartOriginEntityCodeColumnDefinition.Name} {isDataPartOriginEntityCodeColumnDefinition.ConnectorSqlType.StringRepresentation} {nullString}
                    """;
                    var alterCommand = new SqlServerConnectorCommand { Text = alterCommandText, Parameters = Array.Empty<SqlParameter>() };

                    await alterCommand.ToSqlCommand(transaction).ExecuteNonQueryAsync();
                }
                

                // Upgrade custom type
                {
                    var mainTableName = TableNameUtility.GetMainTableName(streamModel, schema);
                    var mainTableNameWithDateAppended = $"{mainTableName.LocalName}_{DateTime.Now:yyyyMMddHHmmss}";
                    var archiveMainTableName = TableNameUtility.GetMainTableName(mainTableNameWithDateAppended, schema);
                    var renameCustomTypeCommand = CreateCustomTypeCommandUtility.BuildRenameCodeTableCustomTypeCommand(mainTableName, archiveMainTableName, schema);

                    await renameCustomTypeCommand.ToSqlCommand(transaction).ExecuteNonQueryAsync();

                    var addCustomTypeCommand = CreateCustomTypeCommandUtility.BuildCreateCodeTableCustomTypeCommand(streamModel, schema);
                    await addCustomTypeCommand.ToSqlCommand(transaction).ExecuteNonQueryAsync();
                }
            }
        }
    }
}
