using CluedIn.Connector.SqlServer.Connector;
using CluedIn.Connector.SqlServer.Exceptions;
using CluedIn.Connector.SqlServer.Utils.TableDefinitions;
using CluedIn.Core.Connectors;
using CluedIn.Core.Streams.Models;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CluedIn.Connector.SqlServer.Utils.Upgrade
{
    internal class UpgradeTo370Utility
    {
        public static async Task Upgrade(IReadOnlyStreamModel streamModel, SqlTableName mainTableName, SqlName schema, SqlTransaction transaction, ILogger logger)
        {
            // Archive old edge table name, since rename logic used during normal archive expect different name
            {
                var oldEdgeLocalTableName = $"{mainTableName.LocalName}Edges";
                var newEdgeLocalTableName = $"{mainTableName.LocalName}_{DateTimeOffset.UtcNow}Edges";

                var oldEdgeTableName = SqlName.FromUnsafe(oldEdgeLocalTableName).ToTableName(schema);
                var newEdgeTableName = SqlName.FromUnsafe(newEdgeLocalTableName).ToTableName(schema);

                var tableRenameText = $"""
                        IF (OBJECT_ID(N'{oldEdgeTableName.FullyQualifiedName}') IS NOT NULL)
                        BEGIN
                            EXEC sp_rename N'{oldEdgeTableName.FullyQualifiedName}', {newEdgeTableName.LocalName};
                        END
                        """;

                var renameSqlConnectorCommand = new SqlServerConnectorCommand() { Text = tableRenameText, Parameters = Array.Empty<SqlParameter>() };
                await renameSqlConnectorCommand
                    .ToSqlCommand(transaction)
                    .ExecuteScalarAsync();
            }

            // Check if old main table is present
            {
                var tableExistsText = $"""
                        IF (OBJECT_ID(N'{mainTableName.FullyQualifiedName}') IS NOT NULL)
                        BEGIN
                        	SELECT 1
                        END
                        ELSE
                        BEGIN
                        	SELECT 0
                        END
                        """;

                var tableExistsCommand = new SqlServerConnectorCommand() { Text = tableExistsText, Parameters = Array.Empty<SqlParameter>() };
                var tableExistsResult = await tableExistsCommand.ToSqlCommand(transaction).ExecuteScalarAsync();

                // If the table exists, we need to check if it was created in an older version of the connector.
                // The way we do this is to check if it contains all of the columns that we expect the
                // main table to have in this version.
                if ((int)tableExistsResult == 1)
                {
                    var tableColumnsSelectText = $"""
                        SELECT columns.name FROM sys.columns columns
                        INNER JOIN sys.tables tables
                        ON tables.object_id = columns.object_id AND 
                           tables.Name = '{mainTableName.LocalName}' AND
                           tables.type = 'U'
                        INNER JOIN sys.schemas schemas
                        ON tables.schema_id = schemas.schema_id
                        WHERE schemas.name = '{mainTableName.Schema}'
                        """;

                    var tableCheckSqlConnectorCommand = new SqlServerConnectorCommand() { Text = tableColumnsSelectText, Parameters = Array.Empty<SqlParameter>() };
                    var reader = await tableCheckSqlConnectorCommand
                        .ToSqlCommand(transaction)
                        .ExecuteReaderAsync();

                    var existingColumns = new List<string>();
                    while (await reader.ReadAsync())
                    {
                        existingColumns.Add(reader[0].ToString());
                    }

                    var expectedColumnNames = MainTableDefinition
                        // streamModel.Mode will always be populated, but is technically nullable.
                        // Even if streamModel.Mode is null, it's okay to use StreamMode.Sync,
                        // since the old schema did not contain all the column in current sync mode,
                        // and sync mode is a subset of event mode.
                        .GetColumnDefinitions(streamModel.Mode ?? StreamMode.Sync, Array.Empty<(string, ConnectorPropertyDataType)>())
                        .Select(columnDefinition => columnDefinition.Name);

                    var existingColumnsContainsAllExpectedColumns = expectedColumnNames.All(column => existingColumns.Contains(column));

                    if (!existingColumnsContainsAllExpectedColumns)
                    {
                        // If an exception is thrown during `VerifyExistingContainer`, nothing can be done with the stream.
                        // This includes reprocessing the stream, to create new tables.
                        // Until this is changed in platform, we simply log the exception instead of throwing
                        // PBI: #23500
                        var exception = IncompatibleTableException.OldTableVersionExists(streamModel.Id, (Guid)streamModel.ConnectorProviderDefinitionId);
                        logger.LogError(exception, "Not all expected columns were present, most likely because the table was created in an old version");
                    }
                }
            }
        }
    }
}
