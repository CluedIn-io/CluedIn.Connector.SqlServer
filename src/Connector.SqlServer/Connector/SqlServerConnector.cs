using CluedIn.Connector.Common.Connectors;
using CluedIn.Connector.SqlServer.Utils;
using CluedIn.Connector.SqlServer.Utils.TableDefinitions;
using CluedIn.Core;
using CluedIn.Core.Connectors;
using CluedIn.Core.Data.Parts;
using CluedIn.Core.Processing;
using CluedIn.Core.Streams.Models;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CluedIn.Connector.SqlServer.Connector
{
    public class SqlServerConnector : SqlConnectorBaseV2<SqlConnection, SqlTransaction, SqlParameter>
    {
        private readonly ISqlClient _client;

        public static string DefaultSizeForFieldConfigurationKey = "SqlConnector.DefaultSizeForField";

        public SqlServerConnector(
            ILogger<SqlServerConnector> logger,
            ISqlClient client,
            ISqlServerConstants constants)
            : base(logger, client, constants.ProviderId, supportsRetrievingLatestEntityPersistInfo: false)
        {
            _client = client;
        }

        public override IReadOnlyCollection<StreamMode> GetSupportedModes()
        {
            return new List<StreamMode> { StreamMode.Sync, StreamMode.EventStream };
        }

        public override async Task VerifyExistingContainer(ExecutionContext executionContext, IReadOnlyStreamModel streamModel)
        {
            await ExecuteWithRetryAsync(async () =>
            {
                var config = await AuthenticationDetailsHelper.GetAuthenticationDetails(executionContext, streamModel.ConnectorProviderDefinitionId.Value);
                var schema = config.GetSchema();
                await using var connectionAndTransaction = await _client.BeginTransaction(config.Authentication);
                var transaction = connectionAndTransaction.Transaction;

                var mainTableName = TableNameUtility.GetMainTableName(streamModel, schema);
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

                var sqlConnectorCommand = new SqlServerConnectorCommand() { Text = tableRenameText, Parameters = Array.Empty<SqlParameter>() };
                await sqlConnectorCommand.ToSqlCommand(transaction).ExecuteScalarAsync();

                await transaction.CommitAsync();
            });
        }

        public override async Task<SaveResult> StoreData(ExecutionContext executionContext, IReadOnlyStreamModel streamModel, IReadOnlyConnectorEntityData connectorEntityData)
        {
            SaveResult result = null;

            await ExecuteWithRetryAsync(async () =>
            {
                var connectorProviderDefinitionId = streamModel.ConnectorProviderDefinitionId!.Value;
                var config = await AuthenticationDetailsHelper.GetAuthenticationDetails(executionContext, connectorProviderDefinitionId);
                var schema = config.GetSchema();
                await using var connectionAndTransaction = await _client.BeginTransaction(config.Authentication);
                var transaction = connectionAndTransaction.Transaction;
                var timeStamp = DateTimeOffset.UtcNow;

                var sqlConnectorEntityData = new SqlConnectorEntityData(connectorEntityData, Guid.NewGuid());

                if (connectorEntityData.ChangeType == VersionChangeType.Removed)
                {
                    switch (streamModel.Mode)
                    {
                        case StreamMode.EventStream:
                            result = await ExecuteUpsert(streamModel, sqlConnectorEntityData, timeStamp, schema, transaction);
                            break;

                        case StreamMode.Sync:
                            result = await ExecuteDelete(streamModel, sqlConnectorEntityData, schema, transaction);
                            break;

                        default:
                            throw new ArgumentOutOfRangeException();
                    }

                    await transaction.CommitAsync();
                    return;
                }

                var existenceCommand = ExistenceCommandUtility.BuildExistenceCommand(streamModel, sqlConnectorEntityData, schema);
                var existenceResult = await ExistenceCommandUtility.ExecuteExistenceCheckCommand(existenceCommand, transaction);

                switch (existenceResult)
                {
                    case ExistenceCommandUtility.ExistenceCheckResult.NoVersionExists:
                    case ExistenceCommandUtility.ExistenceCheckResult.EarlierVersionExists:
                        var upsertResult = await ExecuteUpsert(streamModel, sqlConnectorEntityData, timeStamp, schema, transaction);
                        result = upsertResult;

                        break;
                    case ExistenceCommandUtility.ExistenceCheckResult.SameVersionExists:
                        result = SaveResult.AlreadyUpToDate;

                        break;
                    case ExistenceCommandUtility.ExistenceCheckResult.NewerVersionExists:
                        result = SaveResult.ReQueue;

                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }

                await transaction.CommitAsync();
            });

            return result;
        }

        private async Task<SaveResult> ExecuteDelete(IReadOnlyStreamModel streamModel, SqlConnectorEntityData sqlConnectorEntityData, SqlName schema, SqlTransaction transaction)
        {
            if (streamModel.ExportIncomingEdges)
            {
                var deleteEdgeIncomingPropertiesCommand = StoreCommandBuilder.DeleteEdgePropertiesForEntity(streamModel, sqlConnectorEntityData, EdgeDirection.Incoming, schema);
                await deleteEdgeIncomingPropertiesCommand.ToSqlCommand(transaction).ExecuteNonQueryAsync();

                var deleteEdgesIncomingCommand = StoreCommandBuilder.DeleteEdgesForEntity(streamModel, sqlConnectorEntityData, EdgeDirection.Incoming, schema);
                await deleteEdgesIncomingCommand.ToSqlCommand(transaction).ExecuteNonQueryAsync();
            }

            if (streamModel.ExportOutgoingEdges)
            {
                var deleteEdgeOutgoingPropertiesCommand = StoreCommandBuilder.DeleteEdgePropertiesForEntity(streamModel, sqlConnectorEntityData, EdgeDirection.Outgoing, schema);
                await deleteEdgeOutgoingPropertiesCommand.ToSqlCommand(transaction).ExecuteNonQueryAsync();

                var deleteEdgesOutgoingCommand = StoreCommandBuilder.DeleteEdgesForEntity(streamModel, sqlConnectorEntityData, EdgeDirection.Outgoing, schema);
                await deleteEdgesOutgoingCommand.ToSqlCommand(transaction).ExecuteNonQueryAsync();
            }

            var deleteCodesCommand = StoreCommandBuilder.DeleteCodesForEntity(streamModel, sqlConnectorEntityData, schema);
            await deleteCodesCommand.ToSqlCommand(transaction).ExecuteNonQueryAsync();

            var deleteMainCommand = StoreCommandBuilder.DeleteEntity(streamModel, sqlConnectorEntityData, schema);
            await deleteMainCommand.ToSqlCommand(transaction).ExecuteNonQueryAsync();

            return SaveResult.Success;
        }

        private async Task<SaveResult> ExecuteUpsert(IReadOnlyStreamModel streamModel, SqlConnectorEntityData sqlConnectorEntityData, DateTimeOffset timeStamp, SqlName schema, SqlTransaction transaction)
        {
            var isSyncMode = streamModel.Mode == StreamMode.Sync;

            var updateCommand = StoreCommandBuilder.MainTableCommand(streamModel, sqlConnectorEntityData, timeStamp, schema);
            var updateSqlCommand = updateCommand.ToSqlCommand(transaction);
            await updateSqlCommand.ExecuteNonQueryAsync();

            var insertCodesCommand = StoreCommandBuilder.CodesInsertCommand(streamModel, sqlConnectorEntityData, schema);
            var insertCodeSqlCommand = insertCodesCommand.ToSqlCommand(transaction);
            await insertCodeSqlCommand.ExecuteNonQueryAsync();

            if (streamModel.ExportIncomingEdges && (isSyncMode || sqlConnectorEntityData.IncomingEdges.Any()))
            {
                var incomingEdgesCommand = StoreCommandBuilder.EdgesCommand(streamModel, sqlConnectorEntityData, EdgeDirection.Incoming, schema);
                var incomingEdgesSqlCommand = incomingEdgesCommand.ToSqlCommand(transaction);
                await incomingEdgesSqlCommand.ExecuteNonQueryAsync();

                if (isSyncMode || sqlConnectorEntityData.IncomingEdges.Any(edge => edge.HasProperties))
                {
                    var incomingEdgePropertiesCommand = StoreCommandBuilder.EdgePropertiesCommand(streamModel, sqlConnectorEntityData, EdgeDirection.Incoming, schema);
                    var incomingEdgePropertiesSqlCommand = incomingEdgePropertiesCommand.ToSqlCommand(transaction);
                    await incomingEdgePropertiesSqlCommand.ExecuteNonQueryAsync();
                }
            }

            if (streamModel.ExportOutgoingEdges && (isSyncMode || sqlConnectorEntityData.OutgoingEdges.Any()))
            {
                var outgoingEdgesCommand = StoreCommandBuilder.EdgesCommand(streamModel, sqlConnectorEntityData, EdgeDirection.Outgoing, schema);
                var outgoingEdgesSqlCommand = outgoingEdgesCommand.ToSqlCommand(transaction);
                await outgoingEdgesSqlCommand.ExecuteNonQueryAsync();

                if (isSyncMode || sqlConnectorEntityData.OutgoingEdges.Any(edge => edge.HasProperties))
                {
                    var outgoingEdgePropertiesCommand = StoreCommandBuilder.EdgePropertiesCommand(streamModel, sqlConnectorEntityData, EdgeDirection.Outgoing, schema);
                    var outgoingEdgePropertiesSqlCommand = outgoingEdgePropertiesCommand.ToSqlCommand(transaction);
                    await outgoingEdgePropertiesSqlCommand.ExecuteNonQueryAsync();
                }
            }

            return SaveResult.Success;
        }

        public override async Task<ConnectorLatestEntityPersistInfo> GetLatestEntityPersistInfo(ExecutionContext executionContext, IReadOnlyStreamModel streamModel, Guid entityId)
        {
            ConnectorLatestEntityPersistInfo result = null;

            await ExecuteWithRetryAsync(async () =>
            {
                var config = await AuthenticationDetailsHelper.GetAuthenticationDetails(executionContext, streamModel.ConnectorProviderDefinitionId.Value);
                var schema = config.GetSchema();
                await using var connectionAndTransaction = await _client.BeginTransaction(config.Authentication);
                var transaction = connectionAndTransaction.Transaction;

                var command = LatestPersistInfoCommandUtility.GetSinglePersistInfoCommand(streamModel, schema, entityId);
                var sqlCommand = command.ToSqlCommand(transaction);
                var reader = await sqlCommand.ExecuteReaderAsync();
                result = await LatestPersistInfoCommandUtility.ReadSinglePersistInfo(reader);
            });

            return result;
        }

        public override async Task<IAsyncEnumerable<ConnectorLatestEntityPersistInfo>> GetLatestEntityPersistInfos(ExecutionContext executionContext, IReadOnlyStreamModel streamModel)
        {
            IAsyncEnumerable<ConnectorLatestEntityPersistInfo> result = null;

            await ExecuteWithRetryAsync(async () =>
            {
                var config = await AuthenticationDetailsHelper.GetAuthenticationDetails(executionContext, streamModel.ConnectorProviderDefinitionId.Value);
                var schema = config.GetSchema();
                await using var connectionAndTransaction = await _client.BeginTransaction(config.Authentication);
                var transaction = connectionAndTransaction.Transaction;

                var command = LatestPersistInfoCommandUtility.GetAllPersistInfosCommand(streamModel, schema);
                var sqlCommand = command.ToSqlCommand(transaction);
                var reader = await sqlCommand.ExecuteReaderAsync();
                result = LatestPersistInfoCommandUtility.ReadAllPersistInfos(reader);
            });

            return result;
        }

        public override async Task CreateContainer(ExecutionContext executionContext, Guid connectorProviderDefinitionId, IReadOnlyCreateContainerModelV2 model)
        {
            await ExecuteWithRetryAsync(async () =>
            {
                var config = await AuthenticationDetailsHelper.GetAuthenticationDetails(executionContext, connectorProviderDefinitionId);
                var schema = config.GetSchema();
                await using var connectionAndTransaction = await _client.BeginTransaction(config.Authentication);
                var transaction = connectionAndTransaction.Transaction;

                var commands = new List<SqlServerConnectorCommand>
                {
                    CreateTableCommandUtility.BuildMainTableCommand(model, schema),
                    CreateTableCommandUtility.BuildCodeTableCommand(model, schema),
                    CreateCustomTypeCommandUtility.BuildCodeTableCustomTypeCommand(model, schema)
                };

                if (model.OutgoingEdgesAreExported)
                {
                    commands.Add(CreateTableCommandUtility.BuildEdgeTableCommand(model, EdgeDirection.Outgoing, schema));
                    commands.Add(CreateCustomTypeCommandUtility.BuildEdgeTableCustomTypeCommand(model, EdgeDirection.Outgoing, schema));

                    commands.Add(CreateTableCommandUtility.BuildEdgePropertiesTableCommand(model, EdgeDirection.Outgoing, schema));
                    commands.Add(CreateCustomTypeCommandUtility.BuildEdgePropertiesTableCustomTypeCommand(model, EdgeDirection.Outgoing, schema));
                }

                if (model.IncomingEdgesAreExported)
                {
                    commands.Add(CreateTableCommandUtility.BuildEdgeTableCommand(model, EdgeDirection.Incoming, schema));
                    commands.Add(CreateCustomTypeCommandUtility.BuildEdgeTableCustomTypeCommand(model, EdgeDirection.Incoming, schema));

                    commands.Add(CreateTableCommandUtility.BuildEdgePropertiesTableCommand(model, EdgeDirection.Incoming, schema));
                    commands.Add(CreateCustomTypeCommandUtility.BuildEdgePropertiesTableCustomTypeCommand(model, EdgeDirection.Incoming, schema));
                }

                foreach (var command in commands)
                {
                    _logger.LogDebug("Sql Server Connector - Generated query: {sql}", command.Text);

                    await _client.ExecuteCommandInTransactionAsync(transaction, command.Text, command.Parameters);
                }

                await transaction.CommitAsync();
            });
        }

        public override async Task EmptyContainer(ExecutionContext executionContext, IReadOnlyStreamModel streamModel)
        {
            await ExecuteWithRetryAsync(async () =>
            {
                var config = await AuthenticationDetailsHelper.GetAuthenticationDetails(executionContext, streamModel.ConnectorProviderDefinitionId!.Value);
                var schema = config.GetSchema();
                await using var connectionAndTransaction = await _client.BeginTransaction(config.Authentication);
                var transaction = connectionAndTransaction.Transaction;

                if (streamModel.ExportOutgoingEdges)
                {
                    var outgoingEdgesTableName = TableNameUtility.GetEdgesTableName(streamModel, EdgeDirection.Outgoing, schema);
                    var emptyOutgoingEdgeTableCommand = BuildEmptyContainerSql(outgoingEdgesTableName);
                    await emptyOutgoingEdgeTableCommand.ToSqlCommand(transaction).ExecuteNonQueryAsync();

                    var outgoingEdgesPropertiesTableName = TableNameUtility.GetEdgePropertiesTableName(streamModel, EdgeDirection.Outgoing, schema);
                    var emptyOutgoingEdgesPropertiesTableCommand = BuildEmptyContainerSql(outgoingEdgesPropertiesTableName);
                    await emptyOutgoingEdgesPropertiesTableCommand.ToSqlCommand(transaction).ExecuteNonQueryAsync();
                }

                if (streamModel.ExportIncomingEdges)
                {
                    var incomingEdgesTableName = TableNameUtility.GetEdgesTableName(streamModel, EdgeDirection.Incoming, schema);
                    var emptyIncomingEdgeTableCommand = BuildEmptyContainerSql(incomingEdgesTableName);
                    await emptyIncomingEdgeTableCommand.ToSqlCommand(transaction).ExecuteNonQueryAsync();

                    var incomingEdgesPropertiesTableName = TableNameUtility.GetEdgePropertiesTableName(streamModel, EdgeDirection.Incoming, schema);
                    var emptyIncomingEdgesPropertiesTableCommand = BuildEmptyContainerSql(incomingEdgesPropertiesTableName);
                    await emptyIncomingEdgesPropertiesTableCommand.ToSqlCommand(transaction).ExecuteNonQueryAsync();
                }

                var codeTableName = TableNameUtility.GetCodeTableName(streamModel, schema);
                var emptyCodeTableCommand = BuildEmptyContainerSql(codeTableName);
                await emptyCodeTableCommand.ToSqlCommand( transaction ).ExecuteNonQueryAsync();

                var mainTableName = TableNameUtility.GetMainTableName(streamModel, schema);
                var emptyMainTableCommand = BuildEmptyContainerSql(mainTableName);
                await emptyMainTableCommand.ToSqlCommand(transaction).ExecuteNonQueryAsync();

                await transaction.CommitAsync();
            });
        }

        public override async Task ArchiveContainer(ExecutionContext executionContext, IReadOnlyStreamModel streamModel)
        {
            await ExecuteWithRetryAsync(async () =>
            {
                var connectorProviderDefinitionId = streamModel.ConnectorProviderDefinitionId!.Value;
                var config = await AuthenticationDetailsHelper.GetAuthenticationDetails(executionContext, connectorProviderDefinitionId);
                var schema = config.GetSchema();
                await using var connectionAndTransaction = await _client.BeginTransaction(config.Authentication);
                var transaction = connectionAndTransaction.Transaction;

                var mainTableName = TableNameUtility.GetMainTableName(streamModel, schema);
                var newMainTableNameWithDateAppended = $"{mainTableName.LocalName}_{DateTime.Now:yyyyMMddHHmmss}";
                var newMainTableName = TableNameUtility.GetMainTableName(newMainTableNameWithDateAppended, schema);

                var suffixDate = DateTimeOffset.UtcNow;
                var renameCommands = GetRenameTablesCommands(streamModel, mainTableName, newMainTableName, suffixDate, schema);

                foreach (var renameCommand in renameCommands)
                {
                    var sqlCommand = transaction.Connection.CreateCommand();
                    sqlCommand.CommandText = renameCommand.Text;
                    sqlCommand.Parameters.AddRange(renameCommand.Parameters.ToArray());
                    sqlCommand.Transaction = transaction;

                    await sqlCommand.ExecuteNonQueryAsync();
                }

                await transaction.CommitAsync();
            });
        }

        public override async Task RenameContainer(ExecutionContext executionContext, IReadOnlyStreamModel streamModel, string oldContainerName)
        {
            await ExecuteWithRetryAsync(async () =>
            {
                var connectorProviderDefinitionId = streamModel.ConnectorProviderDefinitionId!.Value;
                var config = await AuthenticationDetailsHelper.GetAuthenticationDetails(executionContext, connectorProviderDefinitionId);
                var schema = config.GetSchema();
                await using var connectionAndTransaction = await _client.BeginTransaction(config.Authentication);
                var transaction = connectionAndTransaction.Transaction;

                var oldMainTableName = TableNameUtility.GetMainTableName(oldContainerName, schema);
                var newMainTableName = TableNameUtility.GetMainTableName(streamModel, schema);

                var suffixDate = DateTimeOffset.UtcNow;
                var renameCommands = GetRenameTablesCommands(streamModel, oldMainTableName, newMainTableName, suffixDate, schema);

                foreach (var renameCommand in renameCommands)
                {
                    var sqlCommand = transaction.Connection.CreateCommand();
                    sqlCommand.CommandText = renameCommand.Text;
                    sqlCommand.Parameters.AddRange(renameCommand.Parameters.ToArray());
                    sqlCommand.Transaction = transaction;

                    await sqlCommand.ExecuteNonQueryAsync();
                }

                await transaction.CommitAsync();
            });
        }

        private IEnumerable<SqlServerConnectorCommand> GetRenameTablesCommands(IReadOnlyStreamModel streamModel, SqlTableName oldMainTableName, SqlTableName newMainTableName, DateTimeOffset suffixDate, SqlName schema)
        {
            var builder = new StringBuilder();

            if (streamModel.ExportOutgoingEdges)
            {
                var outgoingEdgesTableOldName = TableNameUtility.GetEdgesTableName(oldMainTableName, EdgeDirection.Outgoing, schema);
                var outgoingEdgesTableNewName = TableNameUtility.GetEdgesTableName(newMainTableName, EdgeDirection.Outgoing, schema);
                var renameOutgoingEdgesTableCommand = RenameTableCommandUtility.BuildTableRenameCommand(outgoingEdgesTableOldName, outgoingEdgesTableNewName, schema, suffixDate);
                yield return  renameOutgoingEdgesTableCommand;

                var outgoingEdgesPropertiesTableOldName = TableNameUtility.GetEdgePropertiesTableName(oldMainTableName, EdgeDirection.Outgoing, schema);
                var outgoingEdgesPropertiesTableNewName = TableNameUtility.GetEdgePropertiesTableName(newMainTableName, EdgeDirection.Outgoing, schema);
                var renameOutgoingEdgesPropertiesTableCommand = RenameTableCommandUtility.BuildTableRenameCommand(outgoingEdgesPropertiesTableOldName, outgoingEdgesPropertiesTableNewName, schema, suffixDate);
                yield return renameOutgoingEdgesPropertiesTableCommand;
            }

            if (streamModel.ExportIncomingEdges)
            {
                var incomingEdgesTableOldName = TableNameUtility.GetEdgesTableName(oldMainTableName, EdgeDirection.Incoming, schema);
                var incomingEdgesTableNewName = TableNameUtility.GetEdgesTableName(newMainTableName, EdgeDirection.Incoming, schema);
                var renameIncomingEdgesTableCommand = RenameTableCommandUtility.BuildTableRenameCommand(incomingEdgesTableOldName, incomingEdgesTableNewName, schema, suffixDate);
                yield return renameIncomingEdgesTableCommand;

                var incomingEdgesPropertiesTableOldName = TableNameUtility.GetEdgePropertiesTableName(oldMainTableName, EdgeDirection.Incoming, schema);
                var incomingEdgesPropertiesTableNewName = TableNameUtility.GetEdgePropertiesTableName(newMainTableName, EdgeDirection.Incoming, schema);
                var renameIncomingEdgesPropertiesTableCommand = RenameTableCommandUtility.BuildTableRenameCommand(incomingEdgesPropertiesTableOldName, incomingEdgesPropertiesTableNewName, schema, suffixDate);
                yield return renameIncomingEdgesPropertiesTableCommand;
            }

            var oldCodeTableName = TableNameUtility.GetCodeTableName(oldMainTableName, schema);
            var newCodeTableName = TableNameUtility.GetCodeTableName(newMainTableName, schema);
            var renameCodeTableCommand = RenameTableCommandUtility.BuildTableRenameCommand(oldCodeTableName, newCodeTableName, schema, suffixDate);
            yield return renameCodeTableCommand;

            var renameMainTableCommand = RenameTableCommandUtility.BuildTableRenameCommand(oldMainTableName, newMainTableName, schema, suffixDate);
            yield return renameMainTableCommand;
        }

        public override async Task RemoveContainer(ExecutionContext executionContext, IReadOnlyStreamModel streamModel)
        {
            await ExecuteWithRetryAsync(async () =>
            {
                var connectorProviderDefinitionId = streamModel.ConnectorProviderDefinitionId!.Value;
                var config = await AuthenticationDetailsHelper.GetAuthenticationDetails(executionContext, connectorProviderDefinitionId);
                var schema = config.GetSchema();
                await using var connectionAndTransaction = await _client.BeginTransaction(config.Authentication);
                var transaction = connectionAndTransaction.Transaction;

                var builder = new StringBuilder();

                if (streamModel.ExportOutgoingEdges)
                {
                    var outgoingEdgesTableName = TableNameUtility.GetEdgesTableName(streamModel, EdgeDirection.Outgoing, schema);
                    var dropOutgoingEdgesTable = BuildRemoveContainerSql(outgoingEdgesTableName);
                    builder.AppendLine(dropOutgoingEdgesTable);

                    var outgoingEdgesPropertiesTableName = TableNameUtility.GetEdgePropertiesTableName(streamModel, EdgeDirection.Outgoing, schema);
                    var dropOutgoingEdgesPropertiesTable = BuildRemoveContainerSql(outgoingEdgesPropertiesTableName);
                    builder.AppendLine(dropOutgoingEdgesPropertiesTable);
                }

                if (streamModel.ExportIncomingEdges)
                {
                    var incomingEdgesTableName = TableNameUtility.GetEdgesTableName(streamModel, EdgeDirection.Incoming, schema);
                    var dropIncomingEdgesTable = BuildRemoveContainerSql(incomingEdgesTableName);
                    builder.AppendLine(dropIncomingEdgesTable);

                    var incomingEdgesPropertiesTableName = TableNameUtility.GetEdgePropertiesTableName(streamModel, EdgeDirection.Incoming, schema);
                    var dropIncomingEdgesPropertiesTable = BuildRemoveContainerSql(incomingEdgesPropertiesTableName);
                    builder.AppendLine(dropIncomingEdgesPropertiesTable);
                }

                var codeTableName = TableNameUtility.GetCodeTableName(streamModel, schema);
                var dropCodeTable = BuildRemoveContainerSql(codeTableName);
                builder.AppendLine(dropCodeTable);

                var mainTableName = TableNameUtility.GetMainTableName(streamModel, schema);
                var dropMainTable = BuildRemoveContainerSql(mainTableName);
                builder.AppendLine(dropMainTable);

                var commandText = builder.ToString();
                var sqlCommand = transaction.Connection.CreateCommand();
                sqlCommand.CommandText = commandText;
                sqlCommand.Transaction = transaction;

                await sqlCommand.ExecuteNonQueryAsync();
                await transaction.CommitAsync();
            });
        }

        public override async Task<IEnumerable<IConnectorContainer>> GetContainers(ExecutionContext executionContext, Guid connectorProviderDefinitionId)
        {
            var config = await AuthenticationDetailsHelper.GetAuthenticationDetails(executionContext, connectorProviderDefinitionId);
            var schema = config.GetSchema();
            var connection = await _client.BeginConnection(config.Authentication);

            try
            {
                var tables = await _client.GetTables(connection, schema: schema);
                var tableNames = tables.Rows
                    .Cast<DataRow>()
                    .Select(row => row["TABLE_NAME"] as string);

                var tableAndColumns = tableNames
                    .Select(name =>
                    {
                        var tableColumns = _client.GetTableColumns(connection, name, schema).Result.Rows
                            .Cast<DataRow>()
                            .Select(row => row["COLUMN_NAME"])
                            .Cast<string>()
                            .ToArray();
                        return (name, tableColumns);
                    });

                var minimumMainTableColumns = MainTableDefinition
                    .GetColumnDefinitions(StreamMode.Sync, Array.Empty<(string name, ConnectorPropertyDataType dataType)>()).Select(column => column.Name);

                var mainTables = tableAndColumns
                    // Note: This is somewhat of a hack.
                    // In order to determine which tables are main tables, we find all of the tables that as a minimum, have the columns
                    // that a main table would have in sync mode (since the columns in sync mode is a subset of the columns in event mode)
                    .Where(tc => minimumMainTableColumns.All(mainTableColumnName => tc.tableColumns.Contains(mainTableColumnName)))
                    .Select(tc => tc.name);

                var result = mainTables
                    .Select(tableName => new SqlServerConnectorContainer { Id = tableName, Name = tableName });

                return result.ToList();
            }
            catch (Exception e)
            {
                var message = $"Could not get Containers for Connector {connectorProviderDefinitionId}";
                _logger.LogError(e, message);
                throw new GetContainersException(message, e);
            }
        }

        private string BuildRemoveContainerSql(SqlTableName tableName)
        {
            return $"DROP TABLE {tableName.FullyQualifiedName} IF EXISTS";
        }

        private SqlServerConnectorCommand BuildEmptyContainerSql(SqlTableName tableName)
        {
            var text = $"TRUNCATE TABLE {tableName.FullyQualifiedName}";
            return new SqlServerConnectorCommand() { Text = text };
        }

        protected async Task<bool> CheckTableExists(SqlTransaction transaction, SqlTableName tableName)
        {
            try
            {
                // Adapted from the command that Microsoft SQL client uses to get tables with name
                var sqlCommandText = $@"
select count(*)
from INFORMATION_SCHEMA.TABLES
where
(TABLE_SCHEMA = @Owner or (@Owner is null)) and
(TABLE_NAME = @Name or (@Name is null))";
                var command = transaction.Connection.CreateCommand();
                command.Transaction = transaction;
                command.CommandText = sqlCommandText;
                command.Parameters.AddWithValue("@Owner", tableName.Schema.Value);
                command.Parameters.AddWithValue("@Name", tableName.LocalName.Value);

                var tablesCount = await command.ExecuteScalarAsync();
                return tablesCount is int count && count > 0;
            }
            catch (Exception e)
            {
                _logger.LogError(e, $"Error checking Container '{tableName}' exists");
                return false;
            }
        }

        protected override async Task<bool> CheckTableExists(ExecutionContext executionContext, Guid connectorProviderDefinitionId, SqlTransaction transaction, string name)
        {
            var config = await AuthenticationDetailsHelper.GetAuthenticationDetails(executionContext, connectorProviderDefinitionId);
            var tableName = SqlTableName.FromUnsafeName(name, config);

            return await CheckTableExists(transaction, tableName);
        }

        private static async Task ExecuteWithRetryAsync(Func<Task> taskFunc)
        {
            await taskFunc.ExecuteWithRetryAsync(isTransient: ExceptionExtensions.IsTransient);
        }
    }
}
