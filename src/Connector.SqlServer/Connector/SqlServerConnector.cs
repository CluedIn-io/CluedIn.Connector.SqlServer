using CluedIn.Connector.Common.Connectors;
using CluedIn.Connector.SqlServer.Utils;
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
                await using var connectionAndTransaction = await _client.BeginTransaction(config.Authentication);
                var transaction = connectionAndTransaction.Transaction;

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
                var timeStamp = DateTimeOffset.Now;

                var sqlConnectorEntityData = new SqlConnectorEntityData(connectorEntityData, Guid.NewGuid());

                if (connectorEntityData.ChangeType == VersionChangeType.Removed)
                {
                    switch (streamModel.Mode)
                    {
                        case StreamMode.EventStream:
                            result = await ExecuteUpsert(streamModel, sqlConnectorEntityData, timeStamp, schema, transaction);
                            await transaction.CommitAsync();
                            return;

                        case StreamMode.Sync:
                            result = await ExecuteDelete(streamModel, sqlConnectorEntityData, schema, transaction);
                            await transaction.CommitAsync();
                            return;

                        default:
                            throw new ArgumentOutOfRangeException();
                    }
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
                var deleteEdgeIncomingPropertiesCommand = CreateStoreCommandUtility.BuildDeleteEdgePropertiesForEntity(streamModel, sqlConnectorEntityData, EdgeDirection.Incoming, schema);
                await deleteEdgeIncomingPropertiesCommand.ToSqlCommand(transaction).ExecuteNonQueryAsync();

                var deleteEdgesIncomingCommand = CreateStoreCommandUtility.BuildDeleteEdgesForEntity(streamModel, sqlConnectorEntityData, EdgeDirection.Incoming, schema);
                await deleteEdgesIncomingCommand.ToSqlCommand(transaction).ExecuteNonQueryAsync();
            }

            if (streamModel.ExportOutgoingEdges)
            {
                var deleteEdgeOutgoingPropertiesCommand = CreateStoreCommandUtility.BuildDeleteEdgePropertiesForEntity(streamModel, sqlConnectorEntityData, EdgeDirection.Outgoing, schema);
                await deleteEdgeOutgoingPropertiesCommand.ToSqlCommand(transaction).ExecuteNonQueryAsync();

                var deleteEdgesOutgoingCommand = CreateStoreCommandUtility.BuildDeleteEdgesForEntity(streamModel, sqlConnectorEntityData, EdgeDirection.Outgoing, schema);
                await deleteEdgesOutgoingCommand.ToSqlCommand(transaction).ExecuteNonQueryAsync();
            }

            var deleteCodesCommand = CreateStoreCommandUtility.BuildDeleteCodesForEntity(streamModel, sqlConnectorEntityData, schema);
            await deleteCodesCommand.ToSqlCommand(transaction).ExecuteNonQueryAsync();

            var deleteMainCommand = CreateStoreCommandUtility.BuildDeleteEntity(streamModel, sqlConnectorEntityData, schema);
            await deleteMainCommand.ToSqlCommand(transaction).ExecuteNonQueryAsync();

            return SaveResult.Success;
        }

        private async Task<SaveResult> ExecuteUpsert(IReadOnlyStreamModel streamModel, SqlConnectorEntityData sqlConnectorEntityData, DateTimeOffset timeStamp, SqlName schema, SqlTransaction transaction)
        {
            var updateCommand = CreateStoreCommandUtility.BuildStoreMainTableCommand(streamModel, sqlConnectorEntityData, timeStamp, schema);
            var updateSqlCommand = updateCommand.ToSqlCommand(transaction);
            await updateSqlCommand.ExecuteNonQueryAsync();

            var insertCodesCommand = CreateStoreCommandUtility.BuildStoreCodesInsertCommand(streamModel, sqlConnectorEntityData, schema);
            var insertCodeSqlCommand = insertCodesCommand.ToSqlCommand(transaction);
            await insertCodeSqlCommand.ExecuteNonQueryAsync();

            if (streamModel.ExportIncomingEdges && sqlConnectorEntityData.IncomingEdges.Any())
            {
                var incomingEdgesCommand = CreateStoreCommandUtility.BuildStoreEdgesCommand(streamModel, sqlConnectorEntityData, EdgeDirection.Incoming, schema);
                var incomingEdgesSqlCommand = incomingEdgesCommand.ToSqlCommand(transaction);
                await incomingEdgesSqlCommand.ExecuteNonQueryAsync();

                if (sqlConnectorEntityData.IncomingEdges.Any(edge => edge.HasProperties))
                {
                    var incomingEdgePropertiesCommand = CreateStoreCommandUtility.BuildStoreEdgePropertiesCommands(streamModel, sqlConnectorEntityData, EdgeDirection.Incoming, schema);
                    var incomingEdgePropertiesSqlCommand = incomingEdgePropertiesCommand.ToSqlCommand(transaction);
                    await incomingEdgePropertiesSqlCommand.ExecuteNonQueryAsync();
                }
            }

            if (streamModel.ExportOutgoingEdges && sqlConnectorEntityData.OutgoingEdges.Any())
            {
                var outgoingEdgesCommand = CreateStoreCommandUtility.BuildStoreEdgesCommand(streamModel, sqlConnectorEntityData, EdgeDirection.Outgoing, schema);
                var outgoingEdgesSqlCommand = outgoingEdgesCommand.ToSqlCommand(transaction);
                await outgoingEdgesSqlCommand.ExecuteNonQueryAsync();

                if (sqlConnectorEntityData.OutgoingEdges.Any(edge => edge.HasProperties))
                {
                    var outgoingEdgePropertiesCommand = CreateStoreCommandUtility.BuildStoreEdgePropertiesCommands(streamModel, sqlConnectorEntityData, EdgeDirection.Outgoing, schema);
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

                var builder = new StringBuilder();

                if (streamModel.ExportOutgoingEdges)
                {
                    var outgoingEdgesTableName = TableNameUtility.GetEdgesTableName(streamModel, EdgeDirection.Outgoing, schema);
                    var emptyOutgoingEdgeTable = BuildEmptyContainerSql(outgoingEdgesTableName);
                    builder.AppendLine(emptyOutgoingEdgeTable);

                    var outgoingEdgesPropertiesTableName = TableNameUtility.GetEdgePropertiesTableName(streamModel, EdgeDirection.Outgoing, schema);
                    var emptyOutgoingEdgesPropertiesTable = BuildEmptyContainerSql(outgoingEdgesPropertiesTableName);
                    builder.AppendLine(emptyOutgoingEdgesPropertiesTable);
                }

                if (streamModel.ExportIncomingEdges)
                {
                    var incomingEdgesTableName = TableNameUtility.GetEdgesTableName(streamModel, EdgeDirection.Incoming, schema);
                    var emptyIncomingEdgeTable = BuildEmptyContainerSql(incomingEdgesTableName);
                    builder.AppendLine(emptyIncomingEdgeTable);

                    var incomingEdgesPropertiesTableName = TableNameUtility.GetEdgePropertiesTableName(streamModel, EdgeDirection.Incoming, schema);
                    var emptyIncomingEdgesPropertiesTable = BuildEmptyContainerSql(incomingEdgesPropertiesTableName);
                    builder.AppendLine(emptyIncomingEdgesPropertiesTable);
                }

                var codeTableName = TableNameUtility.GetCodeTableName(streamModel, schema);
                var emptyCodeTable = BuildEmptyContainerSql(codeTableName);
                builder.AppendLine(emptyCodeTable);

                var mainTableName = TableNameUtility.GetMainTableName(streamModel, schema);
                var emptyMainTable = BuildEmptyContainerSql(mainTableName);
                builder.AppendLine(emptyMainTable);

                var commandText = builder.ToString();
                var sqlCommand = transaction.Connection.CreateCommand();
                sqlCommand.CommandText = commandText;
                sqlCommand.Transaction = transaction;

                await sqlCommand.ExecuteNonQueryAsync();
                
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

                var suffixDate = DateTimeOffset.Now;
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

                var suffixDate = DateTimeOffset.Now;
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
            await using var connectionAndTransaction = await _client.BeginTransaction(config.Authentication);
            var transaction = connectionAndTransaction.Transaction;

            try
            {
                var tables = await _client.GetTables(transaction, schema: config.GetSchema());

                var result = from DataRow row in tables.Rows
                             select row["TABLE_NAME"] as string
                    into tableName
                             select new SqlServerConnectorContainer { Id = tableName, Name = tableName };

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

        private string BuildEmptyContainerSql(SqlTableName tableName)
        {
            return $"TRUNCATE TABLE {tableName.FullyQualifiedName}";
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
