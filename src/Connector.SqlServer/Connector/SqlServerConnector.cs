using CluedIn.Connector.SqlServer.Exceptions;
using CluedIn.Connector.SqlServer.Utils;
using CluedIn.Connector.SqlServer.Utils.TableDefinitions;
using CluedIn.Connector.SqlServer.Utils.Upgrade;
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
    public class SqlServerConnector : ConnectorBaseV2
    {
        private readonly ILogger<SqlServerConnector> _logger;
        private readonly ISqlClient _client;
        private readonly ISqlServerConstants _constants;

        public static string DefaultSizeForFieldConfigurationKey = "SqlConnector.DefaultSizeForField";

        public SqlServerConnector(
            ILogger<SqlServerConnector> logger,
            ISqlClient client,
            ISqlServerConstants constants)
            : base(constants.ProviderId, supportsRetrievingLatestEntityPersistInfo: false)
        {
            _logger = logger;
            _client = client;
            _constants = constants;

        }

        public override async Task<string> GetValidContainerName(ExecutionContext executionContext, Guid connectorProviderDefinitionId, string containerName)
        {
            var config = await AuthenticationDetailsHelper.GetAuthenticationDetails(executionContext, connectorProviderDefinitionId);
            await using var connectionAndTransaction = await _client.BeginTransaction(config.Authentication);
            var transaction = connectionAndTransaction.Transaction;

            var cleanName = containerName.ToSanitizedMainTableName();

            if (!await CheckTableExists(executionContext, connectorProviderDefinitionId, transaction, cleanName))
            {
                return cleanName;
            }

            // If exists, append count like in windows explorer
            var count = 0;
            string newName;
            do
            {
                count++;
                newName = $"{cleanName}{count}".ToSanitizedMainTableName();
            } while (await CheckTableExists(executionContext, connectorProviderDefinitionId, transaction, newName));

            return newName;
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

                await UpgradeTo370Utility.Upgrade(streamModel, mainTableName, schema, transaction, _logger);
                await UpgradeCodeTableUtility.AddIsDataPartOriginEntityCodeToTable(streamModel, schema, transaction, _logger);

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
                var timestamp = DateTimeOffset.UtcNow;

                var sqlConnectorEntityData = new SqlConnectorEntityData(connectorEntityData, Guid.NewGuid(), timestamp);

                if (connectorEntityData.ChangeType == VersionChangeType.Removed)
                {
                    switch (streamModel.Mode)
                    {
                        case StreamMode.EventStream:
                            result = await ExecuteUpsert(streamModel, sqlConnectorEntityData, schema, transaction);
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
                        var upsertResult = await ExecuteUpsert(streamModel, sqlConnectorEntityData, schema, transaction);
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
            var commandsToRun = new List<SqlServerConnectorCommand>();

            if (streamModel.ExportIncomingEdges)
            {
                if (streamModel.ExportIncomingEdgeProperties)
                {
                    var deleteEdgeIncomingPropertiesCommand = StoreCommandBuilder.DeleteEdgePropertiesForEntity(streamModel, sqlConnectorEntityData, EdgeDirection.Incoming, schema);
                    commandsToRun.Add(deleteEdgeIncomingPropertiesCommand);
                }

                var deleteEdgesIncomingCommand = StoreCommandBuilder.DeleteEdgesForEntity(streamModel, sqlConnectorEntityData, EdgeDirection.Incoming, schema);
                commandsToRun.Add(deleteEdgesIncomingCommand);
            }

            if (streamModel.ExportOutgoingEdges)
            {
                if (streamModel.ExportOutgoingEdgeProperties)
                {
                    var deleteEdgeOutgoingPropertiesCommand = StoreCommandBuilder.DeleteEdgePropertiesForEntity(streamModel, sqlConnectorEntityData, EdgeDirection.Outgoing, schema);
                    commandsToRun.Add(deleteEdgeOutgoingPropertiesCommand);
                }

                var deleteEdgesOutgoingCommand = StoreCommandBuilder.DeleteEdgesForEntity(streamModel, sqlConnectorEntityData, EdgeDirection.Outgoing, schema);
                commandsToRun.Add(deleteEdgesOutgoingCommand);
            }

            var deleteCodesCommand = StoreCommandBuilder.DeleteCodesForEntity(streamModel, sqlConnectorEntityData, schema);
            commandsToRun.Add(deleteCodesCommand);

            var deleteMainCommand = StoreCommandBuilder.DeleteEntity(streamModel, sqlConnectorEntityData, schema);
            commandsToRun.Add(deleteMainCommand);

            var gatheredCommandText = string.Join($"""


                                                  -- Command split


                                                  """, commandsToRun.Select(command => command.Text));

            var gatheredParameters = commandsToRun.SelectMany(command => command.Parameters);

            var gatheredCommand = transaction.Connection.CreateCommand();
            gatheredCommand.CommandText = gatheredCommandText;
            gatheredCommand.Parameters.AddRange(gatheredParameters.ToArray());
            gatheredCommand.Transaction = transaction;

            await gatheredCommand.ExecuteNonQueryAsync();

            return SaveResult.Success;
        }

        private async Task<SaveResult> ExecuteUpsert(IReadOnlyStreamModel streamModel, SqlConnectorEntityData sqlConnectorEntityData, SqlName schema, SqlTransaction transaction)
        {
            var isSyncMode = streamModel.Mode == StreamMode.Sync;

            var commandsToRun = new List<SqlServerConnectorCommand>();

            var updateCommand = StoreCommandBuilder.MainTableCommand(streamModel, sqlConnectorEntityData, schema);
            commandsToRun.Add(updateCommand);

            var insertCodesCommand = StoreCommandBuilder.CodesInsertCommand(streamModel, sqlConnectorEntityData, schema);
            commandsToRun.Add(insertCodesCommand);

            if (streamModel.ExportIncomingEdges && (isSyncMode || sqlConnectorEntityData.IncomingEdges.Any()))
            {
                var incomingEdgesCommand = StoreCommandBuilder.EdgesCommand(streamModel, sqlConnectorEntityData, EdgeDirection.Incoming, schema);
                commandsToRun.Add(incomingEdgesCommand);

                if (streamModel.ExportIncomingEdgeProperties && (isSyncMode || sqlConnectorEntityData.IncomingEdges.Any(edge => edge.HasProperties)))
                {
                    var incomingEdgePropertiesCommand = StoreCommandBuilder.EdgePropertiesCommand(streamModel, sqlConnectorEntityData, EdgeDirection.Incoming, schema);
                    commandsToRun.Add(incomingEdgePropertiesCommand);
                }
            }

            if (streamModel.ExportOutgoingEdges && (isSyncMode || sqlConnectorEntityData.OutgoingEdges.Any()))
            {
                var outgoingEdgesCommand = StoreCommandBuilder.EdgesCommand(streamModel, sqlConnectorEntityData, EdgeDirection.Outgoing, schema);
                commandsToRun.Add(outgoingEdgesCommand);

                if (streamModel.ExportOutgoingEdgeProperties && (isSyncMode || sqlConnectorEntityData.OutgoingEdges.Any(edge => edge.HasProperties)))
                {
                    var outgoingEdgePropertiesCommand = StoreCommandBuilder.EdgePropertiesCommand(streamModel, sqlConnectorEntityData, EdgeDirection.Outgoing, schema);
                    commandsToRun.Add(outgoingEdgePropertiesCommand);
                }
            }

            var gatheredCommandText = string.Join("""


                                                  -- Command split


                                                  """, commandsToRun.Select(command => command.Text));

            var gatheredParameters = commandsToRun.SelectMany(command => command.Parameters);

            var gatheredCommand = transaction.Connection.CreateCommand();
            gatheredCommand.CommandText = gatheredCommandText;
            gatheredCommand.Parameters.AddRange(gatheredParameters.ToArray());
            gatheredCommand.Transaction = transaction;

            await gatheredCommand.ExecuteNonQueryAsync();

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

                await reader.DisposeAsync();
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

                await reader.DisposeAsync();
            });

            return result;
        }

        public override async Task<ConnectionVerificationResult> VerifyConnection(ExecutionContext executionContext, IReadOnlyDictionary<string, object> configurationData)
        {
            try
            {
                if (!_client.VerifyConnectionProperties(configurationData, out var configurationError))
                {
                    return new ConnectionVerificationResult(success: false, errorMessage: configurationError.ErrorMessage);
                }

                await using var connectionAndTransaction = await _client.BeginTransaction(configurationData);
                var connectionIsOpen = connectionAndTransaction.Connection.State == ConnectionState.Open;

                if (!connectionIsOpen)
                {
                    _logger.LogError("SqlServerConnector connection verification failed, connection could not be opened");
                    return new ConnectionVerificationResult(false);
                }

                var schema = configurationData.GetValue(SqlServerConstants.KeyName.Schema, (string)null);
                if (string.IsNullOrEmpty(schema))
                {
                    schema = SqlTableName.DefaultSchema;
                }


                var schemaExists = await _client.VerifySchemaExists(connectionAndTransaction.Transaction, schema);

                await connectionAndTransaction.DisposeAsync();

                if (!schemaExists)
                {
                    _logger.LogError("SqlServerConnector connection verification failed, schema '{schema}' does not exist", schema);
                    return new ConnectionVerificationResult(false);
                }

                return new ConnectionVerificationResult(true);
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Error verifying connection");
                return new ConnectionVerificationResult(false, e.Message);
            }
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
                    CreateCustomTypeCommandUtility.BuildCreateCodeTableCustomTypeCommand(model, schema)
                };

                if (model.OutgoingEdgesAreExported)
                {
                    commands.Add(CreateTableCommandUtility.BuildEdgeTableCommand(model, EdgeDirection.Outgoing, schema));
                    commands.Add(CreateCustomTypeCommandUtility.BuildCreateEdgeTableCustomTypeCommand(model, EdgeDirection.Outgoing, schema));

                    if (model.OutgoingEdgePropertiesAreExported)
                    {
                        commands.Add(CreateTableCommandUtility.BuildEdgePropertiesTableCommand(model, EdgeDirection.Outgoing, schema));
                        commands.Add(CreateCustomTypeCommandUtility.BuildCreateEdgePropertiesTableCustomTypeCommand(model, EdgeDirection.Outgoing, schema));
                    }
                }

                if (model.IncomingEdgesAreExported)
                {
                    commands.Add(CreateTableCommandUtility.BuildEdgeTableCommand(model, EdgeDirection.Incoming, schema));
                    commands.Add(CreateCustomTypeCommandUtility.BuildCreateEdgeTableCustomTypeCommand(model, EdgeDirection.Incoming, schema));

                    if (model.IncomingEdgePropertiesAreExported)
                    {
                        commands.Add(CreateTableCommandUtility.BuildEdgePropertiesTableCommand(model, EdgeDirection.Incoming, schema));
                        commands.Add(CreateCustomTypeCommandUtility.BuildCreateEdgePropertiesTableCustomTypeCommand(model, EdgeDirection.Incoming, schema));
                    }
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

                    if (streamModel.ExportOutgoingEdgeProperties)
                    {
                        var outgoingEdgesPropertiesTableName = TableNameUtility.GetEdgePropertiesTableName(streamModel, EdgeDirection.Outgoing, schema);
                        var emptyOutgoingEdgesPropertiesTableCommand = BuildEmptyContainerSql(outgoingEdgesPropertiesTableName);
                        await emptyOutgoingEdgesPropertiesTableCommand.ToSqlCommand(transaction).ExecuteNonQueryAsync();
                    }
                }

                if (streamModel.ExportIncomingEdges)
                {
                    var incomingEdgesTableName = TableNameUtility.GetEdgesTableName(streamModel, EdgeDirection.Incoming, schema);
                    var emptyIncomingEdgeTableCommand = BuildEmptyContainerSql(incomingEdgesTableName);
                    await emptyIncomingEdgeTableCommand.ToSqlCommand(transaction).ExecuteNonQueryAsync();

                    if (streamModel.ExportIncomingEdgeProperties)
                    {
                        var incomingEdgesPropertiesTableName = TableNameUtility.GetEdgePropertiesTableName(streamModel, EdgeDirection.Incoming, schema);
                        var emptyIncomingEdgesPropertiesTableCommand = BuildEmptyContainerSql(incomingEdgesPropertiesTableName);
                        await emptyIncomingEdgesPropertiesTableCommand.ToSqlCommand(transaction).ExecuteNonQueryAsync();
                    }
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
                var renameCommands = RenameTablesUtility.GetRenameTablesCommands(streamModel, mainTableName, newMainTableName, suffixDate, schema);

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
                var renameCommands = RenameTablesUtility.GetRenameTablesCommands(streamModel, oldMainTableName, newMainTableName, suffixDate, schema);

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

        public override Task<string> GetValidMappingDestinationPropertyName(ExecutionContext executionContext, Guid connectorProviderDefinitionId, string propertyName)
        {
            return Task.FromResult(propertyName);
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

                    if (streamModel.ExportOutgoingEdgeProperties)
                    {
                        var outgoingEdgesPropertiesTableName = TableNameUtility.GetEdgePropertiesTableName(streamModel, EdgeDirection.Outgoing, schema);
                        var dropOutgoingEdgesPropertiesTable = BuildRemoveContainerSql(outgoingEdgesPropertiesTableName);
                        builder.AppendLine(dropOutgoingEdgesPropertiesTable);
                    }
                }

                if (streamModel.ExportIncomingEdges)
                {
                    var incomingEdgesTableName = TableNameUtility.GetEdgesTableName(streamModel, EdgeDirection.Incoming, schema);
                    var dropIncomingEdgesTable = BuildRemoveContainerSql(incomingEdgesTableName);
                    builder.AppendLine(dropIncomingEdgesTable);

                    if (streamModel.ExportIncomingEdgeProperties)
                    {
                        var incomingEdgesPropertiesTableName = TableNameUtility.GetEdgePropertiesTableName(streamModel, EdgeDirection.Incoming, schema);
                        var dropIncomingEdgesPropertiesTable = BuildRemoveContainerSql(incomingEdgesPropertiesTableName);
                        builder.AppendLine(dropIncomingEdgesPropertiesTable);
                    }
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

        protected async Task<bool> CheckTableExists(ExecutionContext executionContext, Guid connectorProviderDefinitionId, SqlTransaction transaction, string name)
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
