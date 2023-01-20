using CluedIn.Connector.Common.Connectors;
using CluedIn.Connector.Common.Features;
using CluedIn.Connector.SqlServer.Features;
using CluedIn.Connector.SqlServer.Utils;
using CluedIn.Core;
using CluedIn.Core.Configuration;
using CluedIn.Core.Connectors;
using CluedIn.Core.Data;
using CluedIn.Core.Data.Parts;
using CluedIn.Core.Data.Vocabularies;
using CluedIn.Core.DataStore;
using CluedIn.Core.Streams.Models;
using Microsoft.Data.SqlClient;
using Microsoft.Data.SqlClient.Server;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CluedIn.Connector.SqlServer.Connector
{
    public class SqlServerConnector : SqlConnectorBase<SqlConnection, SqlTransaction, SqlParameter>, IConnectorStreamModeSupport, IConnectorUpgrade
    {
        private const string TimestampFieldName = "TimeStamp";
        private const string ChangeTypeFieldName = "ChangeType";
        private const string CorrelationIdFieldName = "CorrelationId";
        private readonly IBulkSqlClient _bulkClient;
        private readonly ISqlClient _client;
        private readonly int _bulkDeleteThreshold;
        private readonly int _bulkInsertThreshold;
        private readonly bool _bulkSupported;
        private readonly bool _syncEdgesTable;

        public static string DefaultSizeForFieldConfigurationKey = "SqlConnector.DefaultSizeForField";

        private readonly IList<(string[] columns, bool isUnique)> _syncStreamIndexFields = new List<(string[], bool)>
        {
            (new[] {"Id"}, true),
            (new[] {"OriginEntityCode"}, false)
        };

        private readonly IList<(string[] columns, bool isUnique)> _eventStreamIndexFields = new List<(string[], bool)>
        {
            (new[] {"Id"}, false),
            (new[] {"OriginEntityCode"}, false)
        };

        private readonly IFeatureStore _features;

        public SqlServerConnector(
            IConfigurationRepository repository,
            ILogger<SqlServerConnector> logger,
            ISqlClient client,
            IFeatureStore features,
            ISqlServerConstants constants) : base(repository, logger, client, constants.ProviderId)
        {
            _client = client;
            _features = features ?? throw new ArgumentNullException(nameof(features));

            _bulkInsertThreshold =
                ConfigurationManagerEx.AppSettings.GetValue("Streams.SqlConnector.BulkInsertRecordCount", 0);
            _bulkDeleteThreshold =
                ConfigurationManagerEx.AppSettings.GetValue("Streams.SqlConnector.BulkDeleteRecordCount", 0);
            _bulkClient = client as IBulkSqlClient;
            _bulkSupported = _bulkInsertThreshold > 0 && _bulkClient != null;
            _logger.LogInformation($"{nameof(SqlServerConnector)} - bulk insert support enabled {{enabled}}",
                _bulkSupported);
            _syncEdgesTable =
                ConfigurationManagerEx.AppSettings.GetValue("Streams.SqlConnector.SyncEdgesTable", true);
        }

        public StreamMode StreamMode { get; private set; } = StreamMode.Sync;

        public virtual IList<StreamMode> GetSupportedModes()
        {
            return new List<StreamMode> { StreamMode.Sync, StreamMode.EventStream };
        }

        public virtual void SetMode(StreamMode mode)
        {
            StreamMode = mode;
        }

        public Task<string> GetCorrelationId()
        {
            return Task.FromResult(Guid.NewGuid().ToString());
        }

        public async Task StoreData(
            ExecutionContext executionContext,
            Guid providerDefinitionId,
            string containerName,
            string correlationId,
            DateTimeOffset timestamp,
            VersionChangeType changeType,
            IDictionary<string, object> data)
        {
            await ExecuteWithRetryAsync(async () =>
            {
                var config = await base.GetAuthenticationDetails(executionContext, providerDefinitionId);
                await using var connectionAndTransaction = await _client.BeginTransaction(config.Authentication);
                var transaction = connectionAndTransaction.Transaction;

                try
                {
                    // If we are in Event Stream mode, append extra fields
                    var dataToUse = new Dictionary<string, object>(data);
                    if (StreamMode == StreamMode.EventStream)
                    {
                        dataToUse.Add(TimestampFieldName, timestamp);
                        dataToUse.Add(ChangeTypeFieldName, changeType);
                        dataToUse.Add(CorrelationIdFieldName, correlationId);
                    }
                    else
                    {
                        dataToUse.Add(TimestampFieldName, timestamp);
                    }

                    var indexFieldsToUse = StreamMode == StreamMode.EventStream
                        ? _eventStreamIndexFields
                        : _syncStreamIndexFields;

                    var tableName = SqlTableName.FromUnsafeName(containerName, config);

                    if (_bulkSupported)
                    {
                        var bulkFeature = _features.GetFeature<IBulkStoreDataFeature>();
                        await bulkFeature.BulkTableUpdate(
                            executionContext,
                            providerDefinitionId,
                            tableName,
                            dataToUse,
                            _bulkInsertThreshold,
                            _bulkClient,
                            config,
                            _logger);
                    }
                    else
                    {
                        var feature = _features.GetFeature<IBuildStoreDataFeature>();
                        IEnumerable<SqlServerConnectorCommand> commands;
                        if (feature is IBuildStoreDataForMode modeFeature)
                        {
                            commands = modeFeature.BuildStoreDataSql(
                                executionContext,
                                providerDefinitionId,
                                tableName,
                                dataToUse,
                                uniqueColumns: indexFieldsToUse
                                    .Where(index => index.isUnique)
                                    .SelectMany(index => index.columns)
                                    .ToArray(),
                                StreamMode,
                                correlationId,
                                timestamp,
                                changeType,
                                _logger);
                        }
                        else
                        {
                            commands = feature.BuildStoreDataSql(
                                executionContext,
                                providerDefinitionId,
                                tableName,
                                dataToUse,
                                uniqueColumns: indexFieldsToUse
                                    .Where(index => index.isUnique)
                                    .SelectMany(index => index.columns)
                                    .ToArray(),
                                _logger);
                        }

                        foreach (var command in commands)
                        {
                            await _client.ExecuteCommandInTransactionAsync(transaction, command.Text, command.Parameters);
                        }
                    }
                }
                catch (Exception e)
                {
                    var message = $"Could not store data into Container '{containerName}' for Connector {providerDefinitionId}";
                    _logger.LogError(e, message);
                    await transaction.RollbackAsync();
                    throw new StoreDataException(message, e);
                }

                await transaction.CommitAsync();
            });
        }

        public async Task StoreEdgeData(ExecutionContext executionContext, Guid providerDefinitionId,
            string containerName, string originEntityCode, string correlationId, DateTimeOffset timestamp,
            VersionChangeType changeType, IEnumerable<string> edges)
        {
            await ExecuteWithRetryAsync(async () =>
            {
                var config = await base.GetAuthenticationDetails(executionContext, providerDefinitionId);
                await using (var connectionAndTransaction = await _client.BeginTransaction(config.Authentication))
                {
                    var transaction = connectionAndTransaction.Transaction;

                    var edgeTableName = SqlTableName.FromUnsafeName(GetEdgesContainerName(containerName), config);

                    var command = BuildEdgeStoreDataCommand(edgeTableName, originEntityCode, correlationId, edges, transaction);
                    await command.ExecuteNonQueryAsync();
                    await transaction.CommitAsync();
                }
            });
        }

        public async Task VerifyExistingContainer(ExecutionContext executionContext, StreamModel stream)
        {
            await ExecuteWithRetryAsync(async () =>
            {
                var config = await base.GetAuthenticationDetails(executionContext, stream.ConnectorProviderDefinitionId.Value);
                await using var connectionAndTransaction = await _client.BeginTransaction(config.Authentication);
                var transaction = connectionAndTransaction.Transaction;

                if (stream.ConnectorProviderDefinitionId.HasValue && stream.Mode is StreamMode.Sync)
                {
                    var log = executionContext.ApplicationContext.Container.Resolve<ILogger<SqlServerConnector>>();
                    log.LogInformation("Verifying that necessary columns and indexes exist for provider: {ProviderDefinitionId}", stream.ConnectorProviderDefinitionId);

                    var tableName = SqlTableName.FromUnsafeName(stream.ContainerName, config.GetSchema());
                    if (!await CheckTableExists(transaction, tableName))
                    {
                        // We should really be returning a message that we can't verify the existing container,
                        // but the interface doesn't allow for this, at this point.
                        log.LogInformation("Attempted to verify tables: `{tableName}`, but it did not exist", tableName);
                        await transaction.CommitAsync();
                        return;
                    }

                    var upgrade = _features.GetFeature<IUpgradeTimeStampingFeature>();
                    await upgrade.VerifyTimeStampColumnExist(_client, config, transaction, stream);

                    // Upsert custom type
                    {
                        var addCustomTypesFeature = _features.GetFeature<IAddCustomTypesFeature>();
                        var addCodeTableTypeText = addCustomTypesFeature.GetCreateCustomTypesCommandText();

                        var addCodeTableTypeSqlCommand = transaction.Connection.CreateCommand();
                        addCodeTableTypeSqlCommand.CommandText = addCodeTableTypeText;
                        addCodeTableTypeSqlCommand.Transaction = transaction;
                        await addCodeTableTypeSqlCommand.ExecuteNonQueryAsync();
                    }

                    var indexFieldsToUse = StreamMode == StreamMode.EventStream
                        ? _eventStreamIndexFields
                        : _syncStreamIndexFields;

                    var buildIndexFeature = _features.GetFeature<IBuildCreateIndexFeature>();
                    var verifyUniqueIndexFeature = _features.GetFeature<VerifyUniqueIndexFeature>();
                    var verifyUniqueIndexCommand = verifyUniqueIndexFeature.GetVerifyUniqueIndexCommand(buildIndexFeature, tableName, indexFieldsToUse);

                    var command = transaction.Connection.CreateCommand();
                    command.CommandText = verifyUniqueIndexCommand;
                    command.Transaction = transaction;

                    var response = await command.ExecuteScalarAsync();
                    if (response is int result && result == 0)
                    {
                        log.LogError("Could not add unique index on id, since there were duplicates in the table. To resolve, see upgrade notes");
                    }
                }

                await transaction.CommitAsync();
            });
        }

        public override async Task CreateContainer(ExecutionContext executionContext, Guid providerDefinitionId, CreateContainerModel model)
        {
            await ExecuteWithRetryAsync(async () =>
            {
                var config = await base.GetAuthenticationDetails(executionContext, providerDefinitionId);
                var schema = config.GetSchema();
                await using var connectionAndTransaction = await _client.BeginTransaction(config.Authentication);
                var transaction = connectionAndTransaction.Transaction;

                async Task CreateTable(SqlTableName tableName, IEnumerable<ConnectionDataType> columns, IEnumerable<(string[] columns, bool isUnique)> indexKeys, string context)
                {
                    try
                    {
                        var commands = _features
                            .GetFeature<IBuildCreateContainerFeature>()
                            .BuildCreateContainerSql(executionContext, providerDefinitionId, tableName, columns, Array.Empty<string>(), _logger)
                            .ToList();

                        var createIndexFeature = _features.GetFeature<IBuildCreateIndexFeature>();
                        var indexCommandText = string.Join(Environment.NewLine, indexKeys.Select(key => createIndexFeature.GetCreateIndexCommandText(tableName, key.columns, key.isUnique)));

                        commands.Add(new SqlServerConnectorCommand() { Text = indexCommandText });

                        var addCustomTypesFeature = _features.GetFeature<IAddCustomTypesFeature>();
                        var addCodeTableTypeText = addCustomTypesFeature.GetCreateCustomTypesCommandText();
                        commands.Add(new SqlServerConnectorCommand() { Text = addCodeTableTypeText });

                        foreach (var command in commands)
                        {
                            _logger.LogDebug("Sql Server Connector - Create Container[{Context}] - Generated query: {sql}", context, command.Text);

                            await _client.ExecuteCommandInTransactionAsync(transaction, command.Text, command.Parameters);
                        }
                    }
                    catch (Exception e)
                    {
                        var message = $"Could not create Container {tableName.FullyQualifiedName} for Connector {providerDefinitionId}";
                        _logger.LogError(e, message);
                        throw new CreateContainerException(message, e);
                    }
                }

                var container = new Container(model.Name, StreamMode);
                var codesTable = container.Tables["Codes"];

                var connectionDataTypes = model.DataTypes;
                if (StreamMode == StreamMode.EventStream)
                {
                    connectionDataTypes.Add(new ConnectionDataType
                    {
                        Name = TimestampFieldName, Type = VocabularyKeyDataType.DateTime
                    });
                    connectionDataTypes.Add(new ConnectionDataType
                    {
                        Name = ChangeTypeFieldName, Type = VocabularyKeyDataType.Text
                    });
                    connectionDataTypes.Add(new ConnectionDataType
                    {
                        Name = CorrelationIdFieldName, Type = VocabularyKeyDataType.Text
                    });
                }
                else
                {
                    connectionDataTypes.Add(new ConnectionDataType
                    {
                        Name = TimestampFieldName,
                        Type = VocabularyKeyDataType.DateTime
                    });
                }

                var indexFieldsToUse = StreamMode == StreamMode.EventStream
                    ? _eventStreamIndexFields
                    : _syncStreamIndexFields;

                var tasks = new List<Task>
                {
                    // Primary table
                    CreateTable(container.PrimaryTable.ToTableName(schema), connectionDataTypes, indexFieldsToUse, "Data"),

                    // Codes table
                    CreateTable(codesTable.Name.ToTableName(schema), codesTable.Columns, new[] { (codesTable.Keys.ToArray(), false) }, "Codes")
                };

                // We optionally build an edges table
                if (model.CreateEdgeTable)
                {
                    var edgesTable = container.Tables["Edges"];
                    tasks.Add(CreateTable(edgesTable.Name.ToTableName(schema), edgesTable.Columns, new[] { (codesTable.Keys.ToArray(), false) }, "Edges"));
                }

                await Task.WhenAll(tasks);
                await transaction.CommitAsync();
            });
        }

        public override async Task EmptyContainer(ExecutionContext executionContext, Guid providerDefinitionId, string id)
        {
            await ExecuteWithRetryAsync(async () =>
            {
                var config = await base.GetAuthenticationDetails(executionContext, providerDefinitionId);
                var schema = config.GetSchema();
                await using var connectionAndTransaction = await _client.BeginTransaction(config.Authentication);
                var transaction = connectionAndTransaction.Transaction;

                async Task EmptyTable(SqlTableName tableName, string context)
                {
                    var sql = BuildEmptyContainerSql(tableName);
                    _logger.LogDebug("Sql Server Connector - Empty Container[{Context}] - Generated query: {sql}", context, sql);

                    try
                    {
                        await _client.ExecuteCommandInTransactionAsync(transaction, sql);
                    }
                    catch (Exception e)
                    {
                        var message = $"Could not empty Container {tableName.FullyQualifiedName}";
                        _logger.LogError(e, message);
                        throw new CreateContainerException(message, e);
                    }
                }

                var container = new Container(id, StreamMode);
                var tasks = new List<Task> { EmptyTable(container.PrimaryTable.ToTableName(schema), "Data") };
                foreach (var table in container.Tables)
                {
                    var tableName = table.Value.Name.ToTableName(schema);

                    if (await CheckTableExists(transaction, tableName))
                    {
                        tasks.Add(EmptyTable(tableName, table.Key));
                    }
                }

                await Task.WhenAll(tasks);
                await transaction.CommitAsync();
            });
        }

        public override async Task ArchiveContainer(ExecutionContext executionContext, Guid providerDefinitionId, string id)
        {
            await ExecuteWithRetryAsync(async () =>
            {
                var config = await base.GetAuthenticationDetails(executionContext, providerDefinitionId);
                var schema = config.GetSchema();
                await using var connectionAndTransaction = await _client.BeginTransaction(config.Authentication);
                var transaction = connectionAndTransaction.Transaction;

                async Task ArchiveTable(SqlTableName tableName, string context)
                {
                    var newName = await GetValidContainerNameInTransaction(executionContext, providerDefinitionId, transaction, $"{tableName.LocalName}{DateTime.Now:yyyyMMddHHmmss}");

                    var sql = BuildRenameContainerSql(tableName, SqlName.FromUnsafe(newName), out var param);
                    _logger.LogDebug("Sql Server Connector - Archive Container[{Context}] - Generated query: {sql}", context, sql);

                    try
                    {
                        await _client.ExecuteCommandInTransactionAsync(transaction, sql, param);
                    }
                    catch (Exception e)
                    {
                        var message = $"Could not Archive Container {tableName.FullyQualifiedName} for Connector {providerDefinitionId}";
                        _logger.LogError(e, message);
                        throw new CreateContainerException(message, e);
                    }
                }

                var container = new Container(id, StreamMode);
                await ArchiveTable(container.PrimaryTable.ToTableName(schema), "Data");
                foreach (var table in container.Tables)
                {
                    var tableName = table.Value.Name.ToTableName(schema);

                    await ArchiveTable(tableName, table.Key);
                }

                await transaction.CommitAsync();
            });
        }

        public override async Task RenameContainer(ExecutionContext executionContext, Guid providerDefinitionId, string id, string newName)
        {
            await ExecuteWithRetryAsync(async () =>
            {
                var config = await base.GetAuthenticationDetails(executionContext, providerDefinitionId);
                var schema = config.GetSchema();
                await using var connectionAndTransaction = await _client.BeginTransaction(config.Authentication);
                var transaction = connectionAndTransaction.Transaction;

                async Task RenameTable(SqlTableName currentTableName, string updatedName, string context)
                {
                    var sql = BuildRenameContainerSql(currentTableName, SqlName.FromUnsafe(updatedName), out var param);

                    _logger.LogDebug("Sql Server Connector - Rename Container[{Context}] - Generated query: {sql}", context, sql);

                    try
                    {
                        await _client.ExecuteCommandInTransactionAsync(transaction, sql, param);
                    }
                    catch (Exception e)
                    {
                        var message = $"Could not Rename Container {currentTableName.FullyQualifiedName} for Connector {providerDefinitionId}";
                        _logger.LogError(e, message);
                        throw new CreateContainerException(message, e);
                    }
                }

                var currentContainer = new Container(id, StreamMode);
                var updatedContainer = new Container(newName, StreamMode);

                var tasks = new List<Task>
                {
                    RenameTable(currentContainer.PrimaryTable.ToTableName(schema), updatedContainer.PrimaryTable, "Data")
                };
                foreach (var current in currentContainer.Tables)
                {
                    var tableName = current.Value.Name.ToTableName(schema);

                    if (await CheckTableExists(transaction, tableName))
                    {
                        tasks.Add(RenameTable(tableName, updatedContainer.Tables[current.Key].Name, current.Key));
                    }
                }

                await Task.WhenAll(tasks);
                await transaction.CommitAsync();
            });
        }

        public override Task StoreData(ExecutionContext executionContext, Guid providerDefinitionId,
            string containerName, IDictionary<string, object> data)
        {
            return StoreData(executionContext, providerDefinitionId, containerName, null, DateTimeOffset.Now,
                VersionChangeType.NotSet, data);
        }

        public override Task StoreEdgeData(ExecutionContext executionContext, Guid providerDefinitionId,
            string containerName, string originEntityCode, IEnumerable<string> edges)
        {
            return StoreEdgeData(executionContext, providerDefinitionId, containerName, originEntityCode, null,
                DateTimeOffset.Now, VersionChangeType.NotSet, edges);
        }

        public override async Task RemoveContainer(ExecutionContext executionContext, Guid providerDefinitionId, string id)
        {
            await ExecuteWithRetryAsync(async () =>
            {
                var config = await base.GetAuthenticationDetails(executionContext, providerDefinitionId);
                var schema = config.GetSchema();
                await using var connectionAndTransaction = await _client.BeginTransaction(config.Authentication);
                var transaction = connectionAndTransaction.Transaction;

                async Task RemoveTable(SqlTableName tableName, string context)
                {
                    var sql = BuildRemoveContainerSql(tableName);

                    _logger.LogDebug("Sql Server Connector - Remove Container[{Context}] - Generated query: {sql}",
                        context, sql);

                    try
                    {
                        await _client.ExecuteCommandInTransactionAsync(transaction, sql);
                    }
                    catch (Exception e)
                    {
                        var message = $"Could not Remove Container {tableName.FullyQualifiedName} for Connector {providerDefinitionId}";
                        _logger.LogError(e, message);
                        throw new CreateContainerException(message, e);
                    }
                }

                var container = new Container(id, StreamMode);
                var tasks = new List<Task> { RemoveTable(container.PrimaryTable.ToTableName(schema), "Data") };
                foreach (var table in container.Tables)
                {
                    var tableName = table.Value.Name.ToTableName(schema);

                    if (await CheckTableExists(transaction, tableName))
                    {
                        tasks.Add(RemoveTable(tableName, table.Key));
                    }
                }

                await Task.WhenAll(tasks);
                await transaction.CommitAsync();
            });
        }

        public override async Task<IEnumerable<IConnectorContainer>> GetContainers(ExecutionContext executionContext, Guid providerDefinitionId)
        {
            var config = await base.GetAuthenticationDetails(executionContext, providerDefinitionId);
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
                var message = $"Could not get Containers for Connector {providerDefinitionId}";
                _logger.LogError(e, message);
                throw new GetContainersException(message, e);
            }
        }

        public override async Task<IEnumerable<IConnectionDataType>> GetDataTypes(ExecutionContext executionContext, Guid providerDefinitionId, string containerId)
        {
            var config = await base.GetAuthenticationDetails(executionContext, providerDefinitionId);
            await using var connectionAndTransaction = await _client.BeginTransaction(config.Authentication);
            var transaction = connectionAndTransaction.Transaction;

            try
            {
                var tables = await _client.GetTableColumns(transaction, containerId, schema: config.GetSchema());

                var result = from DataRow row in tables.Rows
                             let name = row["COLUMN_NAME"] as string
                             let rawType = row["DATA_TYPE"] as string
                             let type = VocabularyKeyDataType.Text
                             select new SqlServerConnectorDataType { Name = name, RawDataType = rawType, Type = type };

                return result.ToList();
            }
            catch (Exception e)
            {
                var message = $"Could not get Data types for Container '{containerId}' for Connector {providerDefinitionId}";
                _logger.LogError(e, message);
                throw new GetDataTypesException(message, e);
            }
        }

        public override async Task DeleteData(
            ExecutionContext executionContext,
            Guid providerDefinitionId,
            string containerName,
            string originEntityCode,
            IList<IEntityCode> codes,
            Guid entityId)
        {
            await ExecuteWithRetryAsync(async () =>
            {
                var config = await base.GetAuthenticationDetails(executionContext, providerDefinitionId);
                await using var connectionAndTransaction = await _client.BeginTransaction(config.Authentication);
                var transaction = connectionAndTransaction.Transaction;
                var schema = config.GetSchema();

                try
                {
                    if (_bulkSupported)
                    {
                        var tableName = SqlTableName.FromUnsafeName(containerName, schema);

                        var bulkFeature = _features.GetFeature<IBulkDeleteDataFeature>();
                        await bulkFeature.BulkTableDelete(
                            executionContext,
                            providerDefinitionId,
                            tableName,
                            originEntityCode,
                            codes,
                            entityId,
                            _bulkInsertThreshold,
                            _bulkClient,
                            config,
                            _logger);
                    }
                    else
                    {
                        var container = new Container(containerName, StreamMode);
                        var deleteFeature = _features.GetFeature<IBuildDeleteDataFeature>();
                        var commands = deleteFeature
                            .BuildDeleteDataSql(executionContext, providerDefinitionId,
                                container.PrimaryTable.ToTableName(schema),
                                originEntityCode, codes, entityId, _logger);

                        // We need to origin entity code for entities that have been deleted
                        // see if we need to delete linked tables
                        // do look up of OriginEntityCode from current table data
                        var lookupOriginCodes = new List<string>();
                        var cmd = transaction.Connection.CreateCommand();
                        cmd.CommandText =
                            $"SELECT DISTINCT OriginEntityCode FROM {container.PrimaryTable.ToTableName(schema).FullyQualifiedName} WHERE [Id] = @Id;";
                        cmd.Parameters.Add(new SqlParameter("Id", entityId));
                        cmd.Transaction = transaction;

                        await using var resp = await cmd.ExecuteReaderAsync();
                        if (resp.HasRows)
                        {
                            while (await resp.ReadAsync())
                            {
                                lookupOriginCodes.Add(resp.GetString(0));
                            }
                        }

                        foreach (var table in container.Tables)
                        {
                            var tName = table.Value.Name.ToTableName(schema);

                            if (await CheckTableExists(transaction, tName))
                            {
                                foreach (var entry in lookupOriginCodes)
                                {
                                    commands = commands.Concat(deleteFeature.BuildDeleteDataSql(executionContext,
                                        providerDefinitionId, tName, entry, null, null, _logger));
                                }
                            }
                        }

                        foreach (var command in commands)
                            await _client.ExecuteCommandInTransactionAsync(transaction, command.Text, command.Parameters);
                    }
                }
                catch (Exception e)
                {
                    var message = $"Could not delete data from Container '{containerName}' for Connector {providerDefinitionId}";
                    _logger.LogError(e, message);
                    transaction.Rollback();
                    throw new StoreDataException(message, e);
                }

                await transaction.CommitAsync();
            });
        }


        internal SqlCommand BuildEdgeStoreDataCommand(SqlTableName tableName, string originEntityCode, string correlationId, IEnumerable<string> edges, SqlTransaction transaction)
        {
            var sqlMetaData = new SqlMetaData[1];
            sqlMetaData[0] = new SqlMetaData("Code", SqlDbType.NVarChar, SqlMetaData.Max);

            var codesSqlMetaData = edges.Any()
                ? edges.Select(edge =>
                {
                    var record = new SqlDataRecord(sqlMetaData);
                    record.SetSqlString(0, edge);
                    return record;
                })
                : null;

            var command = transaction.Connection.CreateCommand();
            command.Transaction = transaction;
            
            var originEntityCodeParameter = new SqlParameter("@OriginEntityCode", SqlDbType.NVarChar) { Value = originEntityCode };
            command.Parameters.Add(originEntityCodeParameter);

            var codesTableParameter = command.Parameters.AddWithValue("@codesTable", codesSqlMetaData);
            codesTableParameter.SqlDbType = SqlDbType.Structured;
            codesTableParameter.TypeName = "dbo.CodeTableType";

            if (StreamMode == StreamMode.EventStream)
            {
                var sqlText = @$"
INSERT INTO {tableName.FullyQualifiedName} (OriginEntityCode, CorrelationId, Code)
SELECT @OriginEntityCode, @correlationId, codes.Code
FROM @codesTable codes
LEFT JOIN {tableName.FullyQualifiedName} existingEdges
ON existingEdges.OriginEntityCode = @OriginEntityCode AND existingEdges.Code = codes.Code
WHERE existingEdges.OriginEntityCode IS NULL
";
                command.CommandText = sqlText;
                var correlationParameter = new SqlParameter("@correlationId", SqlDbType.NVarChar) { Value = correlationId };
                command.Parameters.Add(correlationParameter);
            }
            else
            {
                var sqlText = @$"
DELETE {tableName.FullyQualifiedName}
WHERE OriginEntityCode = @OriginEntityCode AND 
      NOT EXISTS (SELECT 1 FROM @codesTable codes WHERE codes.Code = {tableName.FullyQualifiedName}.Code)

INSERT INTO {tableName.FullyQualifiedName} (OriginEntityCode, Code)
SELECT @OriginEntityCode, codes.Code
FROM @codesTable codes
LEFT JOIN {tableName.FullyQualifiedName} existingEdges
ON existingEdges.OriginEntityCode = @OriginEntityCode AND existingEdges.Code = codes.Code
WHERE existingEdges.OriginEntityCode IS NULL
";
                command.CommandText = sqlText;
            }

            return command;
        }

        private string BuildRenameContainerSql(SqlTableName tableName, SqlName newName, out List<SqlParameter> param)
        {
            param = new List<SqlParameter>
            {
                new SqlParameter("@tableOldName", SqlDbType.NVarChar) { Value = tableName.LocalName.Value },
                new SqlParameter("@tableOldFQName", SqlDbType.NVarChar) { Value = tableName.FullyQualifiedName },
                new SqlParameter("@newTableName", SqlDbType.NVarChar) { Value = newName.Value },
                new SqlParameter("@schema", SqlDbType.NVarChar) { Value = tableName.Schema.Value }

            };

            return "IF EXISTS(SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = @tableOldName AND TABLE_SCHEMA = @schema) EXEC sp_rename @tableOldFQName, @newTableName";
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

        protected override async Task<bool> CheckTableExists(ExecutionContext executionContext, Guid providerDefinitionId, SqlTransaction transaction, string name)
        {
            var config = await GetAuthenticationDetails(executionContext, providerDefinitionId);
            var tableName = SqlTableName.FromUnsafeName(name, config);

            return await CheckTableExists(transaction, tableName);
        }

        private static async Task ExecuteWithRetryAsync(Func<Task> taskFunc)
        {
            await taskFunc.ExecuteWithRetryAsync(isTransient: ExceptionExtensions.IsTransient);
        }
    }
}
