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
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CluedIn.Connector.SqlServer.Connector
{
    public class SqlServerConnector : SqlConnectorBase<SqlServerConnector, SqlConnection, SqlParameter>, IConnectorStreamModeSupport,
        IConnectorUpgrade
    {
        private const string TimestampFieldName = "TimeStamp";
        private const string ChangeTypeFieldName = "ChangeType";
        private const string CorrelationIdFieldName = "CorrelationId";
        private readonly IBulkSqlClient _bulkClient;
        private readonly int _bulkDeleteThreshold;
        private readonly int _bulkInsertThreshold;
        private readonly bool _bulkSupported;
        private readonly bool _syncEdgesTable;
        private readonly IList<string> _defaultKeyFields = new List<string> { "Id", "OriginEntityCode" };

        private readonly IFeatureStore _features;

        public SqlServerConnector(
            IConfigurationRepository repository,
            ILogger<SqlServerConnector> logger,
            ISqlClient client,
            IFeatureStore features,
            ISqlServerConstants constants) : base(repository, logger, client, constants.ProviderId)
        {
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
                    dataToUse.Add(TimestampFieldName, timestamp);

                var config = await base.GetAuthenticationDetails(executionContext, providerDefinitionId);

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
                        commands = modeFeature.BuildStoreDataSql(executionContext, providerDefinitionId, tableName,
                            dataToUse, _defaultKeyFields, StreamMode, correlationId, timestamp, changeType, _logger);
                    else
                        commands = feature.BuildStoreDataSql(executionContext, providerDefinitionId, tableName,
                            dataToUse, _defaultKeyFields, _logger);

                    foreach (var command in commands)
                        await ExecuteCommandWithRetryAsync(() => _client.ExecuteCommandAsync(config, command.Text, command.Parameters));
                }
            }
            catch (Exception e)
            {
                var message =
                    $"Could not store data into Container '{containerName}' for Connector {providerDefinitionId}";
                _logger.LogError(e, message);
                throw new StoreDataException(message);
            }
        }

        public async Task StoreEdgeData(ExecutionContext executionContext, Guid providerDefinitionId,
            string containerName, string originEntityCode, string correlationId, DateTimeOffset timestamp,
            VersionChangeType changeType, IEnumerable<string> edges)
        {
            try
            {
                var config = await base.GetAuthenticationDetails(executionContext, providerDefinitionId);

                var edgeTableName = SqlTableName.FromUnsafeName(GetEdgesContainerName(containerName), config);

                if (await CheckTableExists(config, edgeTableName))
                {
                    var sql = BuildEdgeStoreDataSql(edgeTableName, originEntityCode, correlationId, edges, out var param);

                    if (!string.IsNullOrWhiteSpace(sql))
                    {
                        _logger.LogDebug($"Sql Server Connector - Store Edge Data - Generated query: {sql}");

                        await ExecuteCommandWithRetryAsync(() => _client.ExecuteCommandAsync(config, sql, param));
                    }
                }
            }
            catch (Exception e)
            {
                var message = $"Could not store edge data into Container '{containerName}' for Connector {providerDefinitionId}";
                _logger.LogError(e, message);
                throw new StoreDataException(message);
            }
        }

        public async Task VerifyExistingContainer(ExecutionContext executionContext, StreamModel stream)
        {
            if (stream.ConnectorProviderDefinitionId.HasValue && stream.Mode is StreamMode.Sync)
            {
                var log = executionContext.ApplicationContext.Container.Resolve<ILogger<SqlServerConnector>>();
                log.LogInformation("Verifying that necessary columns and indexes exist for provider: {ProviderDefinitionId}", stream.ConnectorProviderDefinitionId);

                var upgrade = _features.GetFeature<IUpgradeTimeStampingFeature>();
                var config = await base.GetAuthenticationDetails(executionContext, stream.ConnectorProviderDefinitionId.Value);
                await ExecuteCommandWithRetryAsync(() => upgrade.VerifyTimeStampColumnExist(_client as ISqlClient, config, stream));

                await using (var connection = await ExecuteResultCommandWithRetryAsync(() => _client.GetConnection(config.Authentication)))
                {
                    var buildIndexFeature = _features.GetFeature<IBuildCreateIndexFeature>();
                    var verifyUniqueIndexFeature = _features.GetFeature<VerifyUniqueIndexFeature>();
                    var tableName = SqlTableName.FromUnsafeName(stream.ContainerName, config.GetSchema());
                    var verifyUniqueIndexCommand = verifyUniqueIndexFeature.GetVerifyUniqueIndexCommand(buildIndexFeature, tableName, _defaultKeyFields);

                    var command = connection.CreateCommand();
                    command.CommandText = verifyUniqueIndexCommand;

                    var response = await ExecuteResultCommandWithRetryAsync(() => command.ExecuteScalarAsync());
                    if (response is int result && result == 0)
                    {
                        log.LogError("Could not add unique index on id, since there were duplicates in the table. To resolve, see upgrade notes");
                    }
                }
            }
        }

        public override async Task CreateContainer(ExecutionContext executionContext, Guid providerDefinitionId, CreateContainerModel model)
        {
            var config = await base.GetAuthenticationDetails(executionContext, providerDefinitionId);
            var schema = config.GetSchema();

            async Task CreateTable(SqlTableName tableName, IEnumerable<ConnectionDataType> columns, IEnumerable<string> keys, string context, bool useUniqueIndex)
            {
                try
                {
                    var commands = _features
                        .GetFeature<IBuildCreateContainerFeature>()
                        .BuildCreateContainerSql(executionContext, providerDefinitionId, tableName, columns, keys, _logger)
                        .ToList();

                    var indexCommand = _features.GetFeature<IBuildCreateIndexFeature>()
                        .BuildCreateIndexSql(tableName, keys, useUniqueIndex);
                    commands.Add(indexCommand);

                    foreach (var command in commands)
                    {
                        _logger.LogDebug("Sql Server Connector - Create Container[{Context}] - Generated query: {sql}",
                            context, command.Text);

                        await ExecuteCommandWithRetryAsync(() => _client.ExecuteCommandAsync(config, command.Text, command.Parameters));
                    }
                }
                catch (Exception e)
                {
                    var message = $"Could not create Container {tableName.FullyQualifiedName} for Connector {providerDefinitionId}";
                    _logger.LogError(e, message);
                    throw new CreateContainerException(message);
                }
            }

            var container = new Container(model.Name, StreamMode);
            var codesTable = container.Tables["Codes"];

            var connectionDataTypes = model.DataTypes;
            if (StreamMode == StreamMode.EventStream)
            {
                connectionDataTypes.Add(new ConnectionDataType
                {
                    Name = TimestampFieldName,
                    Type = VocabularyKeyDataType.DateTime
                });
                connectionDataTypes.Add(new ConnectionDataType
                {
                    Name = ChangeTypeFieldName,
                    Type = VocabularyKeyDataType.Text
                });
                connectionDataTypes.Add(new ConnectionDataType
                {
                    Name = CorrelationIdFieldName,
                    Type = VocabularyKeyDataType.Text
                });
            }
            else
                connectionDataTypes.Add(new ConnectionDataType
                {
                    Name = TimestampFieldName,
                    Type = VocabularyKeyDataType.DateTime
                });

            var tasks = new List<Task>
            {
                // Primary table
                CreateTable(container.PrimaryTable.ToTableName(schema), connectionDataTypes, _defaultKeyFields, "Data", true),

                // Codes table
                CreateTable(codesTable.Name.ToTableName(schema), codesTable.Columns, codesTable.Keys, "Codes", false)
            };

            // We optionally build an edges table
            if (model.CreateEdgeTable)
            {
                var edgesTable = container.Tables["Edges"];
                tasks.Add(CreateTable(edgesTable.Name.ToTableName(schema), edgesTable.Columns, edgesTable.Keys, "Edges", false));
            }

            await Task.WhenAll(tasks);
        }

        public override async Task EmptyContainer(ExecutionContext executionContext, Guid providerDefinitionId, string id)
        {
            var config = await base.GetAuthenticationDetails(executionContext, providerDefinitionId);
            var schema = config.GetSchema();

            async Task EmptyTable(SqlTableName tableName, string context)
            {
                var sql = BuildEmptyContainerSql(tableName);
                _logger.LogDebug("Sql Server Connector - Empty Container[{Context}] - Generated query: {sql}", context, sql);

                try
                {
                    await ExecuteCommandWithRetryAsync(() => _client.ExecuteCommandAsync(config, sql));
                }
                catch (Exception e)
                {
                    var message = $"Could not empty Container {tableName.FullyQualifiedName}";
                    _logger.LogError(e, message);
                    throw new CreateContainerException(message);
                }
            }

            var container = new Container(id, StreamMode);
            var tasks = new List<Task> { EmptyTable(container.PrimaryTable.ToTableName(schema), "Data") };
            foreach (var table in container.Tables)
            {
                var tableName = table.Value.Name.ToTableName(schema);

                if (await CheckTableExists(config, tableName))
                {
                    tasks.Add(EmptyTable(tableName, table.Key));
                }
            }

            await Task.WhenAll(tasks);
        }

        public override async Task ArchiveContainer(ExecutionContext executionContext, Guid providerDefinitionId, string id)
        {
            var config = await base.GetAuthenticationDetails(executionContext, providerDefinitionId);
            var schema = config.GetSchema();

            async Task ArchiveTable(SqlTableName tableName, string context)
            {
                var newName = await GetValidContainerName(executionContext, providerDefinitionId, $"{tableName.LocalName}{DateTime.Now:yyyyMMddHHmmss}");

                var sql = BuildRenameContainerSql(tableName, SqlName.FromUnsafe(newName), out var param);
                _logger.LogDebug("Sql Server Connector - Archive Container[{Context}] - Generated query: {sql}", context, sql);

                try
                {
                    await ExecuteCommandWithRetryAsync(() => _client.ExecuteCommandAsync(config, sql, param));
                }
                catch (Exception e)
                {
                    var message = $"Could not Archive Container {tableName.FullyQualifiedName} for Connector {providerDefinitionId}";
                    _logger.LogError(e, message);
                    throw new CreateContainerException(message);
                }
            }

            var container = new Container(id, StreamMode);
            var tasks = new List<Task> { ArchiveTable(container.PrimaryTable.ToTableName(schema), "Data") };
            foreach (var table in container.Tables)
            {
                var tableName = table.Value.Name.ToTableName(schema);

                if (await CheckTableExists(config, tableName))
                {
                    tasks.Add(ArchiveTable(tableName, table.Key));
                }
            }

            await Task.WhenAll(tasks);
        }

        public override async Task RenameContainer(ExecutionContext executionContext, Guid providerDefinitionId, string id, string newName)
        {
            var config = await base.GetAuthenticationDetails(executionContext, providerDefinitionId);
            var schema = config.GetSchema();

            async Task RenameTable(SqlTableName currentTableName, string updatedName, string context)
            {
                var sql = BuildRenameContainerSql(currentTableName, SqlName.FromUnsafe(updatedName), out var param);

                _logger.LogDebug("Sql Server Connector - Rename Container[{Context}] - Generated query: {sql}", context, sql);

                try
                {
                    await ExecuteCommandWithRetryAsync(() => _client.ExecuteCommandAsync(config, sql, param));
                }
                catch (Exception e)
                {
                    var message = $"Could not Rename Container {currentTableName.FullyQualifiedName} for Connector {providerDefinitionId}";
                    _logger.LogError(e, message);
                    throw new CreateContainerException(message);
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

                if (await CheckTableExists(config, tableName))
                {
                    tasks.Add(RenameTable(tableName, updatedContainer.Tables[current.Key].Name, current.Key));
                }
            }

            await Task.WhenAll(tasks);
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
            var config = await base.GetAuthenticationDetails(executionContext, providerDefinitionId);
            var schema = config.GetSchema();

            async Task RemoveTable(SqlTableName tableName, string context)
            {
                var sql = BuildRemoveContainerSql(tableName);

                _logger.LogDebug("Sql Server Connector - Remove Container[{Context}] - Generated query: {sql}", context, sql);

                try
                {
                    await ExecuteCommandWithRetryAsync(() => _client.ExecuteCommandAsync(config, sql));
                }
                catch (Exception e)
                {
                    var message = $"Could not Remove Container {tableName.FullyQualifiedName} for Connector {providerDefinitionId}";
                    _logger.LogError(e, message);
                    throw new CreateContainerException(message);
                }
            }

            var container = new Container(id, StreamMode);
            var tasks = new List<Task> { RemoveTable(container.PrimaryTable.ToTableName(schema), "Data") };
            foreach (var table in container.Tables)
            {
                var tableName = table.Value.Name.ToTableName(schema);

                if (await CheckTableExists(config, tableName))
                {
                    tasks.Add(RemoveTable(tableName, table.Key));
                }
            }

            await Task.WhenAll(tasks);
        }

        public override async Task<IEnumerable<IConnectorContainer>> GetContainers(ExecutionContext executionContext, Guid providerDefinitionId)
        {
            try
            {
                var config = await base.GetAuthenticationDetails(executionContext, providerDefinitionId);
                var tables = await ExecuteResultCommandWithRetryAsync(() => _client.GetTables(config.Authentication, schema: config.GetSchema()));

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
                throw new GetContainersException(message);
            }
        }

        public override async Task<IEnumerable<IConnectionDataType>> GetDataTypes(ExecutionContext executionContext, Guid providerDefinitionId, string containerId)
        {
            try
            {
                var config = await base.GetAuthenticationDetails(executionContext, providerDefinitionId);
                var tables = await ExecuteResultCommandWithRetryAsync(() => _client.GetTableColumns(config.Authentication, containerId, schema: config.GetSchema()));

                var result = from DataRow row in tables.Rows
                             let name = row["COLUMN_NAME"] as string
                             let rawType = row["DATA_TYPE"] as string
                             let type = VocabularyKeyDataType.Text
                             select new SqlServerConnectorDataType { Name = name, RawDataType = rawType, Type = type };

                return result.ToList();
            }
            catch (Exception e)
            {
                var message =
                    $"Could not get Data types for Container '{containerId}' for Connector {providerDefinitionId}";
                _logger.LogError(e, message);
                throw new GetDataTypesException(message);
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
            try
            {
                var config = await base.GetAuthenticationDetails(executionContext, providerDefinitionId);
                var schema = config.GetSchema();

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
                        .BuildDeleteDataSql(executionContext, providerDefinitionId, container.PrimaryTable.ToTableName(schema),
                            originEntityCode, codes, entityId, _logger);

                    // We need to origin entity code for entities that have been deleted
                    // see if we need to delete linked tables
                    // do look up of OriginEntityCode from current table data
                    var lookupOriginCodes = new List<string>();
                    await using (var connection = await ExecuteResultCommandWithRetryAsync(() => _client.GetConnection(config.Authentication)))
                    {
                        var cmd = connection.CreateCommand();
                        cmd.CommandText = $"SELECT DISTINCT OriginEntityCode FROM {container.PrimaryTable.ToTableName(schema).FullyQualifiedName} WHERE [Id] = @Id;";
                        cmd.Parameters.Add(new SqlParameter("Id", entityId));

                        await using var resp = await ExecuteResultCommandWithRetryAsync(() => cmd.ExecuteReaderAsync());
                        if (resp.HasRows)
                        {
                            while (await resp.ReadAsync())
                            {
                                lookupOriginCodes.Add(resp.GetString(0));
                            }
                        }
                    }

                    foreach (var table in container.Tables)
                    {
                        var tName = table.Value.Name.ToTableName(schema);

                        if (await CheckTableExists(config, tName))
                        {
                            foreach (var entry in lookupOriginCodes)
                            {
                                commands = commands.Concat(deleteFeature.BuildDeleteDataSql(executionContext, providerDefinitionId, tName, entry, null, null, _logger));
                            }
                        }
                    }

                    foreach (var command in commands)
                        await ExecuteCommandWithRetryAsync(() => _client.ExecuteCommandAsync(config, command.Text, command.Parameters));
                }
            }
            catch (Exception e)
            {
                var message = $"Could not delete data from Container '{containerName}' for Connector {providerDefinitionId}";
                _logger.LogError(e, message);
                throw new StoreDataException(message);
            }
        }


        internal string BuildEdgeStoreDataSql(SqlTableName tableName, string originEntityCode, string correlationId, IEnumerable<string> edges, out List<SqlParameter> param)
        {
            var originParam = new SqlParameter { ParameterName = "@OriginEntityCode", Value = originEntityCode };
            var correlationParam = new SqlParameter { ParameterName = "@CorrelationId", Value = correlationId };
            param = new List<SqlParameter> { originParam, correlationParam };

            var builder = new StringBuilder();

            if (StreamMode == StreamMode.Sync && _syncEdgesTable)
                builder.AppendLine(
                    $"DELETE FROM {tableName.FullyQualifiedName} where [OriginEntityCode] = {originParam.ParameterName}");

            var edgeValues = new List<string>();
            foreach (var edge in edges)
            {
                var edgeParam = new SqlParameter { ParameterName = $"@{edgeValues.Count}", Value = edge };
                param.Add(edgeParam);

                if (StreamMode == StreamMode.EventStream)
                    edgeValues.Add($"(@OriginEntityCode, @CorrelationId, {edgeParam.ParameterName})");
                else
                    edgeValues.Add($"(@OriginEntityCode, {edgeParam.ParameterName})");
            }

            if (edgeValues.Count <= 0)
                return builder.ToString();

            builder.AppendLine(
                StreamMode == StreamMode.EventStream
                    ? $"INSERT INTO {tableName.FullyQualifiedName} ([OriginEntityCode],[CorrelationId],[Code]) values"
                    : $"INSERT INTO {tableName.FullyQualifiedName} ([OriginEntityCode],[Code]) values");

            builder.AppendJoin(", ", edgeValues);

            return builder.ToString();
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

        protected async Task<bool> CheckTableExists(IConnectorConnection config, SqlTableName tableName)
        {
            try
            {
                var tables = await ExecuteResultCommandWithRetryAsync(() => _client.GetTables(config.Authentication, name: tableName.LocalName, schema: tableName.Schema));

                return tables.Rows.Count > 0;
            }
            catch (Exception e)
            {
                _logger.LogError(e, $"Error checking Container '{tableName}' exists");
                return false;
            }
        }

        protected override async Task<bool> CheckTableExists(ExecutionContext executionContext, Guid providerDefinitionId, string name)
        {
            var config = await GetAuthenticationDetails(executionContext, providerDefinitionId);
            var tableName = SqlTableName.FromUnsafeName(name, config);

            return await CheckTableExists(config, tableName);
        }

        private static async Task ExecuteCommandWithRetryAsync(Func<Task> command)
        {
            await command.ExecuteWithRetryAsync(isTransient: ExceptionExtensions.IsTransient);
        }

        private static async Task<T> ExecuteResultCommandWithRetryAsync<T>(Func<Task<T>> command)
        {
            return await command.ExecuteWithRetryAsync(isTransient: ExceptionExtensions.IsTransient);
        }
    }
}
