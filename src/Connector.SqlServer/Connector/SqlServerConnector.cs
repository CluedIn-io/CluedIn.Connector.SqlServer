using CluedIn.Connector.Common.Connectors;
using CluedIn.Connector.Common.Features;
using CluedIn.Connector.Common.Helpers;
using CluedIn.Connector.SqlServer.Features;
using CluedIn.Connector.SqlServer.Utility;
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
        private readonly IList<string> _defaultKeyFields = new List<string> { "OriginEntityCode" };

        private readonly IFeatureStore _features;

        public SqlServerConnector(
            IConfigurationRepository repository,
            ILogger<SqlServerConnector> logger,
            ISqlClient client,
            IFeatureStore features,
            ISqlServerConstants constants) : base(repository, logger, client, constants.ProviderId)
        {
            _features = features ?? throw new ArgumentNullException(nameof(logger));

            _bulkInsertThreshold =
                ConfigurationManagerEx.AppSettings.GetValue("Streams.SqlConnector.BulkInsertRecordCount", 0);
            _bulkDeleteThreshold =
                ConfigurationManagerEx.AppSettings.GetValue("Streams.SqlConnector.BulkDeleteRecordCount", 0);
            _bulkClient = _client as IBulkSqlClient;
            _bulkSupported = _bulkInsertThreshold > 0 && _bulkClient != null;
            _logger.LogInformation($"{nameof(SqlServerConnector)} - bulk insert support enabled {_bulkSupported}");
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

        public async Task StoreData(ExecutionContext executionContext, Guid providerDefinitionId, string containerName,
            string correlationId, DateTimeOffset timestamp, VersionChangeType changeType,
            IDictionary<string, object> data)
        {
            var tableName = new SanitizedSqlString(containerName);
            var dataToUse = new Dictionary<string, object>(data)
                {
                    { TimestampFieldName, timestamp }
                };

            // If we are in Event Stream mode, append extra fields
            if (StreamMode == StreamMode.EventStream)
            {
                dataToUse.Add(ChangeTypeFieldName, changeType);
                dataToUse.Add(CorrelationIdFieldName, correlationId);
            }

            try
            {
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
                        () => GetAuthenticationDetails(executionContext, providerDefinitionId),
                        _logger);
                }
                else
                {
                    var config = await GetAuthenticationDetails(executionContext, providerDefinitionId);
                    var feature = _features.GetFeature<IBuildStoreDataFeature>();
                    var schema = config.GetSchema();
                    IEnumerable<SqlServerConnectorCommand> commands;
                    if (feature is IBuildStoreDataForMode modeFeature)
                    {
                        commands = modeFeature.BuildStoreDataSql(executionContext: executionContext,
                            providerDefinitionId: providerDefinitionId,
                            schema: schema,
                            tableName: tableName,
                            data: dataToUse,
                            keys: _defaultKeyFields,
                            mode: StreamMode,
                            correlationId: correlationId,
                            timestamp: timestamp,
                            changeType: changeType,
                            logger: _logger);
                    }
                    else
                    {
                        commands = feature.BuildStoreDataSql(executionContext: executionContext,
                            providerDefinitionId: providerDefinitionId,
                            schema: schema,
                            tableName: tableName,
                            data: dataToUse,
                            keys: _defaultKeyFields,
                            logger: _logger);
                    }

                    foreach (var command in commands)
                        await _client.ExecuteCommandAsync(config, command.Text, command.Parameters);
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
                var edgeTableName = new SanitizedSqlString(GetEdgesContainerName(containerName));
                if (await CheckTableExists(executionContext, providerDefinitionId, edgeTableName.GetValue()))
                {
                    var config = await GetAuthenticationDetails(executionContext, providerDefinitionId);
                    var schema = config.GetSchema();
                    var sql = BuildEdgeStoreDataSql(schema, edgeTableName, originEntityCode, correlationId, edges,
                        out var param);

                    if (!string.IsNullOrWhiteSpace(sql))
                    {
                        _logger.LogDebug($"Sql Server Connector - Store Edge Data - Generated query: {sql}");
                        await _client.ExecuteCommandAsync(config, sql, param);
                    }
                }
            }
            catch (Exception e)
            {
                var message =
                    $"Could not store edge data into Container '{containerName}' for Connector {providerDefinitionId}";
                _logger.LogError(e, message);
                throw new StoreDataException(message);
            }
        }

        public async Task VerifyExistingContainer(ExecutionContext executionContext, StreamModel stream)
        {
            if (stream.ConnectorProviderDefinitionId.HasValue)
            {
                var upgrade = _features.GetFeature<ITimeStampingFeature>();
                var config =
                    await base.GetAuthenticationDetails(executionContext, stream.ConnectorProviderDefinitionId.Value);

                await upgrade.VerifyTimeStampColumnExist(_client as ISqlClient, config, stream);
            }
        }

        public override async Task CreateContainer(ExecutionContext executionContext, Guid providerDefinitionId,
            CreateContainerModel model)
        {
            var config = await base.GetAuthenticationDetails(executionContext, providerDefinitionId);
            var schema = config.GetSchema();
            var container = new Container(model.Name, StreamMode);

            //LDM TODO: This code piece could be better placed inside Container creation.
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

            var codesTable = container.Tables["Codes"];
            var tasks = new List<Task>
            {
                // Primary table
                createTable(container.PrimaryTable, connectionDataTypes, _defaultKeyFields, "Data"),

                // Codes table
                createTable(codesTable.Name, codesTable.Columns, codesTable.Keys, "Codes")
            };

            // We optionally build an edges table
            if (model.CreateEdgeTable)
            {
                var edgesTable = container.Tables["Edges"];
                tasks.Add(createTable(edgesTable.Name, edgesTable.Columns, edgesTable.Keys, "Edges"));
            }

            await Task.WhenAll(tasks);

            async Task createTable(SanitizedSqlString tableName, IEnumerable<ConnectionDataType> columns, IEnumerable<string> keys,
                string context)
            {
                try
                {
                    IEnumerable<SqlServerConnectorCommand> commands = _features
                        .GetFeature<IBuildCreateContainerFeature>()
                        .BuildCreateContainerSql(executionContext, providerDefinitionId, schema, tableName, columns, keys, _logger)
                        .ToList();

                    var indexCommands = _features.GetFeature<IBuildCreateIndexFeature>()
                        .BuildCreateIndexSql(executionContext, providerDefinitionId, schema, tableName, keys, _logger);
                    commands = commands.Union(indexCommands);

                    foreach (var command in commands)
                    {
                        _logger.LogDebug("Sql Server Connector - Create Container[{Context}] - Generated query: {sql}",
                            context, command.Text);

                        await _client.ExecuteCommandAsync(config, command.Text, command.Parameters);
                    }
                }
                catch (Exception e)
                {
                    var message = $"Could not create Container {tableName} for Connector {providerDefinitionId}";
                    _logger.LogError(e, message);
                    throw new CreateContainerException(message);
                }
            }
        }

        public override async Task EmptyContainer(ExecutionContext executionContext, Guid providerDefinitionId,
            string id)
        {
            var config = await base.GetAuthenticationDetails(executionContext, providerDefinitionId);
            var schema = config.GetSchema();
            var container = new Container(id, StreamMode);
            var tasks = new List<Task> { emptyTable(container.PrimaryTable, "Data") };
            foreach (var table in container.Tables)
                if (await CheckTableExists(executionContext, providerDefinitionId, table.Value.Name.GetValue()))
                    tasks.Add(emptyTable(table.Value.Name, table.Key));

            await Task.WhenAll(tasks);

            async Task emptyTable(SanitizedSqlString tableName, string context)
            {
                var sql = BuildEmptyContainerSql(schema, tableName);
                _logger.LogDebug("Sql Server Connector - Empty Container[{Context}] - Generated query: {sql}", context,
                    sql);

                try
                {
                    await _client.ExecuteCommandAsync(config, sql);
                }
                catch (Exception e)
                {
                    var message = $"Could not empty Table {tableName}";
                    _logger.LogError(e, message);
                    throw new CreateContainerException(message);
                }
            }
        }

        public override async Task ArchiveContainer(ExecutionContext executionContext, Guid providerDefinitionId,
            string id)
        {
            var config = await base.GetAuthenticationDetails(executionContext, providerDefinitionId);
            var schema = config.GetSchema();
            var container = new Container(id, StreamMode);
            var tasks = new List<Task> { archiveTable(container.PrimaryTable, "Data") };
            foreach (var table in container.Tables)
                if (await CheckTableExists(executionContext, providerDefinitionId, table.Value.Name.GetValue()))
                    tasks.Add(archiveTable(table.Value.Name, table.Key));

            await Task.WhenAll(tasks);

            async Task archiveTable(SanitizedSqlString name, string context)
            {
                var newName = new SanitizedSqlString($"{name}{DateTime.Now:yyyyMMddHHmmss}");
                var sql = BuildRenameContainerSql(schema, name, newName, out var param);
                _logger.LogDebug("Sql Server Connector - Archive Container[{Context}] - Generated query: {sql}",
                    context, sql);

                try
                {
                    await _client.ExecuteCommandAsync(config, sql, param);
                }
                catch (Exception e)
                {
                    var message = $"Could not Archive Container {name} for Connector {providerDefinitionId}";
                    _logger.LogError(e, message);
                    throw new CreateContainerException(message);
                }
            }
        }

        public override async Task RenameContainer(ExecutionContext executionContext, Guid providerDefinitionId,
            string id, string newName)
        {
            var config = await base.GetAuthenticationDetails(executionContext, providerDefinitionId);
            var schema = config.GetSchema();
            var currentContainer = new Container(id, StreamMode);
            var updatedContainer = new Container(newName, StreamMode);

            var tasks = new List<Task>
            {
                renameTable(currentContainer.PrimaryTable, updatedContainer.PrimaryTable, "Data")
            };
            foreach (var current in currentContainer.Tables)
                if (await CheckTableExists(executionContext, providerDefinitionId, current.Value.Name.GetValue()))
                    tasks.Add(renameTable(current.Value.Name, updatedContainer.Tables[current.Key].Name, current.Key));

            await Task.WhenAll(tasks);

            async Task renameTable(SanitizedSqlString currentName, SanitizedSqlString updatedName, string context)
            {
                var sql = BuildRenameContainerSql(schema, currentName, updatedName, out var param);

                _logger.LogDebug("Sql Server Connector - Rename Container[{Context}] - Generated query: {sql}", context,
                    sql);

                try
                {
                    await _client.ExecuteCommandAsync(config, sql, param);
                }
                catch (Exception e)
                {
                    var message = $"Could not Rename Container {currentName} for Connector {providerDefinitionId}";
                    _logger.LogError(e, message);
                    throw new CreateContainerException(message);
                }
            }
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

        public override async Task RemoveContainer(ExecutionContext executionContext, Guid providerDefinitionId,
            string id)
        {
            var config = await base.GetAuthenticationDetails(executionContext, providerDefinitionId);
            var schema = config.GetSchema();
            var container = new Container(id, StreamMode);
            var tasks = new List<Task> { removeTable(container.PrimaryTable, "Data") };
            foreach (var table in container.Tables)
                if (await CheckTableExists(executionContext, providerDefinitionId, table.Value.Name.GetValue()))
                    tasks.Add(removeTable(table.Value.Name, table.Key));

            await Task.WhenAll(tasks);

            async Task removeTable(SanitizedSqlString name, string context)
            {
                var sql = BuildRemoveContainerSql(schema, name);

                _logger.LogDebug("Sql Server Connector - Remove Container[{Context}] - Generated query: {sql}", context,
                    sql);

                try
                {
                    await _client.ExecuteCommandAsync(config, sql);
                }
                catch (Exception e)
                {
                    var message = $"Could not Remove Container {name} for Connector {providerDefinitionId}";
                    _logger.LogError(e, message);
                    throw new CreateContainerException(message);
                }
            }
        }

        public override async Task<IEnumerable<IConnectorContainer>> GetContainers(ExecutionContext executionContext,
            Guid providerDefinitionId)
        {
            try
            {
                var config = await base.GetAuthenticationDetails(executionContext, providerDefinitionId);
                var tables = await _client.GetTables(config.Authentication);

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

        public override async Task<IEnumerable<IConnectionDataType>> GetDataTypes(ExecutionContext executionContext,
            Guid providerDefinitionId, string containerId)
        {
            try
            {
                var config = await base.GetAuthenticationDetails(executionContext, providerDefinitionId);
                var tables = await _client.GetTableColumns(config.Authentication, containerId);

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
            var tableName = new SanitizedSqlString(containerName);
            try
            {
                if (_bulkSupported)
                {
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
                        () => GetAuthenticationDetails(executionContext, providerDefinitionId),
                        _logger);
                }
                else
                {
                    var config = await GetAuthenticationDetails(executionContext, providerDefinitionId);
                    var schema = config.GetSchema();
                    var container = new Container(tableName, StreamMode);
                    var deleteFeature = _features.GetFeature<IBuildDeleteDataFeature>();
                    var commands = deleteFeature
                        .BuildDeleteDataSql(executionContext, providerDefinitionId, schema, container.PrimaryTable,
                            originEntityCode, codes, entityId, _logger);

                    // We need to origin entity code for entities that have been deleted
                    // see if we need to delete linked tables
                    // do look up of OriginEntityCode from current table data
                    var lookupOriginCodes = new List<string>();
                    await using (var connection = await _client.GetConnection(config.Authentication))
                    {
                        var cmd = connection.CreateCommand();
                        cmd.CommandText =
                            $"Select distinct OriginEntityCode from [{container.PrimaryTable}] where [Id] = @Id;";
                        cmd.Parameters.Add(new SqlParameter("Id", entityId));

                        var resp = await cmd.ExecuteReaderAsync();
                        if (resp.HasRows)
                            while (await resp.ReadAsync())
                                lookupOriginCodes.Add(resp.GetString(0));
                        resp.Close();
                    }

                    foreach (var table in container.Tables)
                        if (await CheckTableExists(executionContext, providerDefinitionId, table.Value.Name.GetValue()))
                            foreach (var entry in lookupOriginCodes)
                                commands = commands.Concat(deleteFeature
                                    .BuildDeleteDataSql(executionContext, providerDefinitionId, schema, table.Value.Name, entry,
                                        null, null, _logger));

                    foreach (var command in commands)
                        await _client.ExecuteCommandAsync(config, command.Text, command.Parameters);
                }
            }
            catch (Exception e)
            {
                var message =
                    $"Could not delete data from Container '{tableName}' for Connector {providerDefinitionId}";
                _logger.LogError(e, message);
                throw new StoreDataException(message);
            }
        }


        public string BuildEdgeStoreDataSql(SanitizedSqlString schema, SanitizedSqlString tableName, string originEntityCode, string correlationId,
            IEnumerable<string> edges, out List<SqlParameter> param)
        {
            var originParam = new SqlParameter { ParameterName = "@OriginEntityCode", Value = originEntityCode };
            var correlationParam = new SqlParameter { ParameterName = "@CorrelationId", Value = correlationId };
            param = new List<SqlParameter> { originParam, correlationParam };

            var builder = new StringBuilder();

            if (StreamMode == StreamMode.Sync && _syncEdgesTable)
                builder.AppendLine(
                    $"DELETE FROM [{schema}].[{tableName}] where [OriginEntityCode] = {originParam.ParameterName}");

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
                    ? $"INSERT INTO [{schema}].[{tableName}] ([OriginEntityCode],[CorrelationId],[Code]) values"
                    : $"INSERT INTO [{schema}].[{tableName}] ([OriginEntityCode],[Code]) values");

            builder.AppendJoin(", ", edgeValues);

            return builder.ToString();
        }

        private string BuildRenameContainerSql(SanitizedSqlString schema, SanitizedSqlString currentName, SanitizedSqlString newName, out List<SqlParameter> param)
        {
            param = new List<SqlParameter>
            {
                new SqlParameter("@currentName", SqlDbType.NVarChar) {Value = $"{schema}.{currentName}"},
                new SqlParameter("@newName", SqlDbType.NVarChar) {Value = newName.GetValue()}
            };

            return $"IF EXISTS(SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{currentName}') EXEC sp_rename @currentName, @newName";
        }

        private string BuildRemoveContainerSql(SanitizedSqlString schema, SanitizedSqlString tableName)
        {
            return $"DROP TABLE [{schema}].[{tableName}] IF EXISTS";
        }

        protected string BuildEmptyContainerSql(SanitizedSqlString schema, SanitizedSqlString tableName)
        {
            return $"TRUNCATE TABLE [{schema}].[{tableName}]";
        }
    }
}
