﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using CluedIn.Connector.SqlServer.Features;
using CluedIn.Core;
using CluedIn.Core.Configuration;
using CluedIn.Core.Connectors;
using CluedIn.Core.Data;
using CluedIn.Core.Data.Vocabularies;
using CluedIn.Core.DataStore;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.SqlServer.Connector
{
    public class SqlServerConnector : ConnectorBase
    {
        private readonly ILogger<SqlServerConnector> _logger;
        private readonly ISqlClient _client;
        private readonly IFeatureStore _features;
        private readonly int _bulkInsertThreshold;
        private readonly int _bulkDeleteThreshold;
        private readonly IBulkSqlClient _bulkClient;
        private readonly bool _bulkSupported;
        private readonly IList<string> _defaultKeyFields = new List<string> { "OriginEntityCode" };

        public SqlServerConnector(
                IConfigurationRepository repo,
                ILogger<SqlServerConnector> logger,
                ISqlClient client,
                IFeatureStore features) : base(repo)
        {
            ProviderId = SqlServerConstants.ProviderId;

            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _client = client ?? throw new ArgumentNullException(nameof(client));
            _features = features ?? throw new ArgumentNullException(nameof(logger));

            _bulkInsertThreshold = ConfigurationManagerEx.AppSettings.GetValue("Streams.SqlConnector.BulkInsertRecordCount", 0);
            _bulkDeleteThreshold = ConfigurationManagerEx.AppSettings.GetValue("Streams.SqlConnector.BulkDeleteRecordCount", 0);
            _bulkClient = _client as IBulkSqlClient;
            _bulkSupported = _bulkInsertThreshold > 0 && _bulkClient != null;
            _logger.LogInformation($"{nameof(SqlServerConnector)} - bulk insert support enabled {{enabled}}", _bulkSupported);
        }

        public override async Task CreateContainer(ExecutionContext executionContext, Guid providerDefinitionId, CreateContainerModel model)
        {
            var config = await base.GetAuthenticationDetails(executionContext, providerDefinitionId);

            async Task CreateTable(string name, IEnumerable<ConnectionDataType> columns, IEnumerable<string> keys, string context)
            {
                try
                {
                    IEnumerable<SqlServerConnectorCommand> commands = _features.GetFeature<IBuildCreateContainerFeature>()
                        .BuildCreateContainerSql(executionContext, providerDefinitionId, name, columns, keys, _logger).ToList();

                    var indexCommands = _features.GetFeature<IBuildCreateIndexFeature>().BuildCreateIndexSql(executionContext, providerDefinitionId, name, keys, _logger);
                    commands = commands.Union(indexCommands);

                    foreach (var command in commands)
                    {
                        _logger.LogDebug("Sql Server Connector - Create Container[{Context}] - Generated query: {sql}", context, command.Text);

                        await _client.ExecuteCommandAsync(config, command.Text, command.Parameters);
                    }
                }
                catch (Exception e)
                {
                    var message = $"Could not create Container {name} for Connector {providerDefinitionId}";
                    _logger.LogError(e, message);
                    throw new CreateContainerException(message);
                }
            }

            var container = new Container(model.Name);
            var codesTable = container.Tables["Codes"];

            var tasks = new List<Task> {
                // Primary table
                CreateTable(container.PrimaryTable, model.DataTypes, _defaultKeyFields, "Data"),
                // Codes table                
                CreateTable(codesTable.Name, codesTable.Columns, codesTable.Keys, "Codes")
            };

            // We optionally build an edges table
            if (model.CreateEdgeTable)
            {
                var edgesTable = container.Tables["Codes"];
                tasks.Add(CreateTable(edgesTable.Name, edgesTable.Columns, edgesTable.Keys, "Edges"));
            }

            await Task.WhenAll(tasks);
        }

        public override async Task EmptyContainer(ExecutionContext executionContext, Guid providerDefinitionId, string id)
        {
            var config = await base.GetAuthenticationDetails(executionContext, providerDefinitionId);

            async Task EmptyTable(string name, string context)
            {
                var sql = BuildEmptyContainerSql(name);
                _logger.LogDebug("Sql Server Connector - Empty Container[{Context}] - Generated query: {sql}", context, sql);

                try
                {
                    await _client.ExecuteCommandAsync(config, sql);
                }
                catch (Exception e)
                {
                    var message = $"Could not empty Container {name}";
                    _logger.LogError(e, message);
                    throw new CreateContainerException(message);
                }
            }

            var container = new Container(id);
            var tasks = new List<Task> { EmptyTable(container.PrimaryTable, "Data") };
            foreach(var table in container.Tables)
            {
                if (await CheckTableExists(executionContext, providerDefinitionId, table.Value.Name))
                        tasks.Add(EmptyTable(table.Value.Name, table.Key));
            }

            await Task.WhenAll(tasks);

        }

        public override async Task ArchiveContainer(ExecutionContext executionContext, Guid providerDefinitionId, string id)
        {
            var config = await base.GetAuthenticationDetails(executionContext, providerDefinitionId);

            async Task ArchiveTable(string name, string context)
            {
                var newName = await GetValidContainerName(executionContext, providerDefinitionId, $"{name}{DateTime.Now:yyyyMMddHHmmss}");
                var sql = BuildRenameContainerSql(name, newName, out var param);
                _logger.LogDebug("Sql Server Connector - Archive Container[{Context}] - Generated query: {sql}", context, sql);

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

            var container = new Container(id);
            var tasks = new List<Task> { ArchiveTable(container.PrimaryTable, "Data") };
            foreach (var table in container.Tables)
            {
                if (await CheckTableExists(executionContext, providerDefinitionId, table.Value.Name))
                    tasks.Add(ArchiveTable(table.Value.Name, table.Key));
            }

            await Task.WhenAll(tasks);
        }

        public override async Task RenameContainer(ExecutionContext executionContext, Guid providerDefinitionId, string id, string newName)
        {
            var config = await base.GetAuthenticationDetails(executionContext, providerDefinitionId);

            async Task RenameTable(string currentName, string updatedName, string context)
            {
                var tempName = updatedName.SqlSanitize();

                var sql = BuildRenameContainerSql(currentName, tempName, out var param);

                _logger.LogDebug("Sql Server Connector - Rename Container[{Context}] - Generated query: {sql}", context, sql);

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

            var currentContainer = new Container(id);
            var updatedContainer = new Container(newName);

            var tasks = new List<Task> { RenameTable(currentContainer.PrimaryTable, updatedContainer.PrimaryTable, "Data") };
            foreach (var current in currentContainer.Tables)
            {
                if (await CheckTableExists(executionContext, providerDefinitionId, current.Value.Name))
                    tasks.Add(RenameTable(current.Value.Name, updatedContainer.Tables[current.Key].Name, current.Key));
            }

            await Task.WhenAll(tasks);

        }

        public override async Task RemoveContainer(ExecutionContext executionContext, Guid providerDefinitionId, string id)
        {
            var config = await base.GetAuthenticationDetails(executionContext, providerDefinitionId);

            async Task RemoveTable(string name, string context)
            {
                var sql = BuildRemoveContainerSql(name);

                _logger.LogDebug("Sql Server Connector - Remove Container[{Context}] - Generated query: {sql}", context, sql);

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

            var container = new Container(id);
            var tasks = new List<Task> { RemoveTable(container.PrimaryTable, "Data") };
            foreach (var table in container.Tables)
            {
                if (await CheckTableExists(executionContext, providerDefinitionId, table.Value.Name))
                    tasks.Add(RemoveTable(table.Value.Name, table.Key));
            }

            await Task.WhenAll(tasks);
        }

        public string BuildEmptyContainerSql(string id) => $"TRUNCATE TABLE [{id.SqlSanitize()}]";

        public override Task<string> GetValidDataTypeName(ExecutionContext executionContext, Guid providerDefinitionId, string name)
        {
            // Strip non-alpha numeric characters
            var result = Regex.Replace(name, @"[^A-Za-z0-9]+", "");

            return Task.FromResult(result);
        }

        public override async Task<string> GetValidContainerName(ExecutionContext executionContext, Guid providerDefinitionId, string name)
        {
            // Strip non-alpha numeric characters
            var result = Regex.Replace(name, @"[^A-Za-z0-9]+", "");

            // Check if exists
            if (await CheckTableExists(executionContext, providerDefinitionId, result))
            {
                // If exists, append count like in windows explorer
                var count = 0;
                string newName;
                do
                {
                    count++;
                    newName = $"{result}{count}";
                } while (await CheckTableExists(executionContext, providerDefinitionId, newName));

                result = newName;
            }

            // return new name
            return result;
        }

        public override async Task<IEnumerable<IConnectorContainer>> GetContainers(ExecutionContext executionContext, Guid providerDefinitionId)
        {
            try
            {
                var config = await base.GetAuthenticationDetails(executionContext, providerDefinitionId);
                var tables = await _client.GetTables(config.Authentication);

                var result = from DataRow row in tables.Rows
                             select row["TABLE_NAME"] as string into tableName
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
                var tables = await _client.GetTableColumns(config.Authentication, containerId);

                var result = from DataRow row in tables.Rows
                             let name = row["COLUMN_NAME"] as string
                             let rawType = row["DATA_TYPE"] as string
                             let type = GetVocabType(rawType)
                             select new SqlServerConnectorDataType
                             {
                                 Name = name,
                                 RawDataType = rawType,
                                 Type = type
                             };

                return result.ToList();
            }
            catch (Exception e)
            {
                var message = $"Could not get Data types for Container '{containerId}' for Connector {providerDefinitionId}";
                _logger.LogError(e, message);
                throw new GetDataTypesException(message);
            }
        }

        public override async Task<bool> VerifyConnection(ExecutionContext executionContext, Guid providerDefinitionId)
        {
            var config = await base.GetAuthenticationDetails(executionContext, providerDefinitionId);
            return await VerifyConnection(executionContext, config.Authentication);
        }

        public override async Task<bool> VerifyConnection(ExecutionContext executionContext, IDictionary<string, object> config)
        {
            try
            {
                var connection = await _client.GetConnection(config);

                return connection.State == ConnectionState.Open;
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Error verifying connection");
                throw new ConnectionException();
            }
        }

        public override async Task StoreData(ExecutionContext executionContext, Guid providerDefinitionId, string containerName, IDictionary<string, object> data)
        {
            try
            {
                if (_bulkSupported)
                {
                    var bulkFeature = _features.GetFeature<IBulkStoreDataFeature>();
                    await bulkFeature.BulkTableUpdate(
                        executionContext,
                        providerDefinitionId,
                        containerName,
                        data,
                        _bulkInsertThreshold,
                        _bulkClient,
                        () => base.GetAuthenticationDetails(executionContext, providerDefinitionId),
                        _logger);
                }
                else
                {
                    var config = await base.GetAuthenticationDetails(executionContext, providerDefinitionId);

                    var commands = _features.GetFeature<IBuildStoreDataFeature>()
                        .BuildStoreDataSql(executionContext, providerDefinitionId, containerName, data, _defaultKeyFields, _logger);

                    foreach (var command in commands)
                    {
                        await _client.ExecuteCommandAsync(config, command.Text, command.Parameters);
                    }
                }
            }
            catch (Exception e)
            {
                var message = $"Could not store data into Container '{containerName}' for Connector {providerDefinitionId}";
                _logger.LogError(e, message);
                throw new StoreDataException(message);
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
                var deleteByColumns = new Dictionary<string, object> { ["Id"] = entityId };

                if (_bulkSupported)
                {
                    var bulkFeature = _features.GetFeature<IBulkDeleteDataFeature>();
                    await bulkFeature.BulkTableDelete(
                        executionContext,
                        providerDefinitionId,
                        containerName,
                        originEntityCode,
                        codes,
                        entityId,
                        _bulkInsertThreshold,
                        _bulkClient,
                        () => base.GetAuthenticationDetails(executionContext, providerDefinitionId),
                        _logger);
                }
                else
                {
                    var config = await base.GetAuthenticationDetails(executionContext, providerDefinitionId);

                    var deleteFeature = _features.GetFeature<IBuildDeleteDataFeature>();
                    var commands = deleteFeature
                        .BuildDeleteDataSql(executionContext, providerDefinitionId, containerName, originEntityCode, codes, entityId, _logger);

                    // see if we need to delete linked tables
                    var edgeTable = EdgeContainerHelper.GetName(containerName);
                    if(await CheckTableExists(executionContext, providerDefinitionId, edgeTable))
                    {
                        commands = commands.Concat(deleteFeature
                                        .BuildDeleteDataSql(executionContext, providerDefinitionId, edgeTable, originEntityCode, null, null, _logger));

                        commands = commands.Concat(deleteFeature
                            .BuildDeleteDataSql(executionContext, providerDefinitionId, edgeTable, null, codes, null, _logger));
                    }

                    var codeTable = CodeContainerHelper.GetName(containerName);
                    if (await CheckTableExists(executionContext, providerDefinitionId, codeTable))
                    {
                        commands = commands.Concat(deleteFeature
                                        .BuildDeleteDataSql(executionContext, providerDefinitionId, codeTable, originEntityCode, null, null, _logger));

                        commands = commands.Concat(deleteFeature
                            .BuildDeleteDataSql(executionContext, providerDefinitionId, codeTable, null, codes, null, _logger));
                    }

                    foreach (var command in commands)
                    {
                        await _client.ExecuteCommandAsync(config, command.Text, command.Parameters);
                    }
                }
            }
            catch (Exception e)
            {
                var message = $"Could not delete data from Container '{containerName}' for Connector {providerDefinitionId}";
                _logger.LogError(e, message);
                throw new StoreDataException(message);
            }
        }

        public override async Task StoreEdgeData(ExecutionContext executionContext, Guid providerDefinitionId, string containerName, string originEntityCode, IEnumerable<string> edges)
        {
            try
            {
                var edgeTableName = EdgeContainerHelper.GetName(containerName);
                if (await CheckTableExists(executionContext, providerDefinitionId, edgeTableName))
                {
                    var sql = BuildEdgeStoreDataSql(edgeTableName, originEntityCode, edges, out var param);

                    _logger.LogDebug($"Sql Server Connector - Store Edge Data - Generated query: {sql}");

                    var config = await base.GetAuthenticationDetails(executionContext, providerDefinitionId);
                    await _client.ExecuteCommandAsync(config, sql, param);
                }
            }
            catch (Exception e)
            {
                var message = $"Could not store edge data into Container '{containerName}' for Connector {providerDefinitionId}";
                _logger.LogError(e, message);
                throw new StoreDataException(message);
            }
        }

        public string BuildEdgeStoreDataSql(string containerName, string originEntityCode, IEnumerable<string> edges, out List<SqlParameter> param)
        {
            var originParam = new SqlParameter { ParameterName = "@OriginEntityCode", Value = originEntityCode };
            param = new List<SqlParameter> { originParam };

            var builder = new StringBuilder();
            builder.AppendLine($"DELETE FROM [{containerName.SqlSanitize()}] where [OriginEntityCode] = {originParam.ParameterName}");
            var edgeValues = new List<string>();
            foreach (var edge in edges)
            {
                var edgeParam = new SqlParameter
                {
                    ParameterName = $"@{edgeValues.Count}",
                    Value = edge
                };
                param.Add(edgeParam);
                edgeValues.Add($"(@OriginEntityCode, {edgeParam.ParameterName})");
            }

            if (edgeValues.Count > 0)
            {
                builder.AppendLine($"INSERT INTO [{containerName.SqlSanitize()}] ([OriginEntityCode],[Code]) values");
                builder.AppendJoin(", ", edgeValues);
            }

            return builder.ToString();
        }

        private string BuildRenameContainerSql(string id, string newName, out List<SqlParameter> param)
        {
            var result = $"IF EXISTS(SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{id.SqlSanitize()}') EXEC sp_rename @tableName, @newName";

            param = new List<SqlParameter>
            {
                new SqlParameter("@tableName", SqlDbType.NVarChar)
                {
                    Value = id.SqlSanitize()
                },
                new SqlParameter("@newName", SqlDbType.NVarChar)
                {
                    Value = newName.SqlSanitize()
                }
            };

            return result;
        }

        private string BuildRemoveContainerSql(string id)
        {
            var result = $"DROP TABLE [{id.SqlSanitize()}] IF EXISTS";

            return result;
        }

        private VocabularyKeyDataType GetVocabType(string rawType)
        {
            // return rawType.ToLower() switch //TODO: @LJU: Disabled as it needs reviewing; Breaks streams;
            // {
            //     "bigint" => VocabularyKeyDataType.Integer,
            //     "int" => VocabularyKeyDataType.Integer,
            //     "smallint" => VocabularyKeyDataType.Integer,
            //     "tinyint" => VocabularyKeyDataType.Integer,
            //     "bit" => VocabularyKeyDataType.Boolean,
            //     "decimal" => VocabularyKeyDataType.Number,
            //     "numeric" => VocabularyKeyDataType.Number,
            //     "float" => VocabularyKeyDataType.Number,
            //     "real" => VocabularyKeyDataType.Number,
            //     "money" => VocabularyKeyDataType.Money,
            //     "smallmoney" => VocabularyKeyDataType.Money,
            //     "datetime" => VocabularyKeyDataType.DateTime,
            //     "smalldatetime" => VocabularyKeyDataType.DateTime,
            //     "date" => VocabularyKeyDataType.DateTime,
            //     "datetimeoffset" => VocabularyKeyDataType.DateTime,
            //     "datetime2" => VocabularyKeyDataType.DateTime,
            //     "time" => VocabularyKeyDataType.Time,
            //     "char" => VocabularyKeyDataType.Text,
            //     "varchar" => VocabularyKeyDataType.Text,
            //     "text" => VocabularyKeyDataType.Text,
            //     "nchar" => VocabularyKeyDataType.Text,
            //     "nvarchar" => VocabularyKeyDataType.Text,
            //     "ntext" => VocabularyKeyDataType.Text,
            //     "binary" => VocabularyKeyDataType.Text,
            //     "varbinary" => VocabularyKeyDataType.Text,
            //     "image" => VocabularyKeyDataType.Text,
            //     "timestamp" => VocabularyKeyDataType.Text,
            //     "uniqueidentifier" => VocabularyKeyDataType.Guid,
            //     "XML" => VocabularyKeyDataType.Xml,
            //     "geometry" => VocabularyKeyDataType.Text,
            //     "geography" => VocabularyKeyDataType.GeographyLocation,
            //     _ => VocabularyKeyDataType.Text
            // };

            return VocabularyKeyDataType.Text;
        }

        private async Task<bool> CheckTableExists(ExecutionContext executionContext, Guid providerDefinitionId, string name)
        {
            try
            {
                var config = await base.GetAuthenticationDetails(executionContext, providerDefinitionId);
                var tables = await _client.GetTables(config.Authentication, name);

                return tables.Rows.Count > 0;
            }
            catch (Exception e)
            {
                var message = $"Error checking Container '{name}' exists for Connector {providerDefinitionId}";
                _logger.LogError(e, message);
                throw new ConnectionException(message);
            }
        }

    }
}
