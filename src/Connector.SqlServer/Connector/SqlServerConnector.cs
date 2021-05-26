using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using CluedIn.Connector.SqlServer.Features;
using CluedIn.Core;
using CluedIn.Core.Connectors;
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
        }

        public override async Task CreateContainer(ExecutionContext executionContext, Guid providerDefinitionId, CreateContainerModel model)
        {
            var config = await base.GetAuthenticationDetails(executionContext, providerDefinitionId);

            async Task CreateTable(string name, IEnumerable<ConnectionDataType> columns, IList<string> keys, string context)
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

            var tasks = new List<Task> { CreateTable(model.Name, model.DataTypes, _defaultKeyFields, "Data") };
            if (model.CreateEdgeTable)
            {
                var originEntityCodeColumn = "OriginEntityCode".SqlSanitize();
                var codeColumn = "Code".SqlSanitize();
                tasks.Add(CreateTable(EdgeContainerHelper.GetName(model.Name), new List<ConnectionDataType>
                {
                    new ConnectionDataType { Name = originEntityCodeColumn, Type = VocabularyKeyDataType.Text },
                    new ConnectionDataType { Name = codeColumn, Type = VocabularyKeyDataType.Text },
                }, new List<string> { originEntityCodeColumn, codeColumn }, "Edges"));


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

            var tasks = new List<Task> { EmptyTable(id, "Data") };
            var edgeTableName = EdgeContainerHelper.GetName(id);
            if (await CheckTableExists(executionContext, providerDefinitionId, edgeTableName))
                tasks.Add(EmptyTable(edgeTableName, "Edges"));

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

            var tasks = new List<Task> { ArchiveTable(id, "Data") };
            var edgeTableName = EdgeContainerHelper.GetName(id);
            if (await CheckTableExists(executionContext, providerDefinitionId, edgeTableName))
                tasks.Add(ArchiveTable(edgeTableName, "Edges"));

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

            var tasks = new List<Task> { RenameTable(id, newName, "Data") };
            var edgeTableName = EdgeContainerHelper.GetName(id);
            if (await CheckTableExists(executionContext, providerDefinitionId, edgeTableName))
                tasks.Add(RenameTable(edgeTableName, EdgeContainerHelper.GetName(newName), "Edges"));

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

            var tasks = new List<Task> { RemoveTable(id, "Data") };
            var edgeTableName = EdgeContainerHelper.GetName(id);
            if (await CheckTableExists(executionContext, providerDefinitionId, edgeTableName))
                tasks.Add(RemoveTable(edgeTableName, "Edges"));

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
                var config = await base.GetAuthenticationDetails(executionContext, providerDefinitionId);
                var commands = new List<SqlServerConnectorCommand>();
                if (data.ContainsKey("Tags"))
                {
                    var codes = data["Tags"];
                    if (codes is IList<object> codeStrings)
                    {
                        var wantedCode = codeStrings; //.Where(d => d.Contains("CustId"));
                        foreach (var code in wantedCode)
                        {
                            var hack = data["Tags"] = code;


                            commands.AddRange(_features.GetFeature<IBuildStoreDataFeature>()
                            .BuildStoreDataSql(executionContext, providerDefinitionId, containerName, data, _defaultKeyFields, _logger));

                            //var sql = BuildStoreDataSql(containerName, data, out var param);

                            //_logger.LogDebug($"Sql Server Connector - Store Data - Generated query: {sql}");

                            //await _client.ExecuteCommandAsync(config, sql, param);
                        }

                        foreach (var command in commands)
                        {
                            _logger.LogDebug("Sql Server Connector - Store Data - Generated query: {command}", command.Text);

                            await _client.ExecuteCommandAsync(config, command.Text, command.Parameters);
                        }
                    }
                }
                else
                {
                    commands = _features.GetFeature<IBuildStoreDataFeature>()
                        .BuildStoreDataSql(executionContext, providerDefinitionId, containerName, data, _defaultKeyFields, _logger).ToList();

                    foreach (var command in commands)
                    {
                        _logger.LogDebug("Sql Server Connector - Store Data - Generated query: {command}", command.Text);

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
