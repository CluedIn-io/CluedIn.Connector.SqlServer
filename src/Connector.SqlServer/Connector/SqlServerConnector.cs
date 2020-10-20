﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using System.Threading.Tasks;
using CluedIn.Core;
using CluedIn.Core.Connectors;
using CluedIn.Core.Data.Vocabularies;
using CluedIn.Core.DataStore;
using Dapper;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.SqlServer.Connector
{
    public class SqlServerConnector : ConnectorBase
    {
        private ILogger<SqlServerConnector> _logger;

        public SqlServerConnector(IConfigurationRepository repo, ILogger<SqlServerConnector> logger) : base(repo)
        {
            ProviderId = SqlServerConstants.ProviderId;
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        public override async Task CreateContainer(ExecutionContext executionContext, Guid providerDefinitionId, CreateContainerModel model)
        {
            try
            {
                var config = await base.GetAuthenticationDetails(executionContext, providerDefinitionId);
                var connection = await GetConnection(config);

                var databaseName = (string)config.Authentication[SqlServerConstants.KeyName.DatabaseName];

                var builder = new StringBuilder();
                builder.AppendLine($"USE quotename({Sanitize(databaseName)})");
                builder.AppendLine("GO");
                builder.AppendLine("");
                builder.AppendLine("SET ANSI_NULLS ON");
                builder.AppendLine("GO");
                builder.AppendLine("");
                builder.AppendLine("SET QUOTED_IDENTIFIER ON");
                builder.AppendLine("GO");
                builder.AppendLine("");
                builder.AppendLine($"CREATE TABLE quotename({Sanitize(model.Name)})(");
                builder.AppendLine("");

                var index = 0;
                var count = model.DataTypes.Count;
                foreach (var type in model.DataTypes)
                {
                    builder.AppendLine($"quotename({Sanitize(type.Name)}][{GetDbType(type.Type)}) NULL{(index < count - 1 ? "," : "")}");

                    index++;
                }
                builder.AppendLine(") ON[PRIMARY]");
                builder.AppendLine("GO");

                var cmd = connection.CreateCommand();
#pragma warning disable CA2100 // Review SQL queries for security vulnerabilities
                cmd.CommandText = builder.ToString();
#pragma warning restore CA2100 // Review SQL queries for security vulnerabilities
                await cmd.ExecuteNonQueryAsync();
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Error Creating Container");
                throw;
            }
        }

        public override async Task EmptyContainer(ExecutionContext executionContext, Guid providerDefinitionId, string id)
        {
            try
            {
                var config = await base.GetAuthenticationDetails(executionContext, providerDefinitionId);
                var connection = await GetConnection(config);

                var databaseName = (string)config.Authentication[SqlServerConstants.KeyName.DatabaseName];

                var builder = new StringBuilder();
                builder.AppendLine($"USE quotename({Sanitize(databaseName)})");
                builder.AppendLine("GO");
                builder.AppendLine($"TRUNCATE quotename({Sanitize(id)})");
                builder.AppendLine("GO");

                var cmd = connection.CreateCommand();
#pragma warning disable CA2100 // Review SQL queries for security vulnerabilities
                cmd.CommandText = builder.ToString();
#pragma warning restore CA2100 // Review SQL queries for security vulnerabilities
                await cmd.ExecuteNonQueryAsync();
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Error Emptying Container");
                throw;
            }
        }

        private string Sanitize(string str)
        {
            return str;
        }

        public override async Task<IEnumerable<IConnectorContainer>> GetContainers(ExecutionContext executionContext, Guid providerDefinitionId)
        {
            try
            {
                var config = await base.GetAuthenticationDetails(executionContext, providerDefinitionId);
                var connection = await GetConnection(config);

                var tables = connection.GetSchema("Tables");

                var result = new List<SqlServerConnectorContainer>();
                foreach (DataRow row in tables.Rows)
                {
                    var tableName = row["TABLE_NAME"] as string;
                    result.Add(new SqlServerConnectorContainer { Id = tableName, Name = tableName });
                }

                return result;
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Error Getting Container");
                throw;
            }
        }

        public override async Task<IEnumerable<IConnectionDataType>> GetDataTypes(ExecutionContext executionContext, Guid providerDefinitionId, string containerId)
        {
            try
            {
                var config = await base.GetAuthenticationDetails(executionContext, providerDefinitionId);
                var connection = await GetConnection(config);

                var restrictions = new string[4];
                restrictions[2] = containerId;
                var tables = connection.GetSchema("Columns", restrictions);

                var result = new List<SqlServerConnectorDataType>();
                foreach (DataRow row in tables.Rows)
                {
                    var name = row["COLUMN_NAME"] as string;
                    var rawType = row["DATA_TYPE"] as string;
                    var type = GetVocabType(rawType);
                    result.Add(new SqlServerConnectorDataType { Name = name, RawDataType = rawType, Type = type });
                }

                return result;
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Error getting Data Types");
                throw e;
            }
        }

        private VocabularyKeyDataType GetVocabType(string rawType)
        {
            return rawType.ToLower() switch
            {
                "bigint" => VocabularyKeyDataType.Integer,
                "int" => VocabularyKeyDataType.Integer,
                "smallint" => VocabularyKeyDataType.Integer,
                "tinyint" => VocabularyKeyDataType.Integer,
                "bit" => VocabularyKeyDataType.Boolean,
                "decimal" => VocabularyKeyDataType.Number,
                "numeric" => VocabularyKeyDataType.Number,
                "float" => VocabularyKeyDataType.Number,
                "real" => VocabularyKeyDataType.Number,
                "money" => VocabularyKeyDataType.Money,
                "smallmoney" => VocabularyKeyDataType.Money,
                "datetime" => VocabularyKeyDataType.DateTime,
                "smalldatetime" => VocabularyKeyDataType.DateTime,
                "date" => VocabularyKeyDataType.DateTime,
                "datetimeoffset" => VocabularyKeyDataType.DateTime,
                "datetime2" => VocabularyKeyDataType.DateTime,
                "time" => VocabularyKeyDataType.Time,
                "char" => VocabularyKeyDataType.Text,
                "varchar" => VocabularyKeyDataType.Text,
                "text" => VocabularyKeyDataType.Text,
                "nchar" => VocabularyKeyDataType.Text,
                "nvarchar" => VocabularyKeyDataType.Text,
                "ntext" => VocabularyKeyDataType.Text,
                "binary" => VocabularyKeyDataType.Text,
                "varbinary" => VocabularyKeyDataType.Text,
                "image" => VocabularyKeyDataType.Text,
                "timestamp" => VocabularyKeyDataType.Text,
                "uniqueidentifier" => VocabularyKeyDataType.Guid,
                "XML" => VocabularyKeyDataType.Xml,
                "geometry" => VocabularyKeyDataType.Text,
                "geography" => VocabularyKeyDataType.GeographyLocation,
                _ => VocabularyKeyDataType.Text
            };
        }


        private string GetDbType(VocabularyKeyDataType type)
        {
            return type switch
            {
                VocabularyKeyDataType.Integer => "bigint",
                VocabularyKeyDataType.Number => "decimal(18,4)",
                VocabularyKeyDataType.Money => "money",
                VocabularyKeyDataType.DateTime => "datetime2",
                VocabularyKeyDataType.Time => "time",
                VocabularyKeyDataType.Xml => "XML",
                VocabularyKeyDataType.Guid => "binary",
                VocabularyKeyDataType.GeographyLocation => "geography",
                _ => "nvarchar(max)"
            };
        }

        private async Task<SqlConnection> GetConnection(IDictionary<string, object> config)
        {
            var cnxString = new SqlConnectionStringBuilder
            {
                Authentication = SqlAuthenticationMethod.SqlPassword,
                Password = (string)config[SqlServerConstants.KeyName.Password],
                UserID = (string)config[SqlServerConstants.KeyName.Username],
                DataSource = (string)config[SqlServerConstants.KeyName.Host],
                InitialCatalog = (string)config[SqlServerConstants.KeyName.DatabaseName],
            };

            var result = new SqlConnection(cnxString.ToString());

            await result.OpenAsync();

            return result;
        }

        private async Task<SqlConnection> GetConnection(IConnectorConnection config)
        {
            return await GetConnection(config.Authentication);
        }

        public override async Task<bool> VerifyConnection(ExecutionContext executionContext, Guid providerDefinitionId)
        {
            var config = await base.GetAuthenticationDetails(executionContext, providerDefinitionId);
            return await VerifyConnection(executionContext, config.Authentication);
        }

        public override async Task<bool> VerifyConnection(ExecutionContext executionContext, IDictionary<string, object> config)
        {
            var connection = await GetConnection(config);

            return connection.State == ConnectionState.Open;
        }
    }
}
