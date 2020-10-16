using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using CluedIn.Core;
using CluedIn.Core.Connectors;
using CluedIn.Core.Data.Vocabularies;
using CluedIn.Core.DataStore;
using Microsoft.Data.SqlClient;

namespace CluedIn.Connector.SqlServer.Connector
{
    public class SqlServerConnector : ConnectorBase
    {
        public SqlServerConnector(IConfigurationRepository repo) : base(repo)
        {
            ProviderId = SqlServerConstants.ProviderId;
        }

        public override async Task CreateContainer(ExecutionContext executionContext, Guid providerDefinitionId, CreateContainerModel model)
        {
            var config = await base.GetAuthenticationDetails(executionContext, providerDefinitionId);
            var connection = await GetConnection(config);

            var databaseName = (string)config.Authentication[SqlServerConstants.KeyName.DatabaseName];

            var builder = new StringBuilder();
            builder.Append($"USE [{Sanitize(databaseName)}]");
            builder.Append("GO");
            builder.Append("");
            builder.Append("SET ANSI_NULLS ON");
            builder.Append("GO");
            builder.Append("");
            builder.Append("SET QUOTED_IDENTIFIER ON");
            builder.Append("GO");
            builder.Append("");
            builder.Append($"CREATE TABLE [{Sanitize(model.Name)}](");
            builder.Append("");

            var index = 0;
            var count = model.DataTypes.Count;
            foreach (var type in model.DataTypes)
            {
                builder.Append($"[{Sanitize(type.Name)}][{GetDbType(type.Type)}] NULL{(index < count -1 ? "," : "")}");

                index++;
            }
            builder.Append(") ON[PRIMARY]");
            builder.Append("GO");

            var cmd = connection.CreateCommand();
#pragma warning disable CA2100 // Review SQL queries for security vulnerabilities
            cmd.CommandText = builder.ToString();
#pragma warning restore CA2100 // Review SQL queries for security vulnerabilities
            await cmd.ExecuteNonQueryAsync();

        }

        private string Sanitize(string str)
        {
            // TODO Sanitize to prevent Sql Injection
            return str;
        }

        public override async Task EmptyContainer(ExecutionContext executionContext, Guid providerDefinitionId, string id)
        {
            var config = await base.GetAuthenticationDetails(executionContext, providerDefinitionId);
            var connection = await GetConnection(config);

            var databaseName = (string)config.Authentication[SqlServerConstants.KeyName.DatabaseName];

            var builder = new StringBuilder();
            builder.Append($"USE [{Sanitize(databaseName)}]");
            builder.Append("GO");
            builder.Append($"TRUNCATE [{Sanitize(id)}]");
            builder.Append("GO");

            var cmd = connection.CreateCommand();
#pragma warning disable CA2100 // Review SQL queries for security vulnerabilities
            cmd.CommandText = builder.ToString();
#pragma warning restore CA2100 // Review SQL queries for security vulnerabilities
            await cmd.ExecuteNonQueryAsync();
        }

        private async Task<SqlConnection> GetConnection(IConnectorConnection config)
        {
            var cnxString = new SqlConnectionStringBuilder
            {
                Authentication = SqlAuthenticationMethod.SqlPassword,
                Password = (string)config.Authentication[SqlServerConstants.KeyName.Password],
                UserID = (string)config.Authentication[SqlServerConstants.KeyName.Username],
                DataSource = (string)config.Authentication[SqlServerConstants.KeyName.Host],
                InitialCatalog = (string)config.Authentication[SqlServerConstants.KeyName.DatabaseName],
            };
            var result = new SqlConnection(cnxString.ToString());

            await result.OpenAsync();

            return result;
        }

        public override async Task<IEnumerable<IConnectorContainer>> GetContainers(ExecutionContext executionContext, Guid providerDefinitionId)
        {
            var config = await base.GetAuthenticationDetails(executionContext, providerDefinitionId);
            var connection = await GetConnection(config);

            var tables = connection.GetSchema("Tables");

            var result = new List<SqlServerConnectorContainer>();
            foreach (System.Data.DataRow row in tables.Rows)
            {
                var tableName = row["TABLE_NAME"] as string;
                result.Add(new SqlServerConnectorContainer { Id = tableName, Name = tableName });
            }

            return result;
        }

        public override async Task<IEnumerable<IConnectionDataType>> GetDataTypes(ExecutionContext executionContext, Guid providerDefinitionId, string containerId)
        {
            var config = await base.GetAuthenticationDetails(executionContext, providerDefinitionId);
            var connection = await GetConnection(config);

            var restrictions = new string[4];
            restrictions[2] = containerId;
            var tables = connection.GetSchema("Columns", restrictions);

            var result = new List<SqlServerConnectorDataType>();
            foreach (System.Data.DataRow row in tables.Rows)
            {
                var name = row["COLUMN_NAME"] as string;
                var rawType = row["DATA_TYPE"] as string;
                var type = GetVocabType(rawType);
                result.Add(new SqlServerConnectorDataType { Name = name, RawDataType = rawType, Type = type });
            }

            return result;
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

        public override Task<bool> VerifyConnection(ExecutionContext executionContext, Guid providerDefinitionId)
        {
            throw new NotImplementedException();
        }

        public override Task<bool> VerifyConnection(ExecutionContext executionContext, IDictionary<string, object> authenticationData)
        {
            throw new NotImplementedException();
        }
    }
}
