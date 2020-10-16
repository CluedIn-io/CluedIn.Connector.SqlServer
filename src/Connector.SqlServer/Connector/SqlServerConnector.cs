using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using CluedIn.Core;
using CluedIn.Core.Connectors;
using CluedIn.Core.Data.Vocabularies;
using CluedIn.Core.DataStore;
using Microsoft.Data.SqlClient;
using RabbitMQ.Client.Apigen.Attributes;

namespace CluedIn.Connector.SqlServer.Connector
{
    public class SqlServerConnector : ConnectorBase
    {
        public SqlServerConnector(IConfigurationRepository repo) : base(repo)
        {
            ProviderId = SqlServerConstants.ProviderId;
        }

        public override Task CreateContainer(ExecutionContext executionContext, CreateContainerModel model)
        {
            throw new NotImplementedException();
        }

        public override Task EmptyContainer(ExecutionContext executionContext, string id)
        {
            throw new NotImplementedException();
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
            switch (rawType.ToLower())
            {
                case "bigint":
                case "int":
                case "smallint":
                case "tinyint":
                    return VocabularyKeyDataType.Integer;
                case "bit":
                    return VocabularyKeyDataType.Boolean;
                case "decimal":
                case "numeric":
                case "float":
                case "real":
                    return VocabularyKeyDataType.Number;
                case "money":
                case "smallmoney":
                    return VocabularyKeyDataType.Money;
                case "datetime":
                case "smalldatetime":
                case "date":
                case "datetimeoffset":
                case "datetime2":
                    return VocabularyKeyDataType.DateTime;
                case "time":
                    return VocabularyKeyDataType.Time;
                case "char":
                case "varchar":
                case "text":
                case "nchar":
                case "nvarchar":
                case "ntext":
                    return VocabularyKeyDataType.Text;
                case "XML":
                    return VocabularyKeyDataType.Xml;
                case "binary":
                case "varbinary":
                case "image":
                case "timestamp":
                case "uniqueidentifier":
                    return VocabularyKeyDataType.Guid;
                case "geometry":
                case "geography":
                    return VocabularyKeyDataType.GeographyLocation;
                default:
                    return VocabularyKeyDataType.Text;
            }
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
