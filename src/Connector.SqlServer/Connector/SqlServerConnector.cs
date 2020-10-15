using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using CluedIn.Core;
using CluedIn.Core.Connectors;
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
                result.Add(new SqlServerConnectorContainer {Id = tableName, Name = tableName});
            }

            return result;
        }

        private async Task<SqlConnection> GetConnection(IConnectorConnection config)
        {
            var cnxString = new SqlConnectionStringBuilder
            {
                Authentication = SqlAuthenticationMethod.SqlPassword,
                Password = (string) config.Authentication[SqlServerConstants.KeyName.Password],
                UserID = (string)config.Authentication[SqlServerConstants.KeyName.Username],
                DataSource = (string)config.Authentication[SqlServerConstants.KeyName.Host],
                InitialCatalog = (string)config.Authentication[SqlServerConstants.KeyName.DatabaseName],
            };
            var result = new SqlConnection(cnxString.ToString());

            await result.OpenAsync();

            return result;
        }

        public override Task<IEnumerable<IConnectionDataType>> GetDataTypes(ExecutionContext executionContext, Guid providerDefinitionId, string containerId)
        {
            throw new NotImplementedException();
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
