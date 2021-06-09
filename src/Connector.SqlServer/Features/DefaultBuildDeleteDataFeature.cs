using System;
using System.Collections.Generic;
using CluedIn.Connector.SqlServer.Connector;
using CluedIn.Core;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.SqlServer.Features
{
    public class DefaultBuildDeleteDataFeature : IBuildDeleteDataFeature
    {
        public const string DefaultKeyField = "OriginEntityCode";

        public IEnumerable<SqlServerConnectorCommand> BuildDeleteDataSql(ExecutionContext executionContext, Guid providerDefinitionId, string containerName, string keyValue, ILogger logger)
        {
            if (executionContext == null)
                throw new ArgumentNullException(nameof(executionContext));

            if (logger == null)
                throw new ArgumentNullException(nameof(logger));

            if (string.IsNullOrWhiteSpace(containerName))
                throw new InvalidOperationException("The containerName must be provided.");

            if (string.IsNullOrWhiteSpace(keyValue))
                throw new InvalidOperationException("The keyValue must be provided.");


            var sql = $"DELETE FROM {containerName.SqlSanitize()} WHERE {DefaultKeyField} = @KeyValue";

            var parameters = new List<SqlParameter>
            {
                new SqlParameter("KeyValue", keyValue)
            };


            return new[]
            {
                new SqlServerConnectorCommand
                {
                    Text = sql,
                    Parameters = parameters
                }
            };
        }
    }
}
