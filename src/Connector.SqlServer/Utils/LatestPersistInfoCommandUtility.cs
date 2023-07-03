using CluedIn.Connector.SqlServer.Connector;
using CluedIn.Core.Connectors;
using CluedIn.Core.Streams.Models;
using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace CluedIn.Connector.SqlServer.Utils
{
    internal static class LatestPersistInfoCommandUtility
    {
        public static SqlServerConnectorCommand GetSinglePersistInfoCommand(IReadOnlyStreamModel streamModel, SqlName schema, Guid entityId)
        {
            var mainTableName = TableNameUtility.GetMainTableName(streamModel, schema);

            var commandText = @$"
SELECT [PersistVersion], [PersistHash], [Id]
FROM {mainTableName.FullyQualifiedName}
WHERE [Id] = @EntityId";
            var idParameter = new SqlParameter("@EntityId", entityId.ToString());

            return new SqlServerConnectorCommand { Text = commandText, Parameters = new[] { idParameter } };
        }

        public static async Task<ConnectorLatestEntityPersistInfo> ReadSinglePersistInfo(SqlDataReader reader)
        {
            await reader.ReadAsync();
            var persistVersion = (int)reader[0];
            var persistHash = (string)reader[1];
            var entityId = (Guid)reader[2];

            var persistInfo = new ConnectorEntityPersistInfo(persistHash, persistVersion);
            return new ConnectorLatestEntityPersistInfo(entityId, persistInfo);
        }

        public static SqlServerConnectorCommand GetAllPersistInfosCommand(IReadOnlyStreamModel streamModel, SqlName schema)
        {
            var mainTableName = TableNameUtility.GetMainTableName(streamModel, schema);

            var commandText = @$"
SELECT [PersistVersion], [PersistHash], [Id]
FROM {mainTableName.FullyQualifiedName}";

            return new SqlServerConnectorCommand { Text = commandText, Parameters = Array.Empty<SqlParameter>() };
        }

        public static async IAsyncEnumerable<ConnectorLatestEntityPersistInfo> ReadAllPersistInfos(SqlDataReader reader)
        {
            while (await reader.ReadAsync())
            {
                var persistVersion = (int)reader[0];
                var persistHash = (string)reader[1];
                var entityId = (Guid)reader[2];

                var persistInfo = new ConnectorEntityPersistInfo(persistHash, persistVersion);
                yield return new ConnectorLatestEntityPersistInfo(entityId, persistInfo);
            }
        }
    }
}
