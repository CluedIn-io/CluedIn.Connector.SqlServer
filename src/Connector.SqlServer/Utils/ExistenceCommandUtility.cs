using CluedIn.Connector.SqlServer.Connector;
using CluedIn.Core.Streams.Models;
using Microsoft.Data.SqlClient;
using System;
using System.Data;
using System.Threading.Tasks;

namespace CluedIn.Connector.SqlServer.Utils
{
    internal static class ExistenceCommandUtility
    {
        public enum ExistenceCheckResult
        {
            NoVersionExists,
            EarlierVersionExists,
            SameVersionExists,
            NewerVersionExists
        }

        public static SqlServerConnectorCommand BuildExistenceCommand(IReadOnlyStreamModel streamModel, SqlConnectorEntityData connectorEntityData, SqlName schema)
        {
            var mainTableName = TableNameUtility.GetMainTableName(streamModel, schema);
            var entityIdParam = new SqlParameter("@entityId", SqlDbType.UniqueIdentifier) { Value = connectorEntityData.EntityId };
            var persistVersionParam = new SqlParameter("@persistVersion", SqlDbType.Int) { Value = connectorEntityData.PersistInfo.PersistVersion };

            var commandText = $@"
-- 1: No version exists
-- 2: Earlier version exists
-- 2: Same version exists
-- 3: Newer version exists

IF NOT EXISTS (SELECT * FROM {mainTableName.FullyQualifiedName} WHERE [Id] = @entityId)
	SELECT 1
ELSE IF EXISTS (SELECT * FROM {mainTableName.FullyQualifiedName} WHERE [Id] = @entityId AND [PersistVersion] < @persistVersion)
	SELECT 2
ELSE IF EXISTS (SELECT * FROM {mainTableName.FullyQualifiedName} WHERE [Id] = @entityId AND [PersistVersion] = @persistVersion)
	SELECT 3
ELSE IF EXISTS (SELECT * FROM {mainTableName.FullyQualifiedName} WHERE [Id] = @entityId AND [PersistVersion] > @persistVersion)
	SELECT 4";

            return new SqlServerConnectorCommand { Text = commandText, Parameters = new[] { entityIdParam, persistVersionParam } };
        }

        public static async Task<ExistenceCheckResult> ExecuteExistenceCheckCommand(SqlServerConnectorCommand command, SqlTransaction transaction)
        {
            var sqlCommand = command.ToSqlCommand(transaction);
            var commandResult = await sqlCommand.ExecuteScalarAsync();

            return commandResult switch
            {
                1 => ExistenceCheckResult.NoVersionExists,
                2 => ExistenceCheckResult.EarlierVersionExists,
                3 => ExistenceCheckResult.SameVersionExists,
                4 => ExistenceCheckResult.NewerVersionExists,
                _ => throw new ArgumentOutOfRangeException(nameof(commandResult), "Existence check returned unexpected result")
            };
        }
    }
}
