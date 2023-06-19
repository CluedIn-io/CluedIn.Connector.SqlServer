using CluedIn.Connector.SqlServer.Connector;
using CluedIn.Connector.SqlServer.Utils.TableDefinitions;
using CluedIn.Core.Streams.Models;
using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Data;

namespace CluedIn.Connector.SqlServer.Utils
{
    internal static class CreateStoreCommandUtility
    {
        public static SqlServerConnectorCommand BuildStoreMainTableCommand(IReadOnlyStreamModel streamModel, SqlConnectorEntityData connectorEntityData, DateTimeOffset timeStamp, SqlName schema)
        {
            return MainTableDefinition.CreateUpsertCommand(streamModel, connectorEntityData, timeStamp, schema);
        }

        public static SqlServerConnectorCommand BuildStoreCodesInsertCommand(IReadOnlyStreamModel streamModel, SqlConnectorEntityData connectorEntityData, SqlName schema)
        {
            return CodeTableDefinition.CreateUpsertCommand(streamModel, connectorEntityData, schema);
        }

        public static SqlServerConnectorCommand BuildStoreEdgesCommand(IReadOnlyStreamModel streamModel, SqlConnectorEntityData connectorEntityData, EdgeDirection edgeDirection, SqlName schema)
        {
            return EdgeTableDefinition.CreateUpsertCommand(streamModel, edgeDirection, connectorEntityData, schema);
        }

        public static SqlServerConnectorCommand BuildStoreEdgePropertiesCommands(IReadOnlyStreamModel streamModel, SqlConnectorEntityData connectorEntityData, EdgeDirection edgeDirection, SqlName schema)
        {
            return EdgePropertiesTableDefinition.CreateUpsertCommands(streamModel, edgeDirection, connectorEntityData, schema);
        }

        public static SqlServerConnectorCommand BuildDeleteEdgePropertiesForEntity(IReadOnlyStreamModel streamModel, SqlConnectorEntityData connectorEntityData, EdgeDirection edgeDirection, SqlName schema)
        {
            var edgeTableName = TableNameUtility.GetEdgesTableName(streamModel, edgeDirection, schema);
            var edgePropertiesTableName = TableNameUtility.GetEdgePropertiesTableName(streamModel, edgeDirection, schema);

            var commandText = $"""
                DELETE edgeProperties
                FROM (SELECT [Id] as [Id] FROM {edgeTableName.FullyQualifiedName} WHERE [EntityId] = @EntityId) edges
                INNER JOIN {edgePropertiesTableName.FullyQualifiedName} edgeProperties
                ON edges.[Id] = edgeProperties.[EdgeId]
                """;

            var entityIdParameter = new SqlParameter("@EntityId", SqlDbType.UniqueIdentifier) { Value = connectorEntityData.EntityId };

            return new SqlServerConnectorCommand() { Text = commandText, Parameters = new[] { entityIdParameter } };
        }

        public static SqlServerConnectorCommand BuildDeleteEdgesForEntity(IReadOnlyStreamModel streamModel, SqlConnectorEntityData connectorEntityData, EdgeDirection edgeDirection, SqlName schema)
        {
            var edgeTableName = TableNameUtility.GetEdgesTableName(streamModel, edgeDirection, schema);

            var commandText = $"DELETE FROM {edgeTableName} WHERE [EntityId] = @EntityId";

            var entityIdParameter = new SqlParameter("@EntityId", SqlDbType.UniqueIdentifier) { Value = connectorEntityData.EntityId };

            return new SqlServerConnectorCommand() { Text = commandText, Parameters = new[] { entityIdParameter } };
        }

        public static SqlServerConnectorCommand BuildDeleteCodesForEntity(IReadOnlyStreamModel streamModel, SqlConnectorEntityData connectorEntityData, SqlName schema)
        {
            var codeTableName = TableNameUtility.GetCodeTableName(streamModel, schema);

            var commandText = $"DELETE FROM {codeTableName} WHERE [EntityId] = @EntityId";

            var entityIdParameter = new SqlParameter("@EntityId", SqlDbType.UniqueIdentifier) { Value = connectorEntityData.EntityId };

            return new SqlServerConnectorCommand() { Text = commandText, Parameters = new[] { entityIdParameter } };
        }

        public static SqlServerConnectorCommand BuildDeleteEntity(IReadOnlyStreamModel streamModel, SqlConnectorEntityData connectorEntityData, SqlName schema)
        {
            var mainTableName = TableNameUtility.GetMainTableName(streamModel, schema);

            var commandText = $"DELETE FROM {mainTableName} WHERE [Id] = @EntityId";

            var entityIdParameter = new SqlParameter("@EntityId", SqlDbType.UniqueIdentifier) { Value = connectorEntityData.EntityId };

            return new SqlServerConnectorCommand() { Text = commandText, Parameters = new[] { entityIdParameter } };
        }
    }
}
