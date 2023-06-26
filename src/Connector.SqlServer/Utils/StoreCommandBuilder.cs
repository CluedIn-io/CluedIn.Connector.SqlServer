using CluedIn.Connector.SqlServer.Connector;
using CluedIn.Connector.SqlServer.Utils.TableDefinitions;
using CluedIn.Core.Streams.Models;
using Microsoft.Data.SqlClient;
using System;
using System.Data;

namespace CluedIn.Connector.SqlServer.Utils
{
    internal static class StoreCommandBuilder
    {
        public static SqlServerConnectorCommand MainTableCommand(IReadOnlyStreamModel streamModel, SqlConnectorEntityData connectorEntityData, DateTimeOffset timeStamp, SqlName schema)
        {
            return MainTableDefinition.CreateUpsertCommand(streamModel, connectorEntityData, timeStamp, schema);
        }

        public static SqlServerConnectorCommand CodesInsertCommand(IReadOnlyStreamModel streamModel, SqlConnectorEntityData connectorEntityData, SqlName schema)
        {
            return CodeTableDefinition.CreateUpsertCommand(streamModel, connectorEntityData, schema);
        }

        public static SqlServerConnectorCommand EdgesCommand(IReadOnlyStreamModel streamModel, SqlConnectorEntityData connectorEntityData, EdgeDirection edgeDirection, SqlName schema)
        {
            return EdgeTableDefinition.CreateUpsertCommand(streamModel, edgeDirection, connectorEntityData, schema);
        }

        public static SqlServerConnectorCommand EdgePropertiesCommand(IReadOnlyStreamModel streamModel, SqlConnectorEntityData connectorEntityData, EdgeDirection edgeDirection, SqlName schema)
        {
            return EdgePropertiesTableDefinition.CreateUpsertCommands(streamModel, edgeDirection, connectorEntityData, schema);
        }

        public static SqlServerConnectorCommand DeleteEdgePropertiesForEntity(IReadOnlyStreamModel streamModel, SqlConnectorEntityData connectorEntityData, EdgeDirection edgeDirection, SqlName schema)
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

        public static SqlServerConnectorCommand DeleteEdgesForEntity(IReadOnlyStreamModel streamModel, SqlConnectorEntityData connectorEntityData, EdgeDirection edgeDirection, SqlName schema)
        {
            var edgeTableName = TableNameUtility.GetEdgesTableName(streamModel, edgeDirection, schema);

            var commandText = $"DELETE FROM {edgeTableName} WHERE [EntityId] = @EntityId";

            var entityIdParameter = new SqlParameter("@EntityId", SqlDbType.UniqueIdentifier) { Value = connectorEntityData.EntityId };

            return new SqlServerConnectorCommand() { Text = commandText, Parameters = new[] { entityIdParameter } };
        }

        public static SqlServerConnectorCommand DeleteCodesForEntity(IReadOnlyStreamModel streamModel, SqlConnectorEntityData connectorEntityData, SqlName schema)
        {
            var codeTableName = TableNameUtility.GetCodeTableName(streamModel, schema);

            var commandText = $"DELETE FROM {codeTableName} WHERE [EntityId] = @EntityId";

            var entityIdParameter = new SqlParameter("@EntityId", SqlDbType.UniqueIdentifier) { Value = connectorEntityData.EntityId };

            return new SqlServerConnectorCommand() { Text = commandText, Parameters = new[] { entityIdParameter } };
        }

        public static SqlServerConnectorCommand DeleteEntity(IReadOnlyStreamModel streamModel, SqlConnectorEntityData connectorEntityData, SqlName schema)
        {
            var mainTableName = TableNameUtility.GetMainTableName(streamModel, schema);

            var commandText = $"DELETE FROM {mainTableName} WHERE [Id] = @EntityId";

            var entityIdParameter = new SqlParameter("@EntityId", SqlDbType.UniqueIdentifier) { Value = connectorEntityData.EntityId };

            return new SqlServerConnectorCommand() { Text = commandText, Parameters = new[] { entityIdParameter } };
        }
    }
}
