using CluedIn.Connector.SqlServer.Connector;
using CluedIn.Connector.SqlServer.Utils;
using CluedIn.Core.Connectors;

namespace CluedIn.Connector.SqlServer.Features
{
    public interface IBuildCreateContainerFeature
    {
        SqlServerConnectorCommand BuildMainTableCommand(SqlTableName tableName, IReadOnlyCreateContainerModelV2 model, SqlName schema);
        SqlServerConnectorCommand BuildCodeTableCommand(SqlTableName tableName, IReadOnlyCreateContainerModelV2 model, SqlName schema);
        SqlServerConnectorCommand BuildEdgeTableCommand(SqlTableName tableName, IReadOnlyCreateContainerModelV2 model, SqlName schema);
        SqlServerConnectorCommand BuildEdgePropertiesTableCommand(SqlTableName tableName, IReadOnlyCreateContainerModelV2 model, SqlName schema);
    }
}
