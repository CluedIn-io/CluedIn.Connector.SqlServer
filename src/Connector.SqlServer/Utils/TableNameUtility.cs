using CluedIn.Core.Connectors;
using CluedIn.Core.Streams.Models;
using System;

namespace CluedIn.Connector.SqlServer.Utils
{
    internal static class TableNameUtility
    {
        public static SqlTableName GetMainTableName(IReadOnlyStreamModel model, SqlName schema) =>
            GetMainTableName(model.ContainerName, schema);

        public static SqlTableName GetMainTableName(IReadOnlyCreateContainerModelV2 model, SqlName schema) =>
            GetMainTableName(model.Name, schema);

        [System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)]
        public static SqlTableName GetMainTableName(string name, SqlName schema)
        {
            return SqlName.FromUnsafeMainTable(name).ToTableName(schema);
        }

        public static SqlTableName GetCodeTableName(IReadOnlyStreamModel model, SqlName schema)
        {
            var mainTableName = GetMainTableName(model, schema);
            return SqlName.FromUnsafe($"{mainTableName.LocalName.Value}Codes").ToTableName(schema);
        }

        public static SqlTableName GetCodeTableName(IReadOnlyCreateContainerModelV2 model, SqlName schema)
        {
            var mainTableName = GetMainTableName(model, schema);
            return SqlName.FromUnsafe($"{mainTableName.LocalName.Value}Codes").ToTableName(schema);
        }

        [System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)]
        public static SqlTableName GetCodeTableName(SqlTableName mainTableName, SqlName schema)
        {
            return SqlName.FromUnsafe($"{mainTableName.LocalName.Value}Codes").ToTableName(schema);
        }

        public static SqlTableName GetEdgesTableName(IReadOnlyStreamModel model, EdgeDirection edgeDirection, SqlName schema)
        {
            var mainTableName = GetMainTableName(model, schema);
            return GetEdgesTableName(mainTableName, edgeDirection, schema);
        }

        public static SqlTableName GetEdgesTableName(IReadOnlyCreateContainerModelV2 model, EdgeDirection edgeDirection, SqlName schema)
        {
            var mainTableName = GetMainTableName(model, schema);
            return GetEdgesTableName(mainTableName, edgeDirection, schema);
        }

        [System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)]
        public static SqlTableName GetEdgesTableName(SqlTableName mainTableName, EdgeDirection edgeDirection, SqlName schema)
        {
            return edgeDirection switch
            {
                EdgeDirection.Outgoing => SqlName.FromUnsafe($"{mainTableName.LocalName.Value}OutgoingEdges").ToTableName(schema),
                EdgeDirection.Incoming => SqlName.FromUnsafe($"{mainTableName.LocalName.Value}IncomingEdges").ToTableName(schema),
                _ => throw new ArgumentOutOfRangeException(nameof(edgeDirection), edgeDirection, null)
            };
        }

        public static SqlTableName GetEdgePropertiesTableName(IReadOnlyStreamModel model, EdgeDirection edgeDirection, SqlName schema)
        {
            var mainTableName = GetMainTableName(model, schema);
            return GetEdgePropertiesTableName(mainTableName, edgeDirection, schema);
        }

        public static SqlTableName GetEdgePropertiesTableName(IReadOnlyCreateContainerModelV2 model, EdgeDirection edgeDirection, SqlName schema)
        {
            var mainTableName = GetMainTableName(model, schema);
            return GetEdgePropertiesTableName(mainTableName, edgeDirection, schema);
        }

        [System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)]
        public static SqlTableName GetEdgePropertiesTableName(SqlTableName mainTableName, EdgeDirection edgeDirection, SqlName schema)
        {
            return edgeDirection switch
            {
                EdgeDirection.Outgoing => SqlName.FromUnsafe($"{mainTableName.LocalName.Value}OutgoingEdgeProperties").ToTableName(schema),
                EdgeDirection.Incoming => SqlName.FromUnsafe($"{mainTableName.LocalName.Value}IncomingEdgeProperties").ToTableName(schema),
                _ => throw new ArgumentOutOfRangeException(nameof(edgeDirection), edgeDirection, null)
            };
        }
    }
}
