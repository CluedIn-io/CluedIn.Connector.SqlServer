using CluedIn.Connector.SqlServer.Connector;
using CluedIn.Core.Streams.Models;
using System;
using System.Collections.Generic;
using System.Text;

namespace CluedIn.Connector.SqlServer.Utils
{
    internal class RenameTablesUtility
    {
        public static IEnumerable<SqlServerConnectorCommand> GetRenameTablesCommands(IReadOnlyStreamModel streamModel, SqlTableName oldMainTableName, SqlTableName newMainTableName, DateTimeOffset suffixDate, SqlName schema)
        {
            var builder = new StringBuilder();

            if (streamModel.ExportOutgoingEdges)
            {
                var outgoingEdgesTableOldName = TableNameUtility.GetEdgesTableName(oldMainTableName, EdgeDirection.Outgoing, schema);
                var outgoingEdgesTableNewName = TableNameUtility.GetEdgesTableName(newMainTableName, EdgeDirection.Outgoing, schema);
                var renameOutgoingEdgesTableCommand = RenameTableCommandUtility.BuildTableRenameCommand(outgoingEdgesTableOldName, outgoingEdgesTableNewName, schema, suffixDate);
                yield return renameOutgoingEdgesTableCommand;

                var renameCustomOutgoingEdgesTableTypeCommand = CreateCustomTypeCommandUtility.BuildRenameEdgeTableCustomTypeCommand(oldMainTableName, newMainTableName, EdgeDirection.Outgoing, schema);
                yield return renameCustomOutgoingEdgesTableTypeCommand;

                if (streamModel.ExportOutgoingEdgeProperties)
                {
                    var outgoingEdgesPropertiesTableOldName = TableNameUtility.GetEdgePropertiesTableName(oldMainTableName, EdgeDirection.Outgoing, schema);
                    var outgoingEdgesPropertiesTableNewName = TableNameUtility.GetEdgePropertiesTableName(newMainTableName, EdgeDirection.Outgoing, schema);
                    var renameOutgoingEdgesPropertiesTableCommand = RenameTableCommandUtility.BuildTableRenameCommand(outgoingEdgesPropertiesTableOldName, outgoingEdgesPropertiesTableNewName, schema, suffixDate);
                    yield return renameOutgoingEdgesPropertiesTableCommand;

                    var renameCustomOutgoingEdgePropertiesTableTypeCommand = CreateCustomTypeCommandUtility.BuildRenameEdgePropertiesTableCustomTypeCommand(oldMainTableName, newMainTableName, EdgeDirection.Outgoing, schema);
                    yield return renameCustomOutgoingEdgePropertiesTableTypeCommand;
                }
            }

            if (streamModel.ExportIncomingEdges)
            {
                var incomingEdgesTableOldName = TableNameUtility.GetEdgesTableName(oldMainTableName, EdgeDirection.Incoming, schema);
                var incomingEdgesTableNewName = TableNameUtility.GetEdgesTableName(newMainTableName, EdgeDirection.Incoming, schema);
                var renameIncomingEdgesTableCommand = RenameTableCommandUtility.BuildTableRenameCommand(incomingEdgesTableOldName, incomingEdgesTableNewName, schema, suffixDate);
                yield return renameIncomingEdgesTableCommand;

                var renameCustomIncomingEdgesTableTypeCommand = CreateCustomTypeCommandUtility.BuildRenameEdgeTableCustomTypeCommand(oldMainTableName, newMainTableName, EdgeDirection.Incoming, schema);
                yield return renameCustomIncomingEdgesTableTypeCommand;

                if (streamModel.ExportIncomingEdgeProperties)
                {
                    var incomingEdgesPropertiesTableOldName = TableNameUtility.GetEdgePropertiesTableName(oldMainTableName, EdgeDirection.Incoming, schema);
                    var incomingEdgesPropertiesTableNewName = TableNameUtility.GetEdgePropertiesTableName(newMainTableName, EdgeDirection.Incoming, schema);
                    var renameIncomingEdgesPropertiesTableCommand = RenameTableCommandUtility.BuildTableRenameCommand(incomingEdgesPropertiesTableOldName, incomingEdgesPropertiesTableNewName, schema, suffixDate);
                    yield return renameIncomingEdgesPropertiesTableCommand;

                    var renameCustomIncomingEdgePropertiesTableTypeCommand = CreateCustomTypeCommandUtility.BuildRenameEdgePropertiesTableCustomTypeCommand(oldMainTableName, newMainTableName, EdgeDirection.Incoming, schema);
                    yield return renameCustomIncomingEdgePropertiesTableTypeCommand;
                }
            }

            var oldCodeTableName = TableNameUtility.GetCodeTableName(oldMainTableName, schema);
            var newCodeTableName = TableNameUtility.GetCodeTableName(newMainTableName, schema);
            var renameCodeTableCommand = RenameTableCommandUtility.BuildTableRenameCommand(oldCodeTableName, newCodeTableName, schema, suffixDate);
            yield return renameCodeTableCommand;

            var renameCustomCodeTableTypeCommand = CreateCustomTypeCommandUtility.BuildRenameCodeTableCustomTypeCommand(oldMainTableName, newMainTableName, schema);
            yield return renameCustomCodeTableTypeCommand;

            var renameMainTableCommand = RenameTableCommandUtility.BuildTableRenameCommand(oldMainTableName, newMainTableName, schema, suffixDate);
            yield return renameMainTableCommand;
        }
    }
}
