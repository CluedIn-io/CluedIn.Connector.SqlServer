using System;

namespace CluedIn.Connector.SqlServer.Exceptions
{
    internal class IncompatibleTableException : Exception
    {
        public IncompatibleTableException(string message)
            : base($"Table is incompatible: {message}")
        {

        }

        public static IncompatibleTableException OldTableVersionExists(Guid streamId, Guid connectorProviderDefinitionId)
        {
            var message = $"""

                StreamId: {streamId}
                ConnectorProviderDefinitionId: {connectorProviderDefinitionId}

                It looks like the tables in the database is not compatible with this version of the connector.
                This is most likely because the tables were created in an older version of the connector.
                To remedy:
                    - Disable all streams using this connector
                    - Reprocess the streams
                    - Reenable the streams
                """;

            return new IncompatibleTableException(message);
        }
    }
}
