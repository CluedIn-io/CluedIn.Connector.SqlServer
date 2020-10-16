using CluedIn.Core.Connectors;

namespace CluedIn.Connector.SqlServer.Connector
{
    public class SqlServerConnectorContainer : IConnectorContainer
    {
        public string Name { get; set; }
        public string Id { get; set; }
        public string FullyQualifiedName { get; set; }
    }
}
