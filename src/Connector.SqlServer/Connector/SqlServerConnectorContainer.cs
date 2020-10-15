using CluedIn.Core.Connectors;

namespace CluedIn.Connector.SqlServer.Connector
{
    public class SqlServerConnectorContainer : IConnectorContainer
    {
        public void Dispose()
        {
            
        }

        public string Name { get; set; }
        public string Id { get; set; }
    }
}
