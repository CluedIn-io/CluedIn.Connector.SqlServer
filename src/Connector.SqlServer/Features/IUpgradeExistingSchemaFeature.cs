using System.Threading.Tasks;
using CluedIn.Connector.SqlServer.Connector;
using CluedIn.Core.Connectors;
using CluedIn.Core.Streams.Models;

namespace CluedIn.Connector.SqlServer.Features
{
    public interface IUpgradeExistingSchemaFeature
    {
        Task VerifyExistingContainer(ISqlClient client, IConnectorConnection config, StreamModel stream);
    }
}
