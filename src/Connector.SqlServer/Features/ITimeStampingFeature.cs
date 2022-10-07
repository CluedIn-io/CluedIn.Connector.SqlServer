using CluedIn.Connector.SqlServer.Connector;
using CluedIn.Core.Connectors;
using CluedIn.Core.Streams.Models;
using System.Threading.Tasks;

namespace CluedIn.Connector.SqlServer.Features
{
    public interface ITimeStampingFeature
    {
        Task VerifyTimeStampColumnExist(ISqlClient client, IConnectorConnection config, StreamModel stream);
    }
}
