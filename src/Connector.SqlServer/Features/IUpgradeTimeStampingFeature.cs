using CluedIn.Connector.SqlServer.Connector;
using CluedIn.Core.Connectors;
using CluedIn.Core.Streams.Models;
using Microsoft.Data.SqlClient;
using System.Threading.Tasks;

namespace CluedIn.Connector.SqlServer.Features
{
    public interface IUpgradeTimeStampingFeature
    {
        Task VerifyTimeStampColumnExist(ISqlClient client, IConnectorConnection config, SqlTransaction transaction, StreamModel stream);
    }
}
