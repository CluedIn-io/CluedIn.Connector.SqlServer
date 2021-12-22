using CluedIn.Core;
using CluedIn.Core.Server;
using ComponentHost;
using CluedIn.Connector.Common;
using Microsoft.Extensions.Logging;
using System.Reflection;

namespace CluedIn.Connector.SqlServer
{
    [Component(nameof(SqlServerConnectorComponent), "Providers", ComponentType.Service, ServerComponents.ProviderWebApi,
        Components.Server, Components.DataStores, Isolation = ComponentIsolation.NotIsolated)]
    public sealed class SqlServerConnectorComponent : ServiceApplicationComponent<IServer>
    {
        public SqlServerConnectorComponent(ComponentInfo componentInfo) : base(componentInfo)
        {
        }

        /// <summary>Starts this instance.</summary>
        public override void Start()
        {
            CommonStaticServiceHolder.InstallBaseComponents<InstallComponents>(Container,
                Assembly.GetExecutingAssembly());
            Log.LogInformation("SQL Client Registered");
            State = ServiceState.Started;
        }

        /// <summary>Stops this instance.</summary>
        public override void Stop()
        {
            if (State == ServiceState.Stopped)
                return;

            State = ServiceState.Stopped;
        }
    }
}
