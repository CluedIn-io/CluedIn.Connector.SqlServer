using CluedIn.Core;
using CluedIn.Core.Server;
using ComponentHost;
using Microsoft.Extensions.Logging;

namespace CluedIn.Connector.SqlServer
{
    [Component(nameof(SqlServerConnectorComponent), "Providers", ComponentType.Service, ServerComponents.ProviderWebApi, Components.Server, Components.DataStores, Isolation = ComponentIsolation.NotIsolated)]
    public sealed class SqlServerConnectorComponent : ServiceApplicationComponent<IServer>
    {
        public SqlServerConnectorComponent(ComponentInfo componentInfo) : base(componentInfo)
        {
        }

        public override void Start()
        {
            Container.Install(new InstallComponents());
            Log.LogInformation($"{ComponentName} Registered");
            State = ServiceState.Started;
        }

        public override void Stop()
        {
            if (State == ServiceState.Stopped)
                return;

            State = ServiceState.Stopped;
        }

        private string ComponentName => GetType().Name;
    }
}
