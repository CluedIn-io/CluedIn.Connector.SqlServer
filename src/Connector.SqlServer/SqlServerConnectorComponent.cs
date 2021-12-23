using CluedIn.Core;
using ComponentHost;
using Connector.Common;

namespace CluedIn.Connector.SqlServer
{
    [Component(nameof(SqlServerConnectorComponent), "Providers", ComponentType.Service, ServerComponents.ProviderWebApi,
        Components.Server, Components.DataStores, Isolation = ComponentIsolation.NotIsolated)]
    public sealed class SqlServerConnectorComponent : ComponentBase<InstallComponents>
    {
        public SqlServerConnectorComponent(ComponentInfo componentInfo) : base(componentInfo)
        {
        }
    }
}
