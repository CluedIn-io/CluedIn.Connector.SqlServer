using Castle.MicroKernel.Registration;
using Castle.Windsor;
using CluedIn.Core;
using CluedIn.Core.Providers;
using ComponentHost;
using Connector.Common;

namespace CluedIn.Connector.SqlServer
{
    [Component(nameof(SqlServerConnectorComponent), "Providers", ComponentType.Service, ServerComponents.ProviderWebApi,
        Components.Server, Components.DataStores, Isolation = ComponentIsolation.NotIsolated)]
    public sealed class SqlServerConnectorComponent : ComponentBase<InstallComponents>
    {
        private static bool init = false;
        public SqlServerConnectorComponent(ComponentInfo componentInfo) : base(componentInfo)
        {
        }

        public override void Start()
        {
            lock (this)
            {
                if (init == false)
                {
                    System.Diagnostics.Debugger.Launch();
                    System.Diagnostics.Debugger.Break();
                    Container.Install(new InstallComponents());

                    var asm = System.Reflection.Assembly.GetExecutingAssembly();
                    Container.Register(Types.FromAssembly(asm).BasedOn<IProvider>().WithServiceFromInterface().If(t => !t.IsAbstract).LifestyleSingleton());
                    //Container.Register(Types.FromAssembly(asm).BasedOn<IEntityActionBuilder>().WithServiceFromInterface().If(t => !t.IsAbstract).LifestyleSingleton());
                    // Container.Register(Types.FromAssembly(asm).BasedOn<IEntityActionBuilder>().WithServiceFromInterface().If(t => !t.IsAbstract).LifestyleSingleton());
                    init = true;
                }
            }
        }
    }
    public sealed class SqlServerConnectorComponent2
    {
        private static bool init = false;
        public SqlServerConnectorComponent2()
        {
        }

        public void Start(IWindsorContainer container)
        {
            lock (this)
            {
                if (init == false)
                {
                    System.Diagnostics.Debugger.Launch();
                    System.Diagnostics.Debugger.Break();
                    container.Install(new InstallComponents());

                    var asm = System.Reflection.Assembly.GetExecutingAssembly();
                    container.Register(Types.FromAssembly(asm).BasedOn<IProvider>().WithServiceFromInterface().If(t => !t.IsAbstract).LifestyleSingleton());
                }
            }
        }
    }
}
