using System.Reflection;
using Castle.MicroKernel.Registration;
using Castle.MicroKernel.SubSystems.Configuration;
using Castle.Windsor;
using CluedIn.Connector.SqlServer.Connector;
using CluedIn.Connector.SqlServer.Features;
using CluedIn.Core.Providers;

namespace CluedIn.Connector.SqlServer
{
    public class InstallComponents : IWindsorInstaller
    {
        public void Install(IWindsorContainer container, IConfigurationStore store)
        {
            var asm = Assembly.GetExecutingAssembly();
            container.Register(Types.FromAssembly(asm).BasedOn<IProvider>().WithServiceFromInterface().If(t => !t.IsAbstract).LifestyleSingleton());
            container.Register(Types.FromAssembly(asm).BasedOn<IEntityActionBuilder>().WithServiceFromInterface().If(t => !t.IsAbstract).LifestyleSingleton());
            container.Register(Types.FromAssembly(asm).BasedOn<IFeatureStore>().WithServiceFromInterface().If(t => !t.IsAbstract).LifestyleSingleton());
            container.Register(Component.For<ISqlClient>().ImplementedBy<BulkSqlClient>().OnlyNewServices());
        }
    }
}
