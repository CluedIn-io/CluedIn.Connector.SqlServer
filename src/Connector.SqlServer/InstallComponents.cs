using Castle.MicroKernel.Registration;
using Castle.MicroKernel.SubSystems.Configuration;
using Castle.Windsor;
using CluedIn.Connector.Common.Features;
using CluedIn.Connector.SqlServer.Connector;
using System.Reflection;

namespace CluedIn.Connector.SqlServer
{
    public class InstallComponents : IWindsorInstaller
    {
        public void Install(IWindsorContainer container, IConfigurationStore store)
        {
            var asm = Assembly.GetExecutingAssembly();
            container.Register(Types.FromAssembly(asm).BasedOn<IFeatureStore>().WithServiceFromInterface()
                .If(t => !t.IsAbstract).LifestyleSingleton());
            container.Register(Component.For<ISqlClient>().ImplementedBy<BulkSqlClient>().OnlyNewServices());
        }
    }
}
