using Castle.MicroKernel.Registration;
using Castle.MicroKernel.SubSystems.Configuration;
using Castle.Windsor;
using CluedIn.Connector.SqlServer.Connector;
using System.Reflection;

namespace CluedIn.Connector.SqlServer
{
    public class InstallComponents : IWindsorInstaller
    {
        public void Install(IWindsorContainer container, IConfigurationStore store)
        {
            var asm = Assembly.GetExecutingAssembly();
            container.Register(Component.For<ISqlClient>().ImplementedBy<SqlClient>().OnlyNewServices());
            container.Register(Component.For<ISqlServerConstants>().ImplementedBy<SqlServerConstants>().LifestyleSingleton());
        }
    }
}
