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
            
        }
    }
}
