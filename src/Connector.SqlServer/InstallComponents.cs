using System;
using Castle.MicroKernel.Registration;
using Castle.MicroKernel.SubSystems.Configuration;
using Castle.Windsor;
using CluedIn.Core;

namespace CluedIn.Connector.SqlServer
{
    public class InstallComponents : IWindsorInstaller
    {
        public void Install(IWindsorContainer container, IConfigurationStore store)
        {
            if (container == null) throw new ArgumentNullException(nameof(container));
            if (store == null) throw new ArgumentNullException(nameof(store));

            if (!container.Kernel.HasComponent(typeof(ISystemNotifications)) && !container.Kernel.HasComponent(typeof(SystemNotifications)))
            {
                container.Register(Component.For<ISystemNotifications, SystemNotifications>());
            }
        }
    }
}
