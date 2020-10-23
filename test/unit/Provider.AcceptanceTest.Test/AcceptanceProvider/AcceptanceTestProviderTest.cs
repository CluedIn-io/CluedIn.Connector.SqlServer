using Castle.Windsor;
using CluedIn.Core;
using CluedIn.Core.Providers;
using Moq;

namespace Provider.Acceptance.Test.AcceptanceProvider
{
  public abstract class AcceptanceProviderTest
  {
    protected readonly ProviderBase Sut;

    protected Mock<IWindsorContainer> Container;

    protected AcceptanceProviderTest()
    {
      Container = new Mock<IWindsorContainer>();
      var applicationContext = new ApplicationContext(Container.Object);
      Sut = new CluedIn.Provider.Acceptance.AcceptanceProvider(applicationContext);
    }
  }
}
