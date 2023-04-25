using CluedIn.Connector.SqlServer.Connector;
using Microsoft.Extensions.Logging;
using Moq;

namespace CluedIn.Connector.SqlServer.Unit.Tests
{
    public class SqlServerConnectorTestsBase
    {
        protected readonly SqlServerConnector Sut;
        protected readonly Mock<ILogger<SqlServerConnector>> Logger = new Mock<ILogger<SqlServerConnector>>();
        protected readonly Mock<ISqlClient> Client = new Mock<ISqlClient>();
        protected readonly TestContext Context = new TestContext();
        protected readonly ISqlServerConstants Constants = new SqlServerConstants();

        public SqlServerConnectorTestsBase()
        {
            Sut = new SqlServerConnector(Logger.Object, Client.Object, Constants);
        }
    }
}
