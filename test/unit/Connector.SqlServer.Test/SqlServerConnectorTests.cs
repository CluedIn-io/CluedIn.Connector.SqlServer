using CluedIn.Connector.SqlServer.Connector;
using CluedIn.Core.DataStore;
using Microsoft.Extensions.Logging;
using Moq;

namespace CluedIn.Connector.SqlServer.Unit.Tests
{
    public class SqlServerConnectorTests
    {
        private readonly SqlServerConnector _sut;
        private readonly Mock<IConfigurationRepository> _repo= new Mock<IConfigurationRepository>();
        private readonly Mock<Logger<SqlServerConnector>> _logger= new Mock<Logger<SqlServerConnector>>();
        private readonly Mock<ISqlClient> _client = new Mock<ISqlClient>();
        private readonly TestContext _context = new TestContext();

        public SqlServerConnectorTests()
        {
            _sut = new SqlServerConnector(_repo.Object, _logger.Object, _client.Object);
        }



    }
}
