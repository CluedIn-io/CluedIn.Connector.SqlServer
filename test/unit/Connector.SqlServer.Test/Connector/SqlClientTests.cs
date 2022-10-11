using System.Collections.Generic;
using CluedIn.Connector.SqlServer.Connector;
using Xunit;
using CluedIn.Connector.Common.Configurations;
using CluedIn.Connector.SqlServer.Utility;

namespace CluedIn.Connector.SqlServer.Unit.Tests.Connector
{
    public class SqlClientTests
    {
        private readonly SqlClient _sut;

        public SqlClientTests()
        {
            _sut = new SqlClient();
        }

        [Fact]
        public void BuildConnectionString_Sets_From_Dictionary()
        {
            var properties = new Dictionary<string, object>
            {
                [KeyName.Password] = "password",
                [KeyName.Username] = "user",
                [KeyName.Host] = "host",
                [KeyName.DatabaseName] = "database"
            };

            var result = _sut.BuildConnectionString(properties);

            Assert.Equal("Data Source=host,1433;Initial Catalog=database;User ID=user;Password=password;Pooling=True;Authentication=SqlPassword", result);
        }

        [Fact]
        public void BuildConnectionString_WithPort_Sets_From_Dictionary()
        {
            var properties = new Dictionary<string, object>
            {
                [KeyName.Password] = "password",
                [KeyName.Username] = "user",
                [KeyName.Host] = "host",
                [KeyName.DatabaseName] = "database",
                [KeyName.PortNumber] = 9499,
            };

            var result = _sut.BuildConnectionString(properties);

            Assert.Equal("Data Source=host,9499;Initial Catalog=database;User ID=user;Password=password;Pooling=True;Authentication=SqlPassword", result);
        }

        [Fact]
        public void BuildConnectionString_WithStringPort_Sets_From_Dictionary()
        {
            var properties = new Dictionary<string, object>
            {
                [KeyName.Password] = "password",
                [KeyName.Username] = "user",
                [KeyName.Host] = "host",
                [KeyName.DatabaseName] = "database",
                [KeyName.PortNumber] = "9499"
            };

            var result = _sut.BuildConnectionString(properties);

            Assert.Equal("Data Source=host,9499;Initial Catalog=database;User ID=user;Password=password;Pooling=True;Authentication=SqlPassword", result);
        }

        [Fact]
        public void BuildConnectionString_WithInvalidPort_Sets_From_Dictionary()
        {
            var properties = new Dictionary<string, object>
            {
                [KeyName.Password] = "password",
                [KeyName.Username] = "user",
                [KeyName.Host] = "host",
                [KeyName.DatabaseName] = "database",
                [KeyName.PortNumber] = new object(),
            };

            var result = _sut.BuildConnectionString(properties);

            Assert.Equal("Data Source=host,1433;Initial Catalog=database;User ID=user;Password=password;Pooling=True;Authentication=SqlPassword", result);
        }
    }
}
