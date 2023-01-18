using System.Collections.Generic;
using CluedIn.Connector.SqlServer.Connector;
using Xunit;
using CluedIn.Connector.Common.Configurations;

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
                [SqlServerConstants.KeyName.Password] = "password",
                [SqlServerConstants.KeyName.Username] = "user",
                [SqlServerConstants.KeyName.Host] = "host",
                [SqlServerConstants.KeyName.DatabaseName] = "database"
            };

            var result = _sut.BuildConnectionString(properties);

            Assert.Equal("Data Source=host,1433;Initial Catalog=database;User ID=user;Password=password;Pooling=True;Max Pool Size=200;Multiple Active Result Sets=True;Authentication=SqlPassword", result);
        }

        [Fact]
        public void BuildConnectionString_WithPort_Sets_From_Dictionary()
        {
            var properties = new Dictionary<string, object>
            {
                [SqlServerConstants.KeyName.Password] = "password",
                [SqlServerConstants.KeyName.Username] = "user",
                [SqlServerConstants.KeyName.Host] = "host",
                [SqlServerConstants.KeyName.DatabaseName] = "database",
                [SqlServerConstants.KeyName.PortNumber] = 9499,
            };

            var result = _sut.BuildConnectionString(properties);

            Assert.Equal("Data Source=host,9499;Initial Catalog=database;User ID=user;Password=password;Pooling=True;Max Pool Size=200;Multiple Active Result Sets=True;Authentication=SqlPassword", result);
        }

        [Fact]
        public void BuildConnectionString_WithStringPort_Sets_From_Dictionary()
        {
            var properties = new Dictionary<string, object>
            {
                [SqlServerConstants.KeyName.Password] = "password",
                [SqlServerConstants.KeyName.Username] = "user",
                [SqlServerConstants.KeyName.Host] = "host",
                [SqlServerConstants.KeyName.DatabaseName] = "database",
                [SqlServerConstants.KeyName.PortNumber] = "9499",
            };

            var result = _sut.BuildConnectionString(properties);

            Assert.Equal("Data Source=host,9499;Initial Catalog=database;User ID=user;Password=password;Pooling=True;Max Pool Size=200;Multiple Active Result Sets=True;Authentication=SqlPassword", result);
        }

        [Fact]
        public void BuildConnectionString_WithInvalidPort_Sets_From_Dictionary()
        {
            var properties = new Dictionary<string, object>
            {
                [SqlServerConstants.KeyName.Password] = "password",
                [SqlServerConstants.KeyName.Username] = "user",
                [SqlServerConstants.KeyName.Host] = "host",
                [SqlServerConstants.KeyName.DatabaseName] = "database",
                [SqlServerConstants.KeyName.PortNumber] = new object(),
            };

            var result = _sut.BuildConnectionString(properties);

            Assert.Equal("Data Source=host,1433;Initial Catalog=database;User ID=user;Password=password;Pooling=True;Max Pool Size=200;Multiple Active Result Sets=True;Authentication=SqlPassword", result);
        }
    }
}
