using System.Collections.Generic;
using CluedIn.Connector.SqlServer.Connector;
using Xunit;
using CluedIn.Connector.Common.Configurations;
using CluedIn.Core;

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
                [CommonConfigurationNames.Password.ToCamelCase()] = "password",
                [CommonConfigurationNames.Username.ToCamelCase()] = "user",
                [CommonConfigurationNames.Host.ToCamelCase()] = "host",
                [CommonConfigurationNames.DatabaseName.ToCamelCase()] = "database"
            };

            var result = _sut.BuildConnectionString(properties);

            Assert.Equal("Data Source=host,1433;Initial Catalog=database;User ID=user;Password=password;Pooling=True;Authentication=SqlPassword", result);
        }

        [Fact]
        public void BuildConnectionString_WithPort_Sets_From_Dictionary()
        {
            var properties = new Dictionary<string, object>
            {
                [CommonConfigurationNames.Password.ToCamelCase()] = "password",
                [CommonConfigurationNames.Username.ToCamelCase()] = "user",
                [CommonConfigurationNames.Host.ToCamelCase()] = "host",
                [CommonConfigurationNames.DatabaseName.ToCamelCase()] = "database",
                [CommonConfigurationNames.PortNumber.ToCamelCase()] = 9499,
            };

            var result = _sut.BuildConnectionString(properties);

            Assert.Equal("Data Source=host,9499;Initial Catalog=database;User ID=user;Password=password;Pooling=True;Authentication=SqlPassword", result);
        }

        [Fact]
        public void BuildConnectionString_WithStringPort_Sets_From_Dictionary()
        {
            var properties = new Dictionary<string, object>
            {
                [CommonConfigurationNames.Password.ToCamelCase()] = "password",
                [CommonConfigurationNames.Username.ToCamelCase()] = "user",
                [CommonConfigurationNames.Host.ToCamelCase()] = "host",
                [CommonConfigurationNames.DatabaseName.ToCamelCase()] = "database",
                [CommonConfigurationNames.PortNumber.ToCamelCase()] = "9499",
            };

            var result = _sut.BuildConnectionString(properties);

            Assert.Equal("Data Source=host,9499;Initial Catalog=database;User ID=user;Password=password;Pooling=True;Authentication=SqlPassword", result);
        }

        [Fact]
        public void BuildConnectionString_WithInvalidPort_Sets_From_Dictionary()
        {
            var properties = new Dictionary<string, object>
            {
                [CommonConfigurationNames.Password.ToCamelCase()] = "password",
                [CommonConfigurationNames.Username.ToCamelCase()] = "user",
                [CommonConfigurationNames.Host.ToCamelCase()] = "host",
                [CommonConfigurationNames.DatabaseName.ToCamelCase()] = "database",
                [CommonConfigurationNames.PortNumber.ToCamelCase()] = new object(),
            };

            var result = _sut.BuildConnectionString(properties);

            Assert.Equal("Data Source=host,1433;Initial Catalog=database;User ID=user;Password=password;Pooling=True;Authentication=SqlPassword", result);
        }
    }
}
