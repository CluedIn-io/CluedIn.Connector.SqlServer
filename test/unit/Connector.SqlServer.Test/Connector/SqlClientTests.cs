using System.Collections.Generic;
using CluedIn.Connector.SqlServer.Connector;
using FluentAssertions;
using Xunit;

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
                [SqlServerConstants.KeyName.DatabaseName] = "database",
            };

            var result = _sut.BuildConnectionString(properties);

            Assert.Equal("Data Source=host,1433;Initial Catalog=database;User ID=user;Password=password;Pooling=True;Max Pool Size=200;Authentication=SqlPassword", result);
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

            Assert.Equal("Data Source=host,9499;Initial Catalog=database;User ID=user;Password=password;Pooling=True;Max Pool Size=200;Authentication=SqlPassword", result);
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

            Assert.Equal("Data Source=host,9499;Initial Catalog=database;User ID=user;Password=password;Pooling=True;Max Pool Size=200;Authentication=SqlPassword", result);
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

            Assert.Equal("Data Source=host,1433;Initial Catalog=database;User ID=user;Password=password;Pooling=True;Max Pool Size=200;Authentication=SqlPassword", result);
        }

        [Fact]
        public void BuildConnectionString_WithConnectionPoolSize_Sets_From_Dictionary()
        {
            // arrange
            var properties = new Dictionary<string, object>
            {
                [SqlServerConstants.KeyName.Password] = "password",
                [SqlServerConstants.KeyName.Username] = "user",
                [SqlServerConstants.KeyName.Host] = "host",
                [SqlServerConstants.KeyName.DatabaseName] = "database",
                [SqlServerConstants.KeyName.ConnectionPoolSize] = 10,
            };

            // act
            var result = _sut.BuildConnectionString(properties);

            // assert
            Assert.Equal("Data Source=host,1433;Initial Catalog=database;User ID=user;Password=password;Pooling=True;Max Pool Size=10;Authentication=SqlPassword", result);
        }

        [Fact] public void VerifyConnectionProperties_WithValidProperties_ReturnsTrue()
        {
            // arrange
            var properties = new Dictionary<string, object>
            {
                [SqlServerConstants.KeyName.Password] = "password",
                [SqlServerConstants.KeyName.Username] = "user",
                [SqlServerConstants.KeyName.Host] = "host",
                [SqlServerConstants.KeyName.DatabaseName] = "database",
                [SqlServerConstants.KeyName.PortNumber] = "9433",
                [SqlServerConstants.KeyName.ConnectionPoolSize] = "10"
            };

            // act
            var result = _sut.VerifyConnectionProperties(properties, out var connectionConfigurationError);

            // assert
            result.Should().BeTrue();
            connectionConfigurationError.Should().BeNull();
        }

        [Fact]
        public void VerifyConnectionProperties_WithInvalidPort_ReturnsFalse()
        {
            // arrange
            var properties = new Dictionary<string, object>
            {
                [SqlServerConstants.KeyName.Password] = "password",
                [SqlServerConstants.KeyName.Username] = "user",
                [SqlServerConstants.KeyName.Host] = "host",
                [SqlServerConstants.KeyName.DatabaseName] = "database",
                [SqlServerConstants.KeyName.PortNumber] = "invalidPort",
            };

            // act
            var result = _sut.VerifyConnectionProperties(properties, out var connectionConfigurationError);

            // assert
            result.Should().BeFalse();
            connectionConfigurationError.ErrorMessage.Should().Be("Port number was set, but could not be read as a number");
        }

        [Fact]
        public void VerifyConnectionProperties_WithInvalidConnectionPoolSize_ReturnsFalse()
        {
            // arrange
            var properties = new Dictionary<string, object>
            {
                [SqlServerConstants.KeyName.Password] = "password",
                [SqlServerConstants.KeyName.Username] = "user",
                [SqlServerConstants.KeyName.Host] = "host",
                [SqlServerConstants.KeyName.DatabaseName] = "database",
                [SqlServerConstants.KeyName.ConnectionPoolSize] = "invalidPort",
            };

            // act
            var result = _sut.VerifyConnectionProperties(properties, out var connectionConfigurationError);

            // assert
            result.Should().BeFalse();
            connectionConfigurationError.ErrorMessage.Should().Be("Connection pool size was set, but could not be read as a number");
        }
    }
}
