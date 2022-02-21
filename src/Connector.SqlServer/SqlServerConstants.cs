using CluedIn.Core.Providers;
using System;
using CluedIn.Connector.Common.Configurations;
using CluedIn.Core;

namespace CluedIn.Connector.SqlServer
{
    public class SqlServerConstants : ConfigurationConstantsBase, ISqlServerConstants
    {
        public struct KeyName
        {
            public const string Host = "host";
            public const string DatabaseName = "databaseName";
            public const string Username = "username";
            public const string Password = "password";
            public const string PortNumber = "portNumber";
        }

        public SqlServerConstants() : base(Guid.Parse("838E4EA2-80E0-4B60-B1D1-F052BFCD0CAF"),
            "Sql Server Connector",
            "SqlServerConnector",
            "Resources.microsoft-sql-server-logo.svg",
            "https://www.microsoft.com/en-us/sql-server",
            "Supports publishing of data to external SQL databases.",
            SqlServerAuthMethods,
            "Provides connectivity to a Microsoft Sql Server database")
        {
        }

        private static AuthMethods SqlServerAuthMethods => new AuthMethods
        {
            token = new[]
            {
                new Control
                {
                    name = KeyName.Host,
                    displayName = CommonConfigurationNames.Host.ToDisplayName(),
                    type = "input",
                    isRequired = true
                },
                new Control
                {
                    name = KeyName.DatabaseName,
                    displayName = CommonConfigurationNames.DatabaseName.ToDisplayName(),
                    type = "input",
                    isRequired = true
                },
                new Control
                {
                    name = KeyName.Username,
                    displayName = CommonConfigurationNames.Username.ToDisplayName(),
                    type = "input",
                    isRequired = true
                },
                new Control
                {
                    name = KeyName.Password,
                    displayName = CommonConfigurationNames.Password.ToDisplayName(),
                    type = "password",
                    isRequired = true
                },
                new Control
                {
                    name = KeyName.PortNumber,
                    displayName = CommonConfigurationNames.PortNumber.ToDisplayName(),
                    type = "input",
                    isRequired = false
                }
            }
        };
    }
}
