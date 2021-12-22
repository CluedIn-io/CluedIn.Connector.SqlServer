using CluedIn.Core.Providers;
using CluedIn.Connector.Common;
using System;

namespace CluedIn.Connector.SqlServer
{
    public class SqlServerConstants : ConfigurationConstantsBase, ISqlServerConstants
    {
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
                    name = CommonConfigurationNames.Host,
                    displayName = CommonConfigurationNames.Host,
                    type = "input",
                    isRequired = true
                },
                new Control
                {
                    name = CommonConfigurationNames.DatabaseName,
                    displayName = CommonConfigurationNames.DatabaseName,
                    type = "input",
                    isRequired = true
                },
                new Control
                {
                    name = CommonConfigurationNames.Username,
                    displayName = CommonConfigurationNames.Username,
                    type = "input",
                    isRequired = true
                },
                new Control
                {
                    name = CommonConfigurationNames.Password,
                    displayName = CommonConfigurationNames.Password,
                    type = "password",
                    isRequired = true
                },
                new Control
                {
                    name = CommonConfigurationNames.PortNumber,
                    displayName = CommonConfigurationNames.PortNumber,
                    type = "input",
                    isRequired = false
                }
            }
        };
    }
}
