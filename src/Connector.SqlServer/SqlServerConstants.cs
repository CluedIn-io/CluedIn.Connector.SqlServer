using CluedIn.Connector.Common.Configurations;
using CluedIn.Connector.SqlServer.Utility;
using CluedIn.Core.Providers;
using System;

namespace CluedIn.Connector.SqlServer
{
    public class SqlServerConstants : ConfigurationConstantsBase, ISqlServerConstants
    {
        public static readonly string DefaultSchema = "dbo";

        public SqlServerConstants() : base(providerId: Guid.Parse("838E4EA2-80E0-4B60-B1D1-F052BFCD0CAF"),
            providerName: "Sql Server Connector",
            componentName: "SqlServerConnector",
            icon: "Resources.microsoft-sql-server-logo.svg",
            domain: "https://www.microsoft.com/en-us/sql-server",
            about: "Supports publishing of data to external SQL databases.",
            authMethods: SqlServerAuthMethods,
            guideDetails: "Provides connectivity to a Microsoft Sql Server database")
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
                },
                new Control
                {
                    name = CommonConfigurationNames.Schema,
                    displayName = CommonConfigurationNames.Schema.ToDisplayName(),
                    type = "input",
                    isRequired = false
                }
            }
        };
    }
}
