using CluedIn.Core.Providers;
using System;
using System.Linq;
using System.Collections.Generic;
using CluedIn.Core.Data.Relational;
using CluedIn.Core.Net.Mail;

namespace CluedIn.Connector.SqlServer
{
    public class SqlServerConstants : ISqlServerConstants
    {
        public struct KeyName
        {
            public const string Host = "host";
            public const string Schema = "schema";
            public const string DatabaseName = "databaseName";
            public const string Username = "username";
            public const string Password = "password";
            public const string PortNumber = "portNumber";
            public const string ConnectionPoolSize = "connectionPoolSize";
        }

        public SqlServerConstants()
        {
            ProviderId = Guid.Parse("838E4EA2-80E0-4B60-B1D1-F052BFCD0CAF");
            ProviderName = "Sql Server Connector";
            ComponentName = "SqlServerConnector";
            Icon = "Resources.microsoft-sql-server-logo.svg";
            Domain = "https://www.microsoft.com/en-us/sql-server";
            About = "Supports publishing of data to external SQL databases.";
            AuthMethods = SqlServerAuthMethods;
            GuideDetails = "Provides connectivity to a Microsoft Sql Server database";
            Properties = Enumerable.Empty<Control>();
            Type = IntegrationType.Connector;
            GuideInstructions = "Provide authentication instructions here, if applicable";
            FeatureCategory = "Connectivity";
            FeatureDescription = "Provides connectivity to a Microsoft Sql Server database";
        }

        private string GuideDetails { get; }
        private string GuideInstructions { get; }
        private string ProviderName { get; }
        private string ComponentName { get; }
        private string FeatureCategory { get; }
        private string FeatureDescription { get; }

        public Guid ProviderId { get; }
        public string Icon { get; }
        public string Domain { get; }
        public string About { get; }
        public AuthMethods AuthMethods { get; }
        public IEnumerable<Control> Properties { get; }
        public IntegrationType Type { get; }

        public Guide Guide => new Guide
        {
            Instructions = GuideInstructions,
            Value = new List<string> { About },
            Details = GuideDetails
        };

        private static AuthMethods SqlServerAuthMethods => new AuthMethods
        {
            token = new[]
            {
                new Control
                {
                    name = KeyName.Host,
                    displayName = "Host",
                    type = "input",
                    isRequired = true
                },
                new Control
                {
                    name = KeyName.DatabaseName,
                    displayName = "Database Name",
                    type = "input",
                    isRequired = true
                },
                new Control
                {
                    name = KeyName.Username,
                    displayName = "Username",
                    type = "input",
                    isRequired = true
                },
                new Control
                {
                    name = KeyName.Password,
                    displayName = "Password",
                    type = "password",
                    isRequired = true
                },
                new Control
                {
                    name = KeyName.PortNumber,
                    displayName = "Port Number",
                    type = "input",
                    isRequired = false
                },
                new Control
                {
                    name = KeyName.Schema,
                    displayName = "Schema",
                    type = "input",
                    isRequired = false
                }
            }
        };

        private ComponentEmailDetails ComponentEmailDetails => new ComponentEmailDetails
        {
            Features = new Dictionary<string, string> { { FeatureCategory, FeatureDescription } },
            Icon = ProviderIconFactory.CreateConnectorUri(ProviderId),
            ProviderName = ProviderName,
            ProviderId = ProviderId
        };

        public IProviderMetadata CreateProviderMetadata()
        {
            return new ProviderMetadata
            {
                Id = ProviderId,
                Name = ProviderName,
                ComponentName = ComponentName,
                Type = Type.ToString(),
                ComponentEmailDetails = ComponentEmailDetails
            };
        }
    }
}
