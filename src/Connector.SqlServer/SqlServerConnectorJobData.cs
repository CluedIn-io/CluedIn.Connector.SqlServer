using System.Collections.Generic;
using CluedIn.Core.Crawling;

namespace CluedIn.Connector.SqlServer
{
    public class SqlServerConnectorJobData : CrawlJobData
    {
        public SqlServerConnectorJobData(IDictionary<string, object> configuration)
        {
            if (configuration == null)
            {
                return;
            }

            Username = GetValue<string>(configuration, SqlServerConstants.KeyName.Username);
            DatabaseName = GetValue<string>(configuration, SqlServerConstants.KeyName.DatabaseName);
            Host = GetValue<string>(configuration, SqlServerConstants.KeyName.Host);
            Password = GetValue<string>(configuration, SqlServerConstants.KeyName.Password);
            PortNumber = GetValue<int>(configuration, SqlServerConstants.KeyName.PortNumber, 1433);
        }

        public IDictionary<string, object> ToDictionary()
        {
            return new Dictionary<string, object> {
                { SqlServerConstants.KeyName.Username, Username },
                { SqlServerConstants.KeyName.DatabaseName, DatabaseName },
                { SqlServerConstants.KeyName.Host, Host },
                { SqlServerConstants.KeyName.Password, Password },
                { SqlServerConstants.KeyName.PortNumber, PortNumber }
            };
        }

        public string Username { get; set; }

        public string DatabaseName { get; set; }

        public string Host { get; set; }

        public string Password { get; set; }

        public int PortNumber { get; set; }
    }
}
