using CluedIn.Connector.Common.Configurations;
using CluedIn.Connector.Common.Helpers;
using CluedIn.Core.Connectors;

namespace CluedIn.Connector.SqlServer
{
    public static class IConnectorConnectionExtensions
    {
        public static string GetSchema(this IConnectorConnection connection)
        {
            if (connection.Authentication.TryGetValue(CommonConfigurationNames.Schema, out var obj)
                && obj is string schema
                && !string.IsNullOrWhiteSpace(schema))
                return SqlStringSanitizer.Sanitize(schema);

            return SqlServerConstants.DefaultSchema;
        }
    }
}
