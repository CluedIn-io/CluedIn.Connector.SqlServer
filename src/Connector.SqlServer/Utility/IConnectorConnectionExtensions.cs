using CluedIn.Connector.Common.Configurations;
using CluedIn.Core.Connectors;

namespace CluedIn.Connector.SqlServer.Utility
{
    public static class IConnectorConnectionExtensions
    {
        public static SanitizedSqlString GetSchema(this IConnectorConnection connection)
        {
            if (connection.Authentication.TryGetValue(CommonConfigurationNames.Schema, out var obj)
                && obj is string schema
                && !string.IsNullOrWhiteSpace(schema))
                return new SanitizedSqlString(schema);

            return new SanitizedSqlString(SqlServerConstants.DefaultSchema);
        }
    }
}
