using CluedIn.Core.Connectors;

namespace CluedIn.Connector.SqlServer.Utils
{
    internal static class ConnectorConnectionExtensions
    {
        /// <summary>
        /// Returns sanitized configured schema or default one if it's not specified by user.
        /// </summary>
        public static SqlName GetSchema(this IConnectorConnectionV2 config)
        {
            if (config.Authentication.TryGetValue(SqlServerConstants.KeyName.Schema, out var value) &&
                value is string schema &&
                !string.IsNullOrEmpty(schema))
            {
                var sanitizedSchema = schema.ToSanitizedSqlName();
                return SqlName.FromSanitized(sanitizedSchema);
            }

            return SqlName.FromSanitized(SqlTableName.DefaultSchema);
        }
    }
}
