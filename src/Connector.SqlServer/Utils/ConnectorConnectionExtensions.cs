using CluedIn.Core.Connectors;

namespace CluedIn.Connector.SqlServer.Utils
{
    internal static class ConnectorConnectionExtensions
    {
        /// <summary>
        /// Returns sanitized configured schema or default one if it's not specified by user.
        /// </summary>
        public static SqlName GetSchema(this IConnectorConnection config)
        {
            if (config.Authentication.TryGetValue(SqlServerConstants.KeyName.Schema, out var value) && value is string schema)
            {
                var sanitizedSchema = schema.ToSanitizedSqlName();

                if (!string.IsNullOrEmpty(sanitizedSchema))
                {
                    return SqlName.FromSanitized(schema);
                }
            }

            return SqlName.FromSanitized(SqlTableName.DefaultSchema);
        }
    }
}
