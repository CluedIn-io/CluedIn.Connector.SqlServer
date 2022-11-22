using CluedIn.Connector.Common.Helpers;
using CluedIn.Core.Connectors;
using System;

#nullable enable

namespace CluedIn.Connector.SqlServer.Utils
{
    public sealed class SqlTableName
    {
        public const string DefaultSchema = "dbo";

        /// <summary>
        /// Creates sanitized table name from unsafe name
        /// </summary>
        public static SqlTableName FromUnsafeName(string rawTableName, string sanitizedSchema)
        {
            if (string.IsNullOrEmpty(rawTableName)) throw new ArgumentException("Value cannot be null or empty.", nameof(rawTableName));
            if (string.IsNullOrEmpty(sanitizedSchema)) throw new ArgumentException("Value cannot be null or empty.", nameof(sanitizedSchema));

            var sanitizedName = rawTableName.ToSanitizedSqlName();
            if (string.IsNullOrEmpty(sanitizedName))
            {
                throw new ArgumentException("Table name cannot be empty after being sanitized", nameof(sanitizedSchema));
            }

            return new SqlTableName(sanitizedName, sanitizedSchema);
        }

        /// <summary>
        /// Creates sanitized table name from unsafe name
        /// </summary>
        public static SqlTableName FromUnsafeName(string rawTableName, IConnectorConnection config) =>
            FromUnsafeName(rawTableName, config.GetSchema());

        public string Schema { get; }

        /// <summary>
        /// Returns value in [Schema].[Name] format, which is suitable for query embedding
        /// </summary>
        public string FullyQualifiedName { get; }

        public string LocalName { get; }

        private SqlTableName(string sanitizedName, string schema)
        {
            Schema = schema;
            LocalName = sanitizedName;
            FullyQualifiedName = $"[{schema}].[{sanitizedName}]";
        }

        public override string ToString() => FullyQualifiedName;
    }
}
