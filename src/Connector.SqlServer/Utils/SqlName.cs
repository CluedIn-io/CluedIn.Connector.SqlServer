using CluedIn.Core.Connectors;
using System;

namespace CluedIn.Connector.SqlServer.Utils
{
    public readonly struct SqlName
    {
        public string Value { get; }

        private SqlName(string value)
        {
            Value = value;
        }

        public static SqlName FromUnsafe(string value)
        {
            if (string.IsNullOrEmpty(value)) throw new ArgumentException("Value cannot be null or empty.", nameof(value));

            var sanitizedValue = value.ToSanitizedSqlName();
            if (string.IsNullOrEmpty(sanitizedValue))
            {
                throw new ArgumentException("Name cannot be empty after being sanitized", nameof(value));
            }

            return new SqlName(sanitizedValue);
        }

        public static SqlName FromUnsafeMainTable(string value)
        {
            if (string.IsNullOrEmpty(value))
                throw new ArgumentException("Value cannot be null or empty.", nameof(value));

            var sanitizedValue = value.ToSanitizedMainTableName();
            if (string.IsNullOrEmpty(sanitizedValue))
            {
                throw new ArgumentException("Name cannot be empty after being sanitized", nameof(value));
            }

            return new SqlName(sanitizedValue);
        }

        public static SqlName FromSanitized(string value)
        {
            if (string.IsNullOrEmpty(value)) throw new ArgumentException("Value cannot be null or empty.", nameof(value));

            return new SqlName(value);
        }

        public static implicit operator string (SqlName value)
        {
            return value.Value;
        }

        public override string ToString() => Value;

        public SqlTableName ToTableName(SqlName schema) => new SqlTableName(this, schema);

        public SqlTableName ToTableName(IConnectorConnectionV2 config) => new SqlTableName(this, config.GetSchema());
    }
}
