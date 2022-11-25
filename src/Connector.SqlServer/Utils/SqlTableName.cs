using CluedIn.Core.Connectors;

#nullable enable

namespace CluedIn.Connector.SqlServer.Utils
{
    public sealed class SqlTableName
    {
        public const string DefaultSchema = "dbo";

        /// <summary>
        /// Creates sanitized table name from unsafe name
        /// </summary>
        public static SqlTableName FromUnsafeName(string rawTableName, SqlName schema)
        {
            return new SqlTableName(SqlName.FromUnsafe(rawTableName), schema);
        }

        /// <summary>
        /// Creates sanitized table name from unsafe name
        /// </summary>
        public static SqlTableName FromUnsafeName(string rawTableName, IConnectorConnection config) =>
            FromUnsafeName(rawTableName, config.GetSchema()); 

        public SqlName Schema { get; }

        /// <summary>
        /// Returns value in [Schema].[Name] format, which is suitable for query embedding
        /// </summary>
        public string FullyQualifiedName { get; }

        public SqlName LocalName { get; }

        public SqlTableName(SqlName localName, SqlName schema)
        {
            Schema = schema;
            LocalName = localName;
            FullyQualifiedName = $"[{schema}].[{localName}]";
        }

        public override string ToString() => FullyQualifiedName;
    }
}
