using Microsoft.Data.SqlClient.Server;
using System;
using System.Data;
using static CluedIn.Connector.SqlServer.Utils.SqlColumnHelper;

namespace CluedIn.Connector.SqlServer.Utils
{
    internal record ColumnDefinition(
        string Name,
        ConnectorSqlType ConnectorSqlType,
        bool CanBeNull = false,
        bool IsPrimaryKey = false,
        bool IsPivotKey = false,
        bool IsPivotValueKey = false,
        bool AddIndex = false)
    {
        public SqlMetaData ToSqlMetaData()
        {
            var sanitizedName = Name.ToSanitizedSqlName();

            if (ConnectorSqlType.Equals(SqlColumnHelper.UniqueIdentifier))
                return new SqlMetaData(sanitizedName, SqlDbType.UniqueIdentifier);

            if (ConnectorSqlType.Equals(SqlColumnHelper.Int))
                return new SqlMetaData(sanitizedName, SqlDbType.Int);

            if (ConnectorSqlType.Equals(SqlColumnHelper.Char24))
                return new SqlMetaData(sanitizedName, SqlDbType.Char);

            if (ConnectorSqlType.Equals(SqlColumnHelper.NVarchar256))
                return new SqlMetaData(sanitizedName, SqlDbType.NVarChar, 256);

            if (ConnectorSqlType.Equals(SqlColumnHelper.NVarchar1024))
                return new SqlMetaData(sanitizedName, SqlDbType.NVarChar, 1024);

            if (ConnectorSqlType.Equals(SqlColumnHelper.NVarcharMax))
                return new SqlMetaData(sanitizedName, SqlDbType.NVarChar, SqlMetaData.Max);

            if (ConnectorSqlType.Equals(SqlColumnHelper.DateTimeOffset7))
                return new SqlMetaData(sanitizedName, SqlDbType.DateTimeOffset);

            throw new ArgumentOutOfRangeException();
        }
    }
}
