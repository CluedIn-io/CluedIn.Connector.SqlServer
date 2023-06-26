using CluedIn.Connector.SqlServer.Connector;
using CluedIn.Core.Configuration;
using CluedIn.Core.Connectors;
using System.Data;

namespace CluedIn.Connector.SqlServer.Utils
{
    public class SqlColumnHelper
    {
        public record ConnectorSqlType(SqlDbType SqlType, string StringRepresentation);

        public static ConnectorSqlType UniqueIdentifier => new(SqlDbType.UniqueIdentifier, "uniqueidentifier");
        public static ConnectorSqlType Int => new(SqlDbType.Int, "int");
        public static ConnectorSqlType Char24 => new(SqlDbType.Char, "char(24)");
        public static ConnectorSqlType NVarchar256 => new(SqlDbType.NVarChar, "nvarchar(256)");
        public static ConnectorSqlType NVarchar1024 => new(SqlDbType.NVarChar, "nvarchar(1024)");
        public static ConnectorSqlType NVarcharMax => new(SqlDbType.NVarChar, "nvarchar(max)");
        public static ConnectorSqlType DateTimeOffset7 => new(SqlDbType.DateTimeOffset, "datetimeoffset(7)");

        public static ConnectorSqlType GetColumnType(ConnectorPropertyDataType type)
        {
            var size = ConfigurationManagerEx.AppSettings.GetValue(SqlServerConnector.DefaultSizeForFieldConfigurationKey, "max");
            return new ConnectorSqlType(SqlDbType.NVarChar, $"nvarchar({size})");
        }

        public static ConnectorSqlType GetColumnTypeForPropertyValue()
        {
            var size = ConfigurationManagerEx.AppSettings.GetValue(SqlServerConnector.DefaultSizeForFieldConfigurationKey, "max");
            return new ConnectorSqlType(SqlDbType.NVarChar, $"nvarchar({size})");
        }
    }
}
