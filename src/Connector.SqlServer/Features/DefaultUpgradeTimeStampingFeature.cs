//using CluedIn.Connector.SqlServer.Connector;
//using CluedIn.Connector.SqlServer.Utils;
//using CluedIn.Core.Configuration;
//using CluedIn.Core.Connectors;
//using CluedIn.Core.Data.Vocabularies;
//using CluedIn.Core.Streams.Models;
//using Microsoft.Data.SqlClient;
//using System;
//using System.Configuration;
//using System.Data;
//using System.Linq;
//using System.Threading.Tasks;

//namespace CluedIn.Connector.SqlServer.Features
//{
//    public class DefaultUpgradeTimeStampingFeature : IUpgradeTimeStampingFeature
//    {
//        public virtual async Task VerifyTimeStampColumnExist(ISqlClient client, IConnectorConnectionV2 config, SqlTransaction transaction, IReadOnlyStreamModel stream)
//        {
//            var tableName = SqlTableName.FromUnsafeName(stream.ContainerName, config);
//            var defaultMaxSize = ConfigurationManagerEx.AppSettings.GetValue(SqlServerConnector.DefaultSizeForFieldConfigurationKey, "max");

//            // Adapted from the command that Microsoft SQL client uses to get table columns
//            var sqlCommandText = $@"EXEC sys.sp_columns_managed @Catalog, @Owner, @Table, @Column, 0";
//            var command = transaction.Connection.CreateCommand();
//            command.Transaction = transaction;
//            command.CommandText = sqlCommandText;
//            command.Parameters.AddWithValue("@Catalog", DBNull.Value);
//            command.Parameters.AddWithValue("@Owner", tableName.Schema.Value);
//            command.Parameters.AddWithValue("@Table", tableName.LocalName.Value);
//            command.Parameters.AddWithValue("@Column", DBNull.Value);

//            var dataTable = new DataTable();
//            var adapter = new SqlDataAdapter(command);
//            adapter.Fill(dataTable);
//            var result = (from DataRow row in dataTable.Rows
//                          let name = row["COLUMN_NAME"] as string
//                          let rawType = row["DATA_TYPE"] as string
//                          select new { Name = name, RawDataType = rawType }).ToList();

//            if (result.Any() && !result.Any(n => n.Name.Equals("TimeStamp", StringComparison.OrdinalIgnoreCase)))
//            {
//                var columnName = "TimeStamp";
//                var addTimeStampSql =
//                    $"alter table {tableName.FullyQualifiedName} add [{columnName}] {SqlColumnHelper.GetColumnType(VocabularyKeyDataType.DateTime, defaultMaxSize)}";
//                await client.ExecuteCommandInTransactionAsync(transaction, addTimeStampSql);
//            }
//        }
//    }
//}
