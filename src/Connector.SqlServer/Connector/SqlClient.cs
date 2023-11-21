using Microsoft.Data.SqlClient;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

namespace CluedIn.Connector.SqlServer.Connector
{
    public class SqlClient : ISqlClient
    {
        private readonly int _defaultPort = 1433;

        public string BuildConnectionString(IReadOnlyDictionary<string, object> config)
        {
            var connectionStringBuilder = new SqlConnectionStringBuilder
            {
                Authentication = SqlAuthenticationMethod.SqlPassword,
                Password = (string)config[SqlServerConstants.KeyName.Password],
                UserID = (string)config[SqlServerConstants.KeyName.Username],
                DataSource = (string)config[SqlServerConstants.KeyName.Host],
                InitialCatalog = (string)config[SqlServerConstants.KeyName.DatabaseName],
                Pooling = true,
                MultipleActiveResultSets = false,
                MaxPoolSize = 200
            };

            if (config.TryGetValue(SqlServerConstants.KeyName.PortNumber, out var portEntry) && int.TryParse(portEntry.ToString(), out var port))
                connectionStringBuilder.DataSource = $"{connectionStringBuilder.DataSource},{port}";
            else
                connectionStringBuilder.DataSource = $"{connectionStringBuilder.DataSource},{_defaultPort}";

            return connectionStringBuilder.ToString();
        }

        public async Task<SqlConnection> BeginConnection(IReadOnlyDictionary<string, object> config)
        {
            var connectionString = BuildConnectionString(config);
            var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();
            return connection;
        }

        public Task<DataTable> GetTableColumns(SqlConnection connection, string tableName, string schema)
        {
            if (string.IsNullOrWhiteSpace(tableName))
                tableName = (string)null;

            var restrictionValues = new string[4]
            {
                null,
                schema,
                tableName,
                null
            };

            return Task.FromResult(connection.GetSchema("Columns", restrictionValues));
        }

        public Task<DataTable> GetTables(SqlConnection connection, string tableName = null, string schema = null)
        {
            if (string.IsNullOrWhiteSpace(tableName))
                tableName = (string)null;

            var restrictionValues = new string[4]
            {
                null,
                schema,
                tableName,
                null
            };

            return Task.FromResult(connection.GetSchema("tables", restrictionValues));
        }

        public async Task ExecuteCommandInTransactionAsync(SqlTransaction transaction, string commandText, IEnumerable<SqlParameter> param = null)
        {
            var cmd = transaction.Connection.CreateCommand();
            cmd.CommandText = commandText;
            cmd.Transaction = transaction;

            if (param != null)
            {
                foreach (var parameter in param)
                {
                    SanitizeParameter(parameter);
                    cmd.Parameters.Add(parameter);
                }
            }

            await cmd.ExecuteNonQueryAsync();
        }

        public async Task<ConnectionAndTransaction> BeginTransaction(IReadOnlyDictionary<string, object> config)
        {
            var connectionString = BuildConnectionString(config);
            var connection = Activator.CreateInstance(typeof(SqlConnection), connectionString) as SqlConnection;

            // ReSharper disable once PossibleNullReferenceException
            await connection.OpenAsync();
            return new ConnectionAndTransaction(connection, await connection.BeginTransactionAsync() as SqlTransaction);
        }

        public static void SanitizeParameter(IDbDataParameter parameter)
        {
            // We need to check if the object is an typeof List, Array, etc. If it is, then the process will fail because of "The CLR Type <> isn't supported" error.

            parameter.Value ??= DBNull.Value;
            var parameterType = parameter.Value.GetType();

            // check if parameterType is an Array or List, covert the Value to string.
            if (parameterType.IsArray || parameterType.IsGenericType)
            {
                parameter.Value = string.Join(",", ((IList)parameter.Value).Cast<string>());
            }

            // TODO: Further investigate, futureproof and test a proper way to handle if the Value's object is not a List<>.
            // If not handled in the above condition, it will fail when we add the Parameter to the command.
        }

        public async Task<DataTable> GetTableColumns(SqlTransaction transaction, string tableName, string schema = null)
        {
            return await GetRestrictedSchema(transaction.Connection, "Columns", tableName, schema);
        }

        public async Task<DataTable> GetTables(SqlTransaction transaction, string name = null, string schema = null)
        {
            return await GetRestrictedSchema(transaction.Connection, "Tables", name, schema);
        }

        public static Task<DataTable> GetRestrictedSchema(
            SqlConnection connection,
            string collectionName,
            string tableName = null,
            string schema = null)
        {
            if (string.IsNullOrWhiteSpace(tableName))
            {
                tableName = null;
            }

            // Read more about syntax of restrictions here: https://learn.microsoft.com/en-us/dotnet/framework/data/adonet/schema-restrictions
            var restrictions = new[] { null, schema, tableName, null };
            return Task.FromResult(connection.GetSchema(collectionName, restrictions));
        }
    }
}
