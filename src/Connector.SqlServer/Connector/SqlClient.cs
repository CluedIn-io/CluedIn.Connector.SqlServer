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
        private readonly int _defaultConnectionPoolSize = 200;

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
                // Turn off unconditionally for now. Later maybe should be coming from configuration.
                // Is needed as new SqlClient library encrypts by default.
                Encrypt = false
            };

            // Configure port
            {
                var port = _defaultPort;
                if (config.TryGetValue(SqlServerConstants.KeyName.PortNumber, out var portEntry) &&
                    !string.IsNullOrEmpty(portEntry.ToString()) &&
                    int.TryParse(portEntry.ToString(), out var parsedPort))
                {
                    port = parsedPort;
                }

                connectionStringBuilder.DataSource = $"{connectionStringBuilder.DataSource},{port}";
            }

            // Configure connection pool size
            {
                var connectionPoolSize = _defaultConnectionPoolSize;
                if (config.TryGetValue(SqlServerConstants.KeyName.ConnectionPoolSize, out var connectionPoolSizeEntry) &&
                    !string.IsNullOrEmpty(connectionPoolSizeEntry.ToString()) &&
                    int.TryParse(connectionPoolSizeEntry.ToString(), out var parsedConnectionPoolSize))
                {
                    connectionPoolSize = parsedConnectionPoolSize;
                }

                connectionStringBuilder.MaxPoolSize = connectionPoolSize;
            }


            return connectionStringBuilder.ToString();
        }

        public bool VerifyConnectionProperties(IReadOnlyDictionary<string, object> config, out ConnectionConfigurationError configurationError)
        {
            if (config.TryGetValue(SqlServerConstants.KeyName.PortNumber, out var portEntry) && !string.IsNullOrEmpty(portEntry.ToString()))
            {
                if (!int.TryParse(portEntry.ToString(), out _))
                {
                    configurationError = new ConnectionConfigurationError("Port number was set, but could not be read as a number");
                    return false;
                }
            }

            if (config.TryGetValue(SqlServerConstants.KeyName.ConnectionPoolSize, out var connectionPoolSizeEntry) && !string.IsNullOrEmpty(connectionPoolSizeEntry.ToString()))
            {
                if (int.TryParse(connectionPoolSizeEntry.ToString(), out var parsedPoolSize))
                {
                    if (parsedPoolSize < 1)
                    {
                        configurationError = new ConnectionConfigurationError("Connection pool size was set to a value smaller than 1");
                        return false;
                    }

                    if (parsedPoolSize > _defaultConnectionPoolSize)
                    {
                        configurationError = new ConnectionConfigurationError("Connection pool size was set to a value higher than 200");
                        return false;
                    }
                }
                else
                {
                    configurationError = new ConnectionConfigurationError("Connection pool size was set, but could not be read as a number");
                    return false;
                }
            }

            configurationError = null;
            return true;
        }

        public async Task<bool> VerifySchemaExists(SqlTransaction transaction, string schema)
        {
            // INFORMATION_SCHEMA.SCHEMATA contains all the views accessible to the current user in SQL Server.
            var schemaQuery = $"SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '{schema}'";

            var command = transaction.Connection.CreateCommand();
            command.CommandText = schemaQuery;
            command.Transaction = transaction;

            await using (var reader = await command.ExecuteReaderAsync())
            {
                return reader.HasRows;
            }
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
