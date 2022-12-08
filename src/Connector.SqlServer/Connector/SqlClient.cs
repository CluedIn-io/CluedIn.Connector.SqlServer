using CluedIn.Connector.Common.Clients;
using CluedIn.Connector.Common.Configurations;
using CluedIn.Core.Connectors;
using Microsoft.Data.SqlClient;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace CluedIn.Connector.SqlServer.Connector
{
    public class SqlClient : ClientBase<SqlConnection, SqlParameter>, ISqlClient
    {
        private readonly int _defaultPort = 1433;

        public override Task ExecuteCommandAsync(IConnectorConnection config, string commandText, IEnumerable<SqlParameter> param = null)
        {
            var d = DateTime.Now;
            var i = 0;
            while(true)
            {
                try
                {
                    return ExecuteCommandAsyncInt(config, commandText, param);
                }
                catch(Exception ex)
                {
                    if (DateTime.Now.Subtract(d).TotalHours > 4)
                        throw;

                    if (ex.ToString().Contains("deadlocked"))
                    {
                        Task.Delay(30000);
                        continue; // unlimited attempts if deadlocked
                    }

                    if (i++ > 10)
                        throw;

                    Task.Delay(30000);
                }
            }
        }

        // HACK copied from base class so we can add the CommandTimeout
        private async Task ExecuteCommandAsyncInt(IConnectorConnection config, string commandText, IEnumerable<SqlParameter> param)
        {
            await using var connection = await GetConnection(config.Authentication);

            var cmd = connection.CreateCommand();
            cmd.CommandText = commandText;
            cmd.CommandTimeout = 60 * 30;

            // use Add instead of AddRange.
            // We need to check if the object is an typeof List, Array, etc. If it is, then the process will fail because of "The CLR Type <> isn't supported" error.
            if (param != null)
                foreach (var parameter in param)
                {
                    parameter.Value ??= DBNull.Value;

                    // FIX: for Npgsql mismatches an empty string "" with System.Type
                    var parameterType = parameter.Value?.ToString() != string.Empty
                        ? parameter.Value.GetType()
                        : typeof(string);

                    // check if parameterType it's an Array or List, covert the Value to string, to be supported.
                    if (parameterType.IsArray || parameterType.IsGenericType)
                    {
                        parameter.Value = string.Join(",", ((IList)parameter.Value).Cast<string>());
                    }

                    // TODO: Further investigate, futureproof and test a proper way to hadle if the Value's object is not a List<>.
                    // If not handled in the above condition, it will fail when we add the Parameter to the command.
                    cmd.Parameters.Add(parameter);
                }

            await cmd.ExecuteNonQueryAsync();
        }

        public override async Task<SqlConnection> GetConnection(IDictionary<string, object> config)
        {
            while (true)
            {
                try
                {
                    return await base.GetConnection(config);
                }
                catch (System.InvalidOperationException ex)
                {
                    if (ex.ToString().Contains("The timeout period elapsed prior to obtaining a connection from the pool"))
                        await Task.Delay(1000);
                    else
                        throw;
                }
            }
        }

        public override string BuildConnectionString(IDictionary<string, object> config)
        {
            var connectionStringBuilder = new SqlConnectionStringBuilder
            {
                Authentication = SqlAuthenticationMethod.SqlPassword,
                Password = (string)config[SqlServerConstants.KeyName.Password],
                UserID = (string)config[SqlServerConstants.KeyName.Username],
                DataSource = (string)config[SqlServerConstants.KeyName.Host],
                InitialCatalog = (string)config[SqlServerConstants.KeyName.DatabaseName],
                Pooling = true
            };

            if (config.TryGetValue(SqlServerConstants.KeyName.PortNumber, out var portEntry) && int.TryParse(portEntry.ToString(), out var port))
                connectionStringBuilder.DataSource = $"{connectionStringBuilder.DataSource},{port}";
            else
                connectionStringBuilder.DataSource = $"{connectionStringBuilder.DataSource},{_defaultPort}";

            return connectionStringBuilder.ToString();
        }
    }
}
