﻿using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using CluedIn.Core.Connectors;
using Microsoft.Data.SqlClient;

namespace CluedIn.Connector.SqlServer.Connector
{
    public class SqlClient : ISqlClient
    {
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Security", "CA2100:Review SQL queries for security vulnerabilities", Justification = "<Pending>")]
        public async Task ExecuteCommandAsync(IConnectorConnection config, string commandText, IList<SqlParameter> param = null)
        {
            using (var connection = await GetConnection(config.Authentication))
            {
                var cmd = connection.CreateCommand();

                cmd.CommandText = commandText;

                if (param != null)
                    cmd.Parameters.AddRange(param.ToArray());

                await cmd.ExecuteNonQueryAsync();
            }
        }

        public string BuildConnectionString(IDictionary<string, object> config)
        {
            var cnxString = new SqlConnectionStringBuilder
            {
                Authentication = SqlAuthenticationMethod.SqlPassword,
                Password = (string)config[SqlServerConstants.KeyName.Password],
                UserID = (string)config[SqlServerConstants.KeyName.Username],
                DataSource = (string)config[SqlServerConstants.KeyName.Host],
                InitialCatalog = (string)config[SqlServerConstants.KeyName.DatabaseName],
            };

            if (config.ContainsKey(SqlServerConstants.KeyName.PortNumber))
                cnxString.DataSource = $"{cnxString.DataSource},{(int)config[SqlServerConstants.KeyName.PortNumber]}";

            return cnxString.ToString();
        }

        public async Task<SqlConnection> GetConnection(IDictionary<string, object> config)
        {
            var connectionString = BuildConnectionString(config);
            var result = new SqlConnection(connectionString);

            await result.OpenAsync();

            return result;
        }

        public async Task<DataTable> GetTables(IDictionary<string, object> config, string name = null)
        {
            using (var connection = await GetConnection(config))
            {
                DataTable result;
                if (!string.IsNullOrEmpty(name))
                {
                    var restrictions = new string[4];
                    restrictions[2] = name;

                    result = connection.GetSchema("Tables", restrictions);
                }
                else
                {
                    result = connection.GetSchema("Tables");
                }

                return result;
            }
        }

        public async Task<DataTable> GetTableColumns(IDictionary<string, object> config, string tableName)
        {
            using (var connection = await GetConnection(config))
            {

                var restrictions = new string[4];
                restrictions[2] = tableName;

                var result = connection.GetSchema("Columns", restrictions);

                return result;
            }
        }
    }
}
