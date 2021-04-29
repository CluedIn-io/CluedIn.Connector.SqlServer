using System.Collections.Generic;
using Microsoft.Data.SqlClient;

namespace CluedIn.Connector.SqlServer.Connector
{
    public class SqlServerConnectorCommand
    {
        public string Text { get; set; }
        public IEnumerable<SqlParameter> Parameters { get; set; }
    }
}
