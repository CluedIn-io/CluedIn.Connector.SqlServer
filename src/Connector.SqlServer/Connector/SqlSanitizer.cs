namespace CluedIn.Connector.SqlServer.Connector
{
    public static class SqlSanitizer
    {
        public static string Sanitize(string str)
        {
            return str.Replace("--", "").Replace(";", "").Replace("'", "");       // Bare-bones sanitization to prevent Sql Injection. Extra info here http://sommarskog.se/dynamic_sql.html
        }
    }
}
