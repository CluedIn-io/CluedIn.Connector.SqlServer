namespace CluedIn.Connector.SqlServer
{
    public static class StringExtensions
    {
        public static string SqlSanitize(this string str)
        {
            return str.Replace("--", "").Replace(";", "").Replace("'", "");       // Bare-bones sanitization to prevent Sql Injection. Extra info here http://sommarskog.se/dynamic_sql.html
        }
    }
}
