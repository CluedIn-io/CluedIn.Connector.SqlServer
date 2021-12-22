namespace CluedIn.Connector.SqlServer
{
    public static class StringExtensions
    {
        public static string SqlSanitize(this string str)
        {
            // Bare-bones sanitization to prevent Sql Injection. Extra info here http://sommarskog.se/dynamic_sql.html
            return
                str
                    .Replace("--", "")
                    .Replace(";", "")
                    .Replace("'", "");
        }
    }
}
