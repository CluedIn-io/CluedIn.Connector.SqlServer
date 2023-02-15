using CluedIn.Connector.Common.Helpers;

namespace CluedIn.Connector.SqlServer.Utils
{
    internal static class StringExtensions
    {
        /// <summary>
        /// Sanitize potentially unsafe value to be a valid SQL name
        /// </summary>
        public static string ToSanitizedSqlName(this string value) => SqlStringSanitizer.Sanitize(value);
    }
}
