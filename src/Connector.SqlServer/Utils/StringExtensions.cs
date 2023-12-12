using System.Text.RegularExpressions;

namespace CluedIn.Connector.SqlServer.Utils
{
    internal static class StringExtensions
    {
        /// <summary>
        /// Sanitize potentially unsafe value to be a valid SQL name
        /// </summary>
        public static string ToSanitizedSqlName(this string value)
        {
            var withoutSpecialCharacters = Regex.Replace(value, @"[^_A-Za-z0-9]+", string.Empty);

            if (Regex.IsMatch(withoutSpecialCharacters, @"^\d.*"))
            {
                return $"Table{withoutSpecialCharacters}";
            }

            return withoutSpecialCharacters;
        }
    }
}
