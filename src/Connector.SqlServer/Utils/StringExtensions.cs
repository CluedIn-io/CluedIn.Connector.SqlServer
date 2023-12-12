using System;
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
            var sanitizedSqlName = Regex.Replace(value, @"[^_A-Za-z0-9]+", string.Empty);

            if (sanitizedSqlName.Length == 0)
            {
                throw new ArgumentException($"Input value contained only non-alphanumeric characters, or was empty: '{value}'", nameof(value));
            }

            if (char.IsDigit(sanitizedSqlName[0]))
            {
                sanitizedSqlName = $"Table{sanitizedSqlName}";
            }

            return sanitizedSqlName;
        }
    }
}
