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
                sanitizedSqlName = $"_{sanitizedSqlName}";
            }

            if (sanitizedSqlName.Length > 127) // 127 instead of 128, since we need space for parameter declaration '@'
            {
                var hashValue = Math.Abs(GetStableHashCode(value)).ToString().PadLeft(10, '0');
                var postFix = $"_{hashValue}";
                sanitizedSqlName = $"{sanitizedSqlName.Remove(127 - postFix.Length)}{postFix}";
            }

            return sanitizedSqlName;
        }

        // # .NET implementation of string hash code copied from here:
        // https://stackoverflow.com/a/36845864/2009373
        private static int GetStableHashCode(string str)
        {
            unchecked
            {
                int hash1 = 5381;
                int hash2 = hash1;

                for (int i = 0; i < str.Length && str[i] != '\0'; i += 2)
                {
                    hash1 = ((hash1 << 5) + hash1) ^ str[i];
                    if (i == str.Length - 1 || str[i + 1] == '\0')
                        break;
                    hash2 = ((hash2 << 5) + hash2) ^ str[i + 1];
                }

                return hash1 + (hash2 * 1566083941);
            }
        }
    }
}
