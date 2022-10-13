using CluedIn.Connector.Common.Helpers;
using System;

namespace CluedIn.Connector.SqlServer.Utility
{
    public class SanitizedSqlString
    {
        protected readonly string Source;
        protected string Sanitized;

        /// <summary>
        /// String sanitized for use in SQL code
        /// </summary>
        /// <param name="source"></param>
        /// <exception cref="ArgumentNullException">Source string can't be null</exception>
        public SanitizedSqlString(string source)
        {
            Source = source ?? throw new ArgumentNullException($"{nameof(source)} can't be null");
        }

        public virtual string GetValue()
        {
            if (Sanitized == null)
                Sanitized = SqlStringSanitizer.Sanitize(Source);

            return Sanitized;
        }

        public override string ToString()
        {
            return GetValue();
        }

        public override bool Equals(object obj)
        {
            if (obj == null || obj is SanitizedSqlString target == false)
                return false;

            return GetValue() == target.GetValue();
        }

        public override int GetHashCode()
        {
            return GetValue().GetHashCode();
        }
    }
}
