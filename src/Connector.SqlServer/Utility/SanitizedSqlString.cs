using CluedIn.Connector.Common.Helpers;

namespace CluedIn.Connector.SqlServer.Utility
{
    public class SanitizedSqlString
    {
        protected readonly string Source;
        protected string Sanitized;

        public SanitizedSqlString(string source)
        {
            Source = source;
        }

        public virtual string GetValue()
        {
            if (Sanitized == null && Source != null)
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

            return Sanitized == target.GetValue();
        }

        public override int GetHashCode()
        {
            return Sanitized.GetHashCode();
        }
    }
}
