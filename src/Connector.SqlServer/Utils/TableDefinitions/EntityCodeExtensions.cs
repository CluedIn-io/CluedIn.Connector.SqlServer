using System;
using System.Reflection;

using CluedIn.Core.Data;
using CluedIn.Core.Reflection;

namespace CluedIn.Connector.SqlServer.Utils.TableDefinitions;

internal static class EntityCodeExtensions
{
    public static object GetIsOriginEntityCodeDBValue(this IEntityCode code)
    {
        if (code is EntityCode)
            return DBNull.Value;

        try
        {
            var activator = ReflectionUtility.GetProperty(code.GetType(), "IsOriginEntityCode", BindingFlags.Instance | BindingFlags.Public);

            var result = (bool?)activator.Invoke(code);

            if (result == null)
                return DBNull.Value;

            return result;
        }
        catch (InvalidOperationException)
        {
            return DBNull.Value;
        }
    }
}
