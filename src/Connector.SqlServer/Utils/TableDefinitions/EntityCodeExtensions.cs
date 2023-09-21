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

            return (bool)activator.Invoke(code);
        }
        catch (InvalidOperationException)
        {
            return DBNull.Value;
        }
    }
}
