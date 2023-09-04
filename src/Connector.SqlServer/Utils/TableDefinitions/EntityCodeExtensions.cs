using System;

namespace CluedIn.Connector.SqlServer.Utils.TableDefinitions;

using CluedIn.Core.Data;
using CluedIn.Core.Reflection;

using System.Reflection;

internal static class EntityCodeExtensions
{
    public static bool IsOriginEntityCode(this IEntityCode code)
    {
        if (code is EntityCode)
            return false;

        try
        {
            var activator = ReflectionUtility.GetProperty(code.GetType(), "IsOriginEntityCode", BindingFlags.Instance | BindingFlags.Public);

            return (bool)activator.Invoke(code);
        }
        catch (InvalidOperationException)
        {
            return false;
        }
    }
}
