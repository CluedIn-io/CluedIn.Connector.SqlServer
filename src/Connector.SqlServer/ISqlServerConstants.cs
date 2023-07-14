using CluedIn.Core.Providers;
using System;

namespace CluedIn.Connector.SqlServer
{
    public interface ISqlServerConstants : IExtendedProviderMetadata
    {
        Guid ProviderId { get; }
        IProviderMetadata CreateProviderMetadata();
    }
}
