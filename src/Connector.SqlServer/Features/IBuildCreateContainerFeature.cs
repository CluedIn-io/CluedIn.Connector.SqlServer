using System.Collections.Generic;
using CluedIn.Core.Connectors;

namespace CluedIn.Connector.SqlServer.Features
{
    public interface IBuildCreateContainerFeature
    {
        string BuildCreateContainerSql(string containerName, IEnumerable<ConnectionDataType> columns);
    }
}
