using System;
using System.Collections.Generic;
using System.Text;

namespace CluedIn.Connector.SqlServer.Features
{
    internal interface IAddCustomTypesFeature
    {
        string GetCreateCustomTypesCommandText();
    }
}
