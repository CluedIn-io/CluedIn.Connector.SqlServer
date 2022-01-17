using CluedIn.Core;
using CluedIn.Core.Accounts;
using System;

namespace CluedIn.Connector.SqlServer.Unit.Tests
{
    public class TestOrganization : Organization
    {
        public TestOrganization(ApplicationContext context, Guid id)
            : base(context, id)
        {
        }
    }
}
