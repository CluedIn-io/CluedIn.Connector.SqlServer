using System;
using CluedIn.Core;
using CluedIn.Core.Accounts;

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
