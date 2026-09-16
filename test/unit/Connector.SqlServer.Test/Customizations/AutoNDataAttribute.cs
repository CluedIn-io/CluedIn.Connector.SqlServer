using AutoFixture;
using AutoFixture.AutoNSubstitute;
using System;

namespace CluedIn.Connector.SqlServer.Unit.Tests.Customizations
{
    public sealed class AutoNDataAttribute : AutoDataAttribute
    {
        public AutoNDataAttribute()
            : base(() =>
            {
                var fixture = new Fixture();
                fixture.Customize(new AutoNSubstituteCustomization { ConfigureMembers = true });
                return fixture;
            })
        {
        }
    }
}
