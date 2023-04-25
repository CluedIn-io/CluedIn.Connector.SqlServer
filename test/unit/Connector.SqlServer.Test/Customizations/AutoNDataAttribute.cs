using AutoFixture;
using AutoFixture.Xunit2;
using AutoFixture.AutoNSubstitute;
using System;
using System.Collections.Generic;
using System.Reflection;
using Xunit.Sdk;

namespace CluedIn.Connector.SqlServer.Unit.Tests.Customizations
{
    public sealed class AutoNDataAttribute : DataAttribute
    {
        public override IEnumerable<object[]> GetData(MethodInfo testMethod) => new CustomizedAutoData(CustomizeFixture).GetData(testMethod);

        private void CustomizeFixture(IFixture fixture)
        {
            fixture.Customize(new AutoNSubstituteCustomization { ConfigureMembers = true });
        }

        private class CustomizedAutoData : AutoDataAttribute
        {
            public CustomizedAutoData(Action<IFixture> customizeFixture)
                : base(() =>
                {
                    var fixture = new Fixture();

                    customizeFixture.Invoke(fixture);

                    return fixture;
                })
            {
            }
        }
    }
}
