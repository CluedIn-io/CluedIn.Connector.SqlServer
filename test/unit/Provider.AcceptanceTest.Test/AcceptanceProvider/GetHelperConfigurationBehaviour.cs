using System;
using CluedIn.Core.Crawling;
using AutoFixture.Xunit2;
using Shouldly;
using Xunit;
using CluedIn.Crawling.Acceptance.Core;
using CluedIn.Crawling.Acceptance.Test.Common;

namespace Provider.Acceptance.Test.AcceptanceProvider
{
    public class GetHelperConfigurationBehaviour : AcceptanceProviderTest
    {
        private readonly CrawlJobData _jobData;

        public GetHelperConfigurationBehaviour()
        {
            _jobData = new AcceptanceCrawlJobData(AcceptanceConfiguration.Create());
        }

        [Fact]
        public void Throws_ArgumentNullException_With_Null_CrawlJobData_Parameter()
        {
            var ex = Assert.Throws<ArgumentNullException>(
                () => Sut.GetHelperConfiguration(null, null, Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid())
                    .Wait());

            ex.ParamName
                .ShouldBe("jobData");
        }

        [Theory]
        [InlineAutoData]
        public void Returns_ValidDictionary_Instance(Guid organizationId, Guid userId, Guid providerDefinitionId)
        {
            Sut.GetHelperConfiguration(null, _jobData, organizationId, userId, providerDefinitionId)
                .Result
                .ShouldNotBeNull();
        }

        [Theory]
        [InlineAutoData("apiToken", "4fad15b1-8d51-4919-b11a-125bd9346e51")]
        public void Returns_Expected_Data(string key, object expectedValue, Guid organizationId, Guid userId, Guid providerDefinitionId)
        {
            ((AcceptanceCrawlJobData) _jobData).ApiToken = expectedValue.ToString();

            var result = Sut.GetHelperConfiguration(null, _jobData, organizationId, userId, providerDefinitionId)
                .Result;

            result
                .ContainsKey(key)
                .ShouldBeTrue(
                    $"{key} not found in results");

            result[key]
                .ShouldBe(expectedValue);
        }

    }
}
