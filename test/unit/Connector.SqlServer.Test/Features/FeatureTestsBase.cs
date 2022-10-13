using CluedIn.Connector.SqlServer.Utility;
using Microsoft.Extensions.Logging;
using Moq;
using System;
using System.Collections.Generic;
using System.Text;

namespace CluedIn.Connector.SqlServer.Unit.Tests.Features
{
    public abstract class FeatureTestsBase
    {
        protected readonly TestContext _testContext;
        protected readonly Mock<ILogger> _logger;

        public FeatureTestsBase()
        {
            _logger = new Mock<ILogger>();
            _testContext = new TestContext();
        }

        protected SanitizedSqlName ToSanitized(string source) => new SanitizedSqlName(source);
    }
}
