using CluedIn.Core.Crawling;
using System.Collections.Generic;

namespace CluedIn.Connector.SqlServer;

public class CrawlJobDataWrapper : CrawlJobData
{
    public CrawlJobDataWrapper(IDictionary<string, object> configurations)
    {
        Configurations = configurations;
    }

    public IDictionary<string, object> Configurations { get; }
}
