using System;
using System.Net;
using Castle.DynamicProxy;
using Castle.MicroKernel.Registration;
using Castle.Windsor;
using CluedIn.Core;
using CluedIn.Core.Accounts;
using CluedIn.Core.Caching;
using CluedIn.Core.Data;
using CluedIn.Core.Data.Relational;
using CluedIn.Core.DataStore;
using CluedIn.Core.DataStore.Entities;
using CluedIn.Core.Messages.Processing;
using CluedIn.Core.Net.Mail;
using CluedIn.Core.Processing;
using CluedIn.Core.Processing.Statistics;
using CluedIn.Core.Rules;
using CluedIn.Core.Server;
using CluedIn.Core.Workflows;
using CluedIn.DataStore.Relational;
using CluedIn.ExternalSearch;
using EasyNetQ;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;

namespace CluedIn.Connector.SqlServer.Unit.Tests
{
    public class TestContext : IDisposable
    {
        public WindsorContainer Container;

        public Mock<IServer> Server;
        public Mock<IBus> Bus;
        public Mock<ISystemConnectionStrings> SystemConnectionStrings;
        public Mock<ISystemDataShards> SystemDataShards;
        public Mock<SystemContext> SystemContext;
        public Mock<ApplicationContext> AppContext;

        public Mock<IPrimaryEntityDataStore<Entity>> PrimaryEntityDataStore;
        public Mock<IGraphEntityDataStore<Entity>> GraphEntityDataStore;
        public Mock<IBlobEntityDataStore<Entity>> BlobEntityDataStore;
        public Mock<IAgentDataStoreImpl> AgentDataStoreMock;

        public IAgentDataStore AgentDataStore;

        public Mock<IOrganizationRepository> OrganizationRepository;

        public Mock<IClueProcessingPipeline> ClueProcessingPipeline;
        public Mock<IClueToEntityMappingProcessor> ClueToEntityMappingProcessor;

        public Mock<ISystemVocabularies> SystemVocabularies;

        public Mock<IExternalSearchProvidersRepository> ExternalSearchProvidersRepository;
        public Mock<IExternalSearchRepository> ExternalSearchRepository;
        public Mock<IRuleRepository> RuleRepository;
        public Mock<IRuleEngine> RuleEngine;

        public Mock<ISystemProcessingHub> ProcessingHub;
        public Mock<IProcessingStatistics> ProcessingStatistics;

        public Mock<WorkflowRepository> WorkflowRepository;
        public Mock<InMemoryApplicationCache> ApplicationCache;

        public Mock<IMailTemplates> MailTemplates;

        public Func<ExecutionContext, Guid, IOrganization> OrganizationFactory;

        public ILogger Logger;

        private ExecutionContext context;

        public ExecutionContext Context
        {
            get
            {
                if (context == null)
                {
                    var o1 = Container.Resolve<ApplicationContext>();
                    context = new ExecutionContext(o1, new TestOrganization(o1, Constants.SystemOrganizationId), Logger);
                }

                return context;
            }
        }

        public TestContext() : this(Mock.Of<ILogger>())
        {

        }

        public TestContext([NotNull] ILogger logger)
        {
            ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;

            Container = new WindsorContainer();

            SystemConnectionStrings = new Mock<SystemConnectionStrings>(MockBehavior.Loose).As<ISystemConnectionStrings>();
            SystemDataShards = new Mock<ISystemDataShards>(MockBehavior.Loose).As<ISystemDataShards>();
            SystemContext = new Mock<SystemContext>(MockBehavior.Loose, Container);
            AppContext = new Mock<ApplicationContext>(MockBehavior.Loose, (IWindsorContainer)Container);
            Server = new Mock<IServer>(MockBehavior.Loose);
            Bus = new Mock<IBus>(MockBehavior.Loose);

            Logger = logger ?? throw new ArgumentNullException(nameof(logger));

            PrimaryEntityDataStore = new Mock<IPrimaryEntityDataStore<Entity>>(MockBehavior.Loose);
            GraphEntityDataStore = new Mock<IGraphEntityDataStore<Entity>>(MockBehavior.Loose);
            BlobEntityDataStore = new Mock<IBlobEntityDataStore<Entity>>(MockBehavior.Loose);
            AgentDataStoreMock = new Mock<IAgentDataStoreImpl>();

            OrganizationRepository = new Mock<IOrganizationRepository>(MockBehavior.Loose).As<IOrganizationRepository>();

            SystemVocabularies = new Mock<SystemVocabularies>(MockBehavior.Loose, AppContext.Object).As<ISystemVocabularies>();
            ExternalSearchRepository = new Mock<IExternalSearchRepository>(MockBehavior.Loose).As<IExternalSearchRepository>();
            RuleRepository = new Mock<IRuleRepository>(MockBehavior.Loose).As<IRuleRepository>();
            RuleEngine = new Mock<IRuleEngine>(MockBehavior.Loose).As<IRuleEngine>();

            ProcessingHub = new Mock<ISystemProcessingHub>();
            ProcessingStatistics = new Mock<IProcessingStatistics>();
            MailTemplates = new Mock<IMailTemplates>();
            WorkflowRepository = new Mock<WorkflowRepository>(MockBehavior.Loose, AppContext.Object);
            ApplicationCache = new Mock<InMemoryApplicationCache>(MockBehavior.Loose, Container);

            SystemConnectionStrings.CallBase = true;
            SystemDataShards.CallBase = true;
            SystemContext.CallBase = true;
            AppContext.CallBase = true;
            ClueProcessingPipeline.CallBase = true;
            ClueToEntityMappingProcessor.CallBase = true;
            OrganizationRepository.CallBase = true;
            SystemVocabularies.CallBase = true;
            ExternalSearchProvidersRepository.CallBase = true;
            WorkflowRepository.CallBase = true;
            ApplicationCache.CallBase = true;

            //this.AppContext = new ApplicationContext(this.Container);
            //this.SystemContext = new SystemContext(this.Container);

            var proxyGenerator = new ProxyGenerator();

            // Container Registration
            var options = new DbContextOptionsBuilder<DbContext>()
                .UseInMemoryDatabase(databaseName: "InMemoryDatabase")
                .Options;

            Container.Register(Component.For<DbContextOptions<DbContext>>().UsingFactoryMethod(() => options));

            Container.Register(
                Component.For<CluedInEntities>()
                    .UsingFactoryMethod(() => new CluedInEntities(options))
                    .OnlyNewServices());
            Container.Register(Component.For<IRelationalDataStore<Rule>>()
                .Forward<ISimpleDataStore<Rule>>()
                .Forward<IDataStore<Rule>>()
                .Forward<IDataStore>()
                .UsingFactoryMethod(() => new RuleDataStore(Container.Resolve<ApplicationContext>()))
                .LifestyleTransient());
            Container.Register(Component.For<IRelationalDataStore<CluedInStream>>()
                .Forward<ISimpleDataStore<CluedInStream>>()
                .Forward<IDataStore<CluedInStream>>()
                .Forward<IDataStore>()
                .UsingFactoryMethod(() => new StreamDataStore(Container.Resolve<ApplicationContext>()))
                .LifestyleTransient());
            Container.Register(Component.For<IRelationalDataStore<Notification>>()
                .Forward<ISimpleDataStore<Notification>>()
                .Forward<IDataStore<Notification>>()
                .Forward<IDataStore>()
                .UsingFactoryMethod(() => new NotificationDataStore(Container.Resolve<ApplicationContext>()))
                .LifestyleTransient());

            Container.Register(Component.For<ApplicationContext>().UsingFactoryMethod(() => AppContext.Object));
            Container.Register(Component.For<ISystemConnectionStrings>().UsingFactoryMethod(() => SystemConnectionStrings.Object));
            Container.Register(Component.For<ISystemDataShards>().UsingFactoryMethod(() => proxyGenerator.CreateInterfaceProxyWithTarget(SystemDataShards.Object)));
            Container.Register(Component.For<SystemContext>().UsingFactoryMethod(() => SystemContext.Object));
            //this.Container.Register(Component.For<ILogger>().LifeStyle.Singleton.UsingFactoryMethod(() => this.Logger));
            Container.Register(Component.For<ILoggerFactory>().UsingFactoryMethod(() => new NullLoggerFactory()).LifestyleSingleton());
            Container.Register(Component.For(typeof(ILogger<>)).ImplementedBy(typeof(NullLogger<>)).LifestyleSingleton());

            Container.Register(Component.For<IPrimaryEntityDataStore<Entity>>().UsingFactoryMethod(() => proxyGenerator.CreateInterfaceProxyWithTarget(PrimaryEntityDataStore.Object)));
            Container.Register(Component.For<IGraphEntityDataStore<Entity>>().UsingFactoryMethod(() => proxyGenerator.CreateInterfaceProxyWithTarget(GraphEntityDataStore.Object)));
            Container.Register(Component.For<IBlobEntityDataStore<Entity>>().UsingFactoryMethod(() => proxyGenerator.CreateInterfaceProxyWithTarget(BlobEntityDataStore.Object)));
            Container.Register(Component.For<IClueProcessingPipeline>().UsingFactoryMethod(() => ClueProcessingPipeline.Object));
            Container.Register(Component.For<IClueToEntityMappingProcessor>().UsingFactoryMethod(() => ClueToEntityMappingProcessor.Object));
            Container.Register(Component.For<IOrganizationRepository>().UsingFactoryMethod(() => proxyGenerator.CreateInterfaceProxyWithTarget(OrganizationRepository.Object)));
            Container.Register(Component.For<IServer>().UsingFactoryMethod(() => proxyGenerator.CreateInterfaceProxyWithTarget(Server.Object)));
            Container.Register(Component.For<IBus>().UsingFactoryMethod(() => proxyGenerator.CreateInterfaceProxyWithTarget(Bus.Object)));
            Container.Register(Component.For<IAgentDataStore>().UsingFactoryMethod(() => AgentDataStore));
            Container.Register(Component.For<ISystemVocabularies>().UsingFactoryMethod(() => SystemVocabularies.Object));
            Container.Register(Component.For<IExternalSearchProvidersRepository>().UsingFactoryMethod(() => ExternalSearchProvidersRepository.Object));
            Container.Register(Component.For<IExternalSearchRepository>().UsingFactoryMethod(() => proxyGenerator.CreateInterfaceProxyWithTarget(ExternalSearchRepository.Object)));
            Container.Register(Component.For<IRuleRepository>().UsingFactoryMethod(() => proxyGenerator.CreateInterfaceProxyWithTarget(RuleRepository.Object)));
            Container.Register(Component.For<IRuleEngine>().UsingFactoryMethod(() => proxyGenerator.CreateInterfaceProxyWithTarget(RuleEngine.Object)));
            Container.Register(Component.For<ISystemProcessingHub>().UsingFactoryMethod(() => proxyGenerator.CreateInterfaceProxyWithTarget(ProcessingHub.Object)));
            Container.Register(Component.For<IProcessingStatistics>().UsingFactoryMethod(() => proxyGenerator.CreateInterfaceProxyWithTarget(ProcessingStatistics.Object)));
            Container.Register(Component.For<WorkflowRepository>().UsingFactoryMethod(() => WorkflowRepository.Object));
            Container.Register(Component.For<IApplicationCache>().UsingFactoryMethod(() => ApplicationCache.Object));
            Container.Register(Component.For<IMailTemplates>().UsingFactoryMethod(() => proxyGenerator.CreateInterfaceProxyWithTarget(MailTemplates.Object)));


            // Setup
            Server.Setup(s => s.ApplicationContext).Returns(() => Container.Resolve<ApplicationContext>());
            Bus.Setup(s => s.IsConnected).Returns(false);
            Bus.Setup(s => s.Advanced.IsConnected).Returns(false);

            ProcessingHub.Setup(h => h.SendCommand(It.IsAny<IProcessingCommand>()));

            OrganizationFactory = (ctx, id) => new TestOrganization(ctx.ApplicationContext, id);

            var o1 = Container.Resolve<ApplicationContext>();

            OrganizationRepository.Setup(r => r.GetOrganization(It.IsAny<ExecutionContext>(), new TestOrganization(o1, Constants.SystemOrganizationId).Id)).Returns<ExecutionContext, Guid>((c, i) => OrganizationFactory(c, i));

            OrganizationRepository.Setup(r => r.GetOrganization(It.IsAny<ExecutionContext>(), It.IsAny<Guid>())).Returns<ExecutionContext, Guid>((c, i) => OrganizationFactory(c, i));
        }

        public void Dispose()
        {
            Container?.Dispose();
            Context?.Dispose();
        }
    }
}

