using CluedIn.Core.Connectors;
using CluedIn.Core.Data;
using CluedIn.Core.Data.Parts;
using CluedIn.Core.Streams.Models;
using System;
using System.Collections.Generic;

namespace CluedIn.Connector.SqlServer.Utils
{
    internal class SqlConnectorEntityData : IReadOnlyConnectorEntityData
    {
        private readonly IReadOnlyConnectorEntityData _inner;

        public VersionChangeType ChangeType => _inner.ChangeType;
        public StreamMode StreamMode => _inner.StreamMode;
        public Guid EntityId => _inner.EntityId;
        public ConnectorEntityPersistInfo PersistInfo => _inner.PersistInfo;
        public ConnectorEntityPersistInfo PreviousPersistInfo => _inner.PreviousPersistInfo;
        public IEntityCode OriginEntityCode => _inner.OriginEntityCode;
        public EntityType EntityType => _inner.EntityType;
        public IReadOnlyCollection<ConnectorPropertyData> Properties => _inner.Properties;
        public IReadOnlyCollection<IEntityCode> EntityCodes => _inner.EntityCodes;
        public IReadOnlyCollection<EntityEdge> IncomingEdges => _inner.IncomingEdges;
        public IReadOnlyCollection<EntityEdge> OutgoingEdges => _inner.OutgoingEdges;

        public Guid? CorrelationId;

        public DateTimeOffset Timestamp;

        public SqlConnectorEntityData(IReadOnlyConnectorEntityData inner, Guid? correlationId, DateTimeOffset timestamp)
        {
            _inner = inner;
            CorrelationId = correlationId;
            Timestamp = timestamp;
        }
    }
}
