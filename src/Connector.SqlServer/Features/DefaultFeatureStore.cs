using System;
using System.Collections.Concurrent;

namespace CluedIn.Connector.SqlServer.Features
{
    public class DefaultFeatureStore : IFeatureStore
    {
        private readonly ConcurrentDictionary<Type, object> _store = new ConcurrentDictionary<Type, object>
        {
            [typeof(IBuildStoreDataFeature)] = new DefaultBuildStoreDataFeature(),
            [typeof(IBuildCreateContainerFeature)] = new DefaultBuildCreateContainerFeature(),
            [typeof(IBuildCreateIndexFeature)] = new DefaultBuildCreateIndexFeature()
        };

        public T GetFeature<T>() => _store.TryGetValue(typeof(T), out var result) ? (T)result : default;        

        public void SetFeature<T>(T instance) => _store.AddOrUpdate(typeof(T), instance, (_, __) => instance);        
    }
}
