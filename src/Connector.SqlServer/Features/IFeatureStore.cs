namespace CluedIn.Connector.SqlServer.Features
{
    public interface IFeatureStore
    {
        void SetFeature<T>(T instance);

        T GetFeature<T>();
    }
}
