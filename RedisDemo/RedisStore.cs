using StackExchange.Redis;
using System;

namespace RedisDemo
{
    public class RedisStore : IDisposable
    {
        private readonly Lazy<ConnectionMultiplexer> LazyConnection;

        public RedisStore(string connectionString)
        {
            var configurationOptions = new ConfigurationOptions
            {
                AbortOnConnectFail = false,
                EndPoints = { connectionString }
            };

            LazyConnection = new Lazy<ConnectionMultiplexer>(() => 
                ConnectionMultiplexer.Connect(configurationOptions));
        }

        private ConnectionMultiplexer Connection => LazyConnection.Value;

        public IDatabase GetDatabase()
        {
            return Connection.GetDatabase(); 
        }

        public void Dispose()
        {
            Connection.Dispose();
        }
    }
}
