using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Shuttle.Core.Contract;
using Shuttle.Core.Pipelines.Logging;
using Shuttle.Hopper.Logging;

namespace Shuttle.Hopper.Testing;

public static class ServiceCollectionExtensions
{
    extension(IServiceCollection services)
    {
        public IServiceCollection AddTransientQueues()
        {
            Guard.AgainstNull(services);

            services.TryAddSingleton<ITransportFactory, TransientQueueFactory>();

            return services;
        }

        public IServiceCollection AddTransientStreams()
        {
            Guard.AgainstNull(services);

            services.TryAddSingleton<ITransportFactory, TransientStreamFactory>();

            return services;
        }

        public IServiceCollection ConfigureLogging(string test)
        {
            Guard.AgainstNull(services);

            services.TryAddEnumerable(ServiceDescriptor.Singleton<ILoggerProvider>(new FixtureFileLoggerProvider(Guard.AgainstEmpty(test))));
            services.TryAddEnumerable(ServiceDescriptor.Singleton<ILoggerProvider, ConsoleLoggerProvider>());

            services
                .AddTransportEventLogging()
                .AddPipelineLogging()
                .AddLogging(builder =>
                {
                    builder.SetMinimumLevel(LogLevel.Trace);
                });

            return services;
        }
    }
}