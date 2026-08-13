using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Shuttle.Contract;
using Shuttle.Pipelines;

namespace Shuttle.Hopper.Testing;

public static class ServiceProviderExtensions
{
    extension(IServiceProvider serviceProvider)
    {
        public ITransportService CreateTransportService()
        {
            Guard.AgainstNull(serviceProvider);

            return new TransportService(
                serviceProvider.GetRequiredService<IOptions<HopperOptions>>(),
                serviceProvider.GetRequiredService<ITransportFactoryService>(),
                serviceProvider.GetRequiredService<IUriResolver>()
            );
        }

        /// <summary>
        /// Creates a throwaway pipeline for use when calling `ITransport` methods directly in test code, outside of an actual pipeline execution.
        /// </summary>
        public IPipeline CreatePipeline()
        {
            Guard.AgainstNull(serviceProvider);

            return new Pipeline(serviceProvider.GetRequiredService<IOptions<PipelineOptions>>(), serviceProvider);
        }

        public ILogger<T> GetLogger<T>()
        {
            return Guard.AgainstNull(serviceProvider).GetService<ILoggerFactory>()?.CreateLogger<T>() ?? NullLogger<T>.Instance;
        }

        public ILogger GetLogger()
        {
            return Guard.AgainstNull(serviceProvider).GetService<ILoggerFactory>()?.CreateLogger("Fixture") ?? NullLogger.Instance;
        }

        public async Task<IServiceProvider> StartHostedServicesAsync()
        {
            Guard.AgainstNull(serviceProvider);

            var logger = serviceProvider.GetLogger();

            logger.LogInformation("[StartHostedServices]");

            foreach (var hostedService in serviceProvider.GetServices<IHostedService>())
            {
                logger.LogInformation($"[HostedService-starting] : {hostedService.GetType().Name}");

                await hostedService.StartAsync(CancellationToken.None).ConfigureAwait(false);

                logger.LogInformation($"[HostedService-started] : {hostedService.GetType().Name}");
            }

            return serviceProvider;
        }

        public async Task<IServiceProvider> StopHostedServicesAsync()
        {
            Guard.AgainstNull(serviceProvider);

            var logger = serviceProvider.GetLogger();

            logger.LogInformation("[StopHostedServices]");

            foreach (var hostedService in serviceProvider.GetServices<IHostedService>())
            {
                logger.LogInformation($"[HostedService-stopping] : {hostedService.GetType().Name}");

                await hostedService.StopAsync(CancellationToken.None).ConfigureAwait(false);

                logger.LogInformation($"[HostedService-stopped] : {hostedService.GetType().Name}");
            }

            return serviceProvider;
        }
    }
}