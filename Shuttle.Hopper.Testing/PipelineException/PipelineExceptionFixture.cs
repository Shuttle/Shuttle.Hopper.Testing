using Microsoft.Extensions.DependencyInjection;
using NUnit.Framework;
using Shuttle.Core.Pipelines;
using Shuttle.Core.Reflection;
using Shuttle.Core.Serialization;

namespace Shuttle.Hopper.Testing;

public class PipelineExceptionFixture : IntegrationFixture
{
    protected async Task TestExceptionHandlingAsync(IServiceCollection services, string transportUriFormat)
    {
        var serviceBusOptions = new ServiceBusOptions
        {
            Inbox = new()
            {
                WorkTransportUri = new(string.Format(transportUriFormat, "test-inbox-work")),
                DurationToSleepWhenIdle = [TimeSpan.FromMilliseconds(5)],
                DurationToIgnoreOnFailure = [TimeSpan.FromMilliseconds(5)],
                MaximumFailureCount = 100,
                ThreadCount = 1
            }
        };

        services.AddServiceBus(builder =>
        {
            builder.Options = serviceBusOptions;
            builder.SuppressHostedService();
        });

        services.ConfigureLogging(nameof(PipelineExceptionFixture));

        services.AddSingleton<ReceivePipelineExceptionFeature>();

        var serviceProvider = await services.BuildServiceProvider().StartHostedServicesAsync().ConfigureAwait(false);

        var serviceBus = serviceProvider.GetRequiredService<IServiceBus>();
        var serviceBusConfiguration = serviceProvider.GetRequiredService<IServiceBusConfiguration>();

        await serviceBusConfiguration.ConfigureAsync();

        var inboxWorkTransport = serviceBusConfiguration.Inbox!.WorkTransport!;

        if (serviceBusConfiguration.Inbox!.WorkTransport is IDeleteTransport delete)
        {
            await delete.DeleteAsync().ConfigureAwait(false);
        }
        else
        {
            await inboxWorkTransport.TryPurgeAsync().ConfigureAwait(false);
        }

        await serviceBusConfiguration.CreatePhysicalTransportsAsync().ConfigureAwait(false);

        var pipelineFactory = serviceProvider.GetRequiredService<IPipelineFactory>();
        var transportMessagePipeline = await pipelineFactory.GetPipelineAsync<TransportMessagePipeline>();
        var feature = serviceProvider.GetRequiredService<ReceivePipelineExceptionFeature>();
        var serializer = serviceProvider.GetRequiredService<ISerializer>();

        try
        {
            await transportMessagePipeline.ExecuteAsync(new ReceivePipelineCommand(), null, builder =>
            {
                builder.WithRecipient(inboxWorkTransport);
            }).ConfigureAwait(false);

            var transportMessage = transportMessagePipeline.State.GetTransportMessage()!;

            await inboxWorkTransport.SendAsync(transportMessage, await serializer.SerializeAsync(transportMessage).ConfigureAwait(false)).ConfigureAwait(false);

            await serviceBus.StartAsync().ConfigureAwait(false);

            var timeout = DateTime.Now.AddSeconds(2);
            var timedOut = false;

            while (feature.ShouldWait() && !timedOut)
            {
                await Task.Delay(10).ConfigureAwait(false);
                timedOut = DateTime.Now > timeout;
            }

            Assert.That(!timedOut, "Timed out before message was received.");
        }
        finally
        {
            await serviceBus.TryDisposeAsync().ConfigureAwait(false);
        }

        await serviceProvider.StopHostedServicesAsync().ConfigureAwait(false);
    }
}