using Microsoft.Extensions.DependencyInjection;
using NUnit.Framework;
using Shuttle.Serialization;

namespace Shuttle.Hopper.Testing;

public class PipelineExceptionFixture : IntegrationFixture
{
    protected async Task TestExceptionHandlingAsync(IServiceCollection services, string transportUriFormat)
    {
        services.AddHopper(options =>
        {
            options.Inbox = new()
            {
                WorkTransportUri = new(string.Format(transportUriFormat, "test-inbox-work")),
                IdleDurations = [TimeSpan.FromMilliseconds(5)],
                IgnoreOnFailureDurations = [TimeSpan.FromMilliseconds(5)],
                MaximumFailureCount = 100,
                ThreadCount = 1
            };

            options.AutoStart = false;
        });

        services.AddSingleton<ReceivePipelineExceptionFeature>();

        var serviceProvider = await services.BuildServiceProvider().StartHostedServicesAsync().ConfigureAwait(false);

        var busControl = serviceProvider.GetRequiredService<IBusControl>();
        var busConfiguration = serviceProvider.GetRequiredService<IBusConfiguration>();

        await busConfiguration.ConfigureAsync();

        var inboxWorkTransport = busConfiguration.Inbox!.WorkTransport!;

        if (busConfiguration.Inbox!.WorkTransport is IDeleteTransport delete)
        {
            await delete.DeleteAsync().ConfigureAwait(false);
        }
        else
        {
            await inboxWorkTransport.TryPurgeAsync().ConfigureAwait(false);
        }

        await busConfiguration.CreatePhysicalTransportsAsync().ConfigureAwait(false);

        var transportMessagePipeline = serviceProvider.GetRequiredService<ITransportMessagePipeline>();
        var feature = serviceProvider.GetRequiredService<ReceivePipelineExceptionFeature>();
        var serializer = serviceProvider.GetRequiredService<ISerializer>();

        try
        {
            await transportMessagePipeline.ExecuteAsync(new ReceivePipelineCommand(), builder =>
            {
                builder.WithRecipient(inboxWorkTransport);
            }).ConfigureAwait(false);

            var transportMessage = transportMessagePipeline.State.GetTransportMessage()!;

            await inboxWorkTransport.SendAsync(await serializer.SerializeAsync(transportMessage).ConfigureAwait(false), transportMessagePipeline).ConfigureAwait(false);

            await busControl.StartAsync().ConfigureAwait(false);

            var timeout = DateTimeOffset.UtcNow.AddSeconds(200);
            var timedOut = false;

            while (feature.ShouldWait() && !timedOut)
            {
                await Task.Delay(10).ConfigureAwait(false);
                timedOut = DateTimeOffset.UtcNow > timeout;
            }

            Assert.That(!timedOut, "Timed out before message was received.");
        }
        finally
        {
            await busControl.DisposeAsync().ConfigureAwait(false);
        }

        await serviceProvider.StopHostedServicesAsync().ConfigureAwait(false);
    }
}