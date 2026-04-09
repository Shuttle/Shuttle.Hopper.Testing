using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NUnit.Framework;
using Shuttle.Contract;
using Shuttle.Reflection;

namespace Shuttle.Hopper.Testing;

public class DeferredFixture : IntegrationFixture
{
    private void ConfigureServices(IServiceCollection services, int threadCount, string transportUriFormat)
    {
        Guard.AgainstNull(services);

        services.AddHopper(options =>
        {
            options.Inbox = new()
            {
                WorkTransportUri = new(string.Format(transportUriFormat, "test-inbox-work")),
                DeferredTransportUri = new(string.Format(transportUriFormat, "test-inbox-deferred")),
                ErrorTransportUri = new(string.Format(transportUriFormat, "test-error")),
                IdleDurations = [TimeSpan.FromMilliseconds(25)],
                IgnoreOnFailureDurations = [TimeSpan.FromMilliseconds(25)],
                ThreadCount = threadCount,
                DeferredMessageProcessorResetInterval = TimeSpan.FromMilliseconds(25),
                DeferredMessageProcessorIdleDuration = TimeSpan.FromMilliseconds(25)
            };

            options.AutoStart = false;
        })
        .AddMessageHandlersFrom(Assembly.GetExecutingAssembly());
    }

    private async Task ConfigureTransportsAsync(ITransportService transportService, string transportUriFormat)
    {
        var workTransport = await transportService.GetAsync(string.Format(transportUriFormat, "test-inbox-work"));
        var deferredTransport = await transportService.GetAsync(string.Format(transportUriFormat, "test-inbox-deferred"));
        var errorTransport = await transportService.GetAsync(string.Format(transportUriFormat, "test-error"));

        await workTransport.TryDeleteAsync().ConfigureAwait(false);
        await workTransport.TryCreateAsync().ConfigureAwait(false);
        await workTransport.TryPurgeAsync().ConfigureAwait(false);

        await deferredTransport.TryDeleteAsync().ConfigureAwait(false);
        await deferredTransport.TryCreateAsync().ConfigureAwait(false);
        await deferredTransport.TryPurgeAsync().ConfigureAwait(false);

        await errorTransport.TryDeleteAsync().ConfigureAwait(false);
        await errorTransport.TryCreateAsync().ConfigureAwait(false);
        await errorTransport.TryPurgeAsync().ConfigureAwait(false);
    }

    protected async Task TestDeferredProcessingAsync(IServiceCollection services, string transportUriFormat, TimeSpan? timeoutTimeSpan = null, TimeSpan? deferTimeSpan = null)
    {
        Guard.AgainstNull(services);

        const int deferredMessageCount = 10;
        const int millisecondsToDefer = 100;

        services.AddOptions<MessageCountOptions>().Configure(options =>
        {
            options.MessageCount = deferredMessageCount;
        });

        services.AddSingleton<DeferredMessageFeature>();

        ConfigureServices(services, 1, transportUriFormat);

        var serviceProvider = await services.BuildServiceProvider().StartHostedServicesAsync().ConfigureAwait(false);

        serviceProvider.GetRequiredService<DeferredMessageFeature>();

        var logger = serviceProvider.GetLogger<DeferredFixture>();
        var busConfiguration = serviceProvider.GetRequiredService<IBusConfiguration>();
        var feature = serviceProvider.GetRequiredService<DeferredMessageFeature>();
        var busControl = serviceProvider.GetRequiredService<IBusControl>();
        var bus = serviceProvider.GetRequiredService<IBus>();
        var hopperOptions = serviceProvider.GetRequiredService<IOptions<HopperOptions>>();
        var transportService = serviceProvider.CreateTransportService();

        await ConfigureTransportsAsync(transportService, transportUriFormat).ConfigureAwait(false);

        hopperOptions.Value.DeferredMessageProcessingHalted += (eventArgs, _) =>
        {
            logger.LogDebug($"[DeferredMessageProcessingHalted] : restart date/time = '{eventArgs.RestartAt}'");

            return Task.CompletedTask;
        };

        hopperOptions.Value.DeferredMessageProcessingAdjusted += (eventArgs, _) =>
        {
            logger.LogDebug($"[DeferredMessageProcessingAdjusted] : next processing date/time = '{eventArgs.NextProcessingAt}'");

            return Task.CompletedTask;
        };

        try
        {
            var ignoreTillDate = DateTimeOffset.UtcNow.Add(deferTimeSpan ?? TimeSpan.FromSeconds(1));

            await busControl.StartAsync().ConfigureAwait(false);

            for (var i = 0; i < deferredMessageCount; i++)
            {
                var command = new SimpleCommand
                {
                    Name = Guid.NewGuid().ToString(),
                    Context = "SendDeferredMessage"
                };

                var date = ignoreTillDate;

                await bus.SendAsync(command, builder => builder.DeferUntil(date).WithRecipient(busConfiguration.Inbox!.WorkTransport!)).ConfigureAwait(false);

                ignoreTillDate = ignoreTillDate.AddMilliseconds(millisecondsToDefer);
            }

            logger.LogInformation($"[start wait] : now = '{DateTimeOffset.UtcNow}'");

            var timeout = ignoreTillDate.Add(timeoutTimeSpan ?? TimeSpan.FromSeconds(5));
            var timedOut = false;

            // wait for the message to be returned from the deferred queue
            while (await feature.HasPendingDeferredMessagesAsync() && !timedOut)
            {
                await Task.Delay(millisecondsToDefer).ConfigureAwait(false);

                timedOut = timeout < DateTimeOffset.UtcNow;
            }

            logger.LogInformation($"[end wait] : now = '{DateTimeOffset.UtcNow}' / expiry = '{timeout}' / timed out = '{timedOut}'");
            logger.LogInformation($"{feature.NumberOfDeferredMessagesReturned} of {deferredMessageCount} deferred messages returned to the inbox.");
            logger.LogInformation($"{feature.NumberOfMessagesHandled} of {deferredMessageCount} deferred messages handled.");

            Assert.That(await feature.HasPendingDeferredMessagesAsync(), Is.False, "All the deferred messages were not handled.");

            Assert.That(await busConfiguration.Inbox!.ErrorTransport!.HasPendingAsync().ConfigureAwait(false), Is.False);
            Assert.That(await busConfiguration.Inbox!.DeferredTransport!.ReceiveAsync().ConfigureAwait(false), Is.Null);
            Assert.That(await busConfiguration.Inbox!.WorkTransport!.ReceiveAsync().ConfigureAwait(false), Is.Null);

            await busControl.StopAsync().ConfigureAwait(false);
        }
        finally
        {
            await busControl.DisposeAsync().ConfigureAwait(false);
            await transportService.TryDeleteTransportsAsync(transportUriFormat).ConfigureAwait(false);
            await transportService.TryDisposeAsync().ConfigureAwait(false);
            await serviceProvider.StopHostedServicesAsync().ConfigureAwait(false);
        }
    }
}