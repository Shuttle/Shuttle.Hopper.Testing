using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NUnit.Framework;
using Shuttle.Core.Contract;
using Shuttle.Core.Reflection;
using Shuttle.Core.TransactionScope;

namespace Shuttle.Hopper.Testing;

public class DeferredFixture : IntegrationFixture
{
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

    private void ConfigureServices(IServiceCollection services, string test, int threadCount, bool isTransactional, string transportUriFormat)
    {
        Guard.AgainstNull(services);

        services.AddTransactionScope(builder =>
        {
            builder.Options.Enabled = isTransactional;
        });

        services.AddServiceBus(builder =>
        {
            builder.Options = new()
            {
                Inbox = new()
                {
                    WorkTransportUri = new(string.Format(transportUriFormat, "test-inbox-work")),
                    DeferredTransportUri = new(string.Format(transportUriFormat, "test-inbox-deferred")),
                    ErrorTransportUri = new(string.Format(transportUriFormat, "test-error")),
                    DurationToSleepWhenIdle = [TimeSpan.FromMilliseconds(25)],
                    DurationToIgnoreOnFailure = [TimeSpan.FromMilliseconds(25)],
                    ThreadCount = threadCount,
                    DeferredMessageProcessorResetInterval = TimeSpan.FromMilliseconds(25),
                    DeferredMessageProcessorWaitInterval = TimeSpan.FromMilliseconds(25)
                }
            };
            builder.SuppressHostedService();
        });

        services.ConfigureLogging(test);
    }

    protected async Task TestDeferredProcessingAsync(IServiceCollection services, string transportUriFormat, bool isTransactional)
    {
        Guard.AgainstNull(services);

        const int deferredMessageCount = 10;
        const int millisecondsToDefer = 100;

        services.AddOptions<MessageCountOptions>().Configure(options =>
        {
            options.MessageCount = deferredMessageCount;
        });

        services.AddSingleton<DeferredMessageFeature>();

        ConfigureServices(services, nameof(TestDeferredProcessingAsync), 1, isTransactional, transportUriFormat);

        var serviceProvider = await services.BuildServiceProvider().StartHostedServicesAsync().ConfigureAwait(false);

        serviceProvider.GetRequiredService<DeferredMessageFeature>();

        var logger = serviceProvider.GetLogger<DeferredFixture>();
        var serviceBusConfiguration = serviceProvider.GetRequiredService<IServiceBusConfiguration>();
        var feature = serviceProvider.GetRequiredService<DeferredMessageFeature>();
        var serviceBus = serviceProvider.GetRequiredService<IServiceBus>();
        var serviceBusOptions = serviceProvider.GetRequiredService<IOptions<ServiceBusOptions>>();
        var transportService = serviceProvider.CreateTransportService();

        await ConfigureTransportsAsync(transportService, transportUriFormat).ConfigureAwait(false);

        serviceBusOptions.Value.DeferredMessageProcessingHalted += (args, _) =>
        {
            logger.LogDebug($"[DeferredMessageProcessingHalted] : restart date/time = '{args.RestartDateTime}'");

            return Task.CompletedTask;
        };

        serviceBusOptions.Value.DeferredMessageProcessingAdjusted += (args, _) =>
        {
            logger.LogDebug($"[DeferredMessageProcessingAdjusted] : next processing date/time = '{args.NextProcessingDateTime}'");

            return Task.CompletedTask;
        };
        
        try
        {
            var ignoreTillDate = DateTime.UtcNow.AddSeconds(1);

            await serviceBus.StartAsync().ConfigureAwait(false);

            for (var i = 0; i < deferredMessageCount; i++)
            {
                var command = new SimpleCommand
                {
                    Name = Guid.NewGuid().ToString(),
                    Context = "EnqueueDeferredMessage"
                };

                var date = ignoreTillDate;

                await serviceBus.SendAsync(command, builder => builder.Defer(date).WithRecipient(serviceBusConfiguration.Inbox!.WorkTransport!)).ConfigureAwait(false);

                ignoreTillDate = ignoreTillDate.AddMilliseconds(millisecondsToDefer);
            }

            logger.LogInformation($"[start wait] : now = '{DateTime.Now}'");

            var timeout = ignoreTillDate.AddMilliseconds(deferredMessageCount * millisecondsToDefer + millisecondsToDefer * 2 + 3000);
            var timedOut = false;

            // wait for the message to be returned from the deferred queue
            while (await feature.HasPendingDeferredMessagesAsync() && !timedOut)
            {
                await Task.Delay(millisecondsToDefer).ConfigureAwait(false);

                timedOut = timeout < DateTime.UtcNow;
            }

            logger.LogInformation($"[end wait] : now = '{DateTime.Now}' / timeout = '{timeout.ToLocalTime()}' / timed out = '{timedOut}'");
            logger.LogInformation($"{feature.NumberOfDeferredMessagesReturned} of {deferredMessageCount} deferred messages returned to the inbox.");
            logger.LogInformation($"{feature.NumberOfMessagesHandled} of {deferredMessageCount} deferred messages handled.");

            Assert.That(await feature.HasPendingDeferredMessagesAsync(), Is.False, "All the deferred messages were not handled.");

            await serviceBus.StopAsync().ConfigureAwait(false);

            Assert.That(await serviceBusConfiguration.Inbox!.ErrorTransport!.HasPendingAsync().ConfigureAwait(false), Is.False);
            Assert.That(await serviceBusConfiguration.Inbox!.DeferredTransport!.ReceiveAsync().ConfigureAwait(false), Is.Null);
            Assert.That(await serviceBusConfiguration.Inbox!.WorkTransport!.ReceiveAsync().ConfigureAwait(false), Is.Null);
        }
        finally
        {
            await serviceBus.DisposeAsync().ConfigureAwait(false);
            await transportService.TryDeleteTransportsAsync(transportUriFormat).ConfigureAwait(false);
            await transportService.TryDisposeAsync().ConfigureAwait(false);
            await serviceProvider.StopHostedServicesAsync().ConfigureAwait(false);
        }
    }
}