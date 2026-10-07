using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NUnit.Framework;
using Shuttle.Contract;
using Shuttle.Reflection;

namespace Shuttle.Hopper.Testing;

public abstract class AdditionalInboxFixture : IntegrationFixture
{
    private const string InboxName = "priority";

    private static readonly string[] TransportNames =
    [
        "test-inbox-work",
        "test-inbox-deferred",
        "test-inbox-work-priority",
        "test-inbox-deferred-priority",
        "test-error"
    ];

    private static async Task ConfigureTransportsAsync(ITransportService transportService, string transportUriFormat)
    {
        foreach (var transportName in TransportNames)
        {
            var transport = await transportService.GetAsync(string.Format(transportUriFormat, transportName)).ConfigureAwait(false);

            await transport.TryDeleteAsync().ConfigureAwait(false);
            await transport.TryCreateAsync().ConfigureAwait(false);
            await transport.TryPurgeAsync().ConfigureAwait(false);
        }
    }

    // NOT APPLICABLE TO STREAMS
    protected async Task TestAdditionalInboxAsync(IServiceCollection services, string transportUriFormat, bool useOwnDeferredTransport, int messageCount = 5, TimeSpan? timeoutTimeSpan = null)
    {
        Guard.AgainstNull(services);
        Guard.AgainstEmpty(transportUriFormat);

        var primaryWorkTransportUri = new Uri(string.Format(transportUriFormat, "test-inbox-work"));
        var priorityWorkTransportUri = new Uri(string.Format(transportUriFormat, "test-inbox-work-priority"));

        var handled = new ConcurrentDictionary<Guid, ConcurrentBag<Uri>>();

        services
            .AddHopper(options =>
            {
                options.Inbox = new()
                {
                    WorkTransportUri = primaryWorkTransportUri,
                    DeferredTransportUri = new(string.Format(transportUriFormat, "test-inbox-deferred")),
                    ErrorTransportUri = new(string.Format(transportUriFormat, "test-error")),
                    IdleDurations = [TimeSpan.FromMilliseconds(25)],
                    IgnoreOnFailureDurations = [TimeSpan.FromMilliseconds(25)],
                    ThreadCount = 1,
                    DeferredMessageProcessorResetInterval = TimeSpan.FromMilliseconds(250),
                    DeferredMessageProcessorIdleDuration = TimeSpan.FromMilliseconds(25)
                };

                options.AutoStart = false;
            })
            .AddInbox(InboxName, options =>
            {
                options.WorkTransportUri = priorityWorkTransportUri;
                options.DeferredTransportUri = useOwnDeferredTransport ? new(string.Format(transportUriFormat, "test-inbox-deferred-priority")) : null;
                options.IdleDurations = [TimeSpan.FromMilliseconds(25)];
                options.IgnoreOnFailureDurations = [TimeSpan.FromMilliseconds(25)];
                options.ThreadCount = 1;
                options.DeferredMessageProcessorResetInterval = TimeSpan.FromMilliseconds(250);
                options.DeferredMessageProcessorIdleDuration = TimeSpan.FromMilliseconds(25);
            })
            .AddMessageHandler(async (IHandlerContext<AdditionalInboxCommand> context) =>
            {
                handled.GetOrAdd(context.Message.Id, _ => []).Add(Guard.AgainstNull(context.State.GetWorkTransport()).Uri.Uri);

                await Task.CompletedTask.ConfigureAwait(false);
            });

        var serviceProvider = await services.BuildServiceProvider().StartHostedServicesAsync().ConfigureAwait(false);

        var busControl = serviceProvider.GetRequiredService<IBusControl>();
        var bus = serviceProvider.GetRequiredService<IBus>();
        var busConfiguration = serviceProvider.GetRequiredService<IBusConfiguration>();
        var logger = serviceProvider.GetLogger<AdditionalInboxFixture>();
        var transportService = serviceProvider.CreateTransportService();

        logger.LogInformation("[TestAdditionalInbox] : message count = '{MessageCount}' / own deferred transport = '{UseOwnDeferredTransport}'", messageCount, useOwnDeferredTransport);

        try
        {
            await ConfigureTransportsAsync(transportService, transportUriFormat).ConfigureAwait(false);

            await busControl.StartAsync().ConfigureAwait(false);

            var priorityWorkTransport = Guard.AgainstNull(busConfiguration.AdditionalInboxes[InboxName].WorkTransport);

            Assert.That(priorityWorkTransport.Type, Is.EqualTo(TransportType.Queue), "This test can only be run against queues.");

            // A transport may normalise its uri, so the source of each message is compared to the transport's uri.
            var primaryUri = Guard.AgainstNull(busConfiguration.Inbox!.WorkTransport).Uri.Uri;
            var priorityUri = priorityWorkTransport.Uri.Uri;

            var expected = new Dictionary<Guid, Uri>();

            for (var i = 0; i < messageCount; i++)
            {
                var primaryCommand = new AdditionalInboxCommand();
                var priorityCommand = new AdditionalInboxCommand();

                await bus.SendAsync(primaryCommand, builder => builder.ToSelf()).ConfigureAwait(false);
                await bus.SendAsync(priorityCommand, builder => builder.ToInbox(InboxName)).ConfigureAwait(false);

                expected.Add(primaryCommand.Id, primaryUri);
                expected.Add(priorityCommand.Id, priorityUri);
            }

            var deferredCommand = new AdditionalInboxCommand();

            await bus.SendAsync(deferredCommand, builder => builder.ToInbox(InboxName).DeferFor(TimeSpan.FromMilliseconds(500))).ConfigureAwait(false);

            expected.Add(deferredCommand.Id, priorityUri);

            logger.LogInformation("[TestAdditionalInbox] : sent '{Count}' messages", expected.Count);

            var timeout = DateTimeOffset.UtcNow.Add(timeoutTimeSpan ?? TimeSpan.FromSeconds(30));

            while (handled.Count < expected.Count && DateTimeOffset.UtcNow < timeout)
            {
                await Task.Delay(25).ConfigureAwait(false);
            }

            Assert.Multiple(() =>
            {
                Assert.That(handled.Count, Is.EqualTo(expected.Count), $"[TIMEOUT] : Only {handled.Count} of the {expected.Count} messages were handled before {timeout:O}.");

                foreach (var (id, uri) in expected)
                {
                    Assert.That(handled.TryGetValue(id, out var sources), Is.True, $"Message '{id}' was not handled.");
                    Assert.That(sources, Is.EquivalentTo(new[] { uri }), $"Message '{id}' was not handled exactly once from '{uri}'.");
                }
            });
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
