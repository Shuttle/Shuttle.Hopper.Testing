using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NUnit.Framework;
using Shuttle.Core.Contract;
using Shuttle.Core.Pipelines;
using Shuttle.Core.Reflection;
using Shuttle.Core.Serialization;
using Shuttle.Core.Threading;
using System.Diagnostics;
using System.Reflection;
using Microsoft.Extensions.Logging.Abstractions;

namespace Shuttle.Hopper.Testing;

public class ThroughputObserver : IPipelineObserver<MessageAcknowledged>
{
    private readonly SemaphoreSlim _lock = new(1, 1);

    public int HandledMessageCount { get; private set; }

    public async Task ExecuteAsync(IPipelineContext<MessageAcknowledged> pipelineContext, CancellationToken cancellationToken = default)
    {
        await _lock.WaitAsync(cancellationToken);

        try
        {
            HandledMessageCount++;
        }
        finally
        {
            _lock.Release();
        }
    }
}

public class InboxMessagePipelineObserver(ILogger<InboxFixture>? logger = null) : IPipelineObserver<PipelineFailed>
{
    private readonly ILogger<InboxFixture> _logger = logger ?? NullLogger<InboxFixture>.Instance;

    public bool HasReceivedPipelineException { get; private set; }

    public async Task ExecuteAsync(IPipelineContext<PipelineFailed> pipelineContext, CancellationToken cancellationToken = default)
    {
        HasReceivedPipelineException = true;

        _logger.LogInformation($"[OnPipelineException] : {nameof(HasReceivedPipelineException)} = 'true'");

        await Task.CompletedTask.ConfigureAwait(false);
    }
}

public abstract class InboxFixture : IntegrationFixture
{
    private static void ConfigureServices(IServiceCollection services, bool hasErrorTransport, int threadCount, string transportUriFormat, TimeSpan durationToSleepWhenIdle)
    {
        Guard.AgainstNull(services);

        services.AddHopper(options =>
        {
            options.Inbox = new()
            {
                WorkTransportUri = new(string.Format(transportUriFormat, "test-inbox-work")),
                ErrorTransportUri = hasErrorTransport ? new(string.Format(transportUriFormat, "test-error")) : null,
                IdleDurations = [durationToSleepWhenIdle],
                IgnoreOnFailureDurations = [TimeSpan.FromMilliseconds(25)],
                ThreadCount = threadCount,
                MaximumFailureCount = 0
            };

            options.AutoStart = false;
        })
        .AddMessageHandlersFrom(Assembly.GetExecutingAssembly());
    }

    private static async Task ConfigureTransportsAsync(ITransportService transportService, string transportUriFormat, bool hasErrorTransport)
    {
        var workTransport = await transportService.GetAsync(string.Format(transportUriFormat, "test-inbox-work"));
        var errorTransport = hasErrorTransport ? await transportService.GetAsync(string.Format(transportUriFormat, "test-error")) : null;

        await workTransport.TryDeleteAsync().ConfigureAwait(false);
        await workTransport.TryCreateAsync().ConfigureAwait(false);
        await workTransport.TryPurgeAsync().ConfigureAwait(false);

        await (errorTransport?.TryDeleteAsync() ?? ValueTask.FromResult(false)).ConfigureAwait(false);
        await (errorTransport?.TryCreateAsync() ?? ValueTask.FromResult(false)).ConfigureAwait(false);
        await (errorTransport?.TryPurgeAsync() ?? ValueTask.FromResult(false)).ConfigureAwait(false);
    }

    // NOT APPLICABLE TO STREAMS
    protected async Task TestInboxConcurrencyAsync(IServiceCollection services, string transportUriFormat, TimeSpan expectedCompletionTimeSpan, TimeSpan? timeoutTimeSpan = null)
    {
        const int threadCount = 3;

        var semaphoreSlim = new SemaphoreSlim(1, 1);

        ConfigureServices(services, true, threadCount, transportUriFormat, TimeSpan.FromMilliseconds(25));

        services.AddSingleton<InboxConcurrencyFeature>();

        var serviceProvider = await services.BuildServiceProvider().StartHostedServicesAsync().ConfigureAwait(false);

        var transportMessagePipeline = serviceProvider.GetRequiredService<ITransportMessagePipeline>();
        var serializer = serviceProvider.GetRequiredService<ISerializer>();
        var feature = serviceProvider.GetRequiredService<InboxConcurrencyFeature>();
        var logger = serviceProvider.GetLogger<InboxFixture>();
        var transportService = serviceProvider.CreateTransportService();
        var busControl = serviceProvider.GetRequiredService<IBusControl>();
        var threadingOptions = serviceProvider.GetRequiredService<IOptions<ThreadingOptions>>();
        var busConfiguration = serviceProvider.GetRequiredService<IBusConfiguration>();

        logger.LogInformation("[TestInboxConcurrency] : thread count = '{ThreadCount}'", threadCount);

        try
        {
            await busConfiguration.ConfigureAsync();
            await ConfigureTransportsAsync(transportService, transportUriFormat, true).ConfigureAwait(false);

            var managedThreadIds = new List<int>();

            threadingOptions.Value.ProcessorExecuted += async (eventArgs, cancellationToken) =>
            {
                if (eventArgs is { WorkPerformed: false, ServiceKey: "InboxProcessor" })
                {
                    await semaphoreSlim.WaitAsync(cancellationToken);

                    try
                    {
                        if (!managedThreadIds.Contains(eventArgs.ManagedThreadId))
                        {
                            logger.LogInformation($"[TestInboxConcurrency] : service key = '{eventArgs.ServiceKey}' / managed thread id {eventArgs.ManagedThreadId} is idle");

                            managedThreadIds.Add(eventArgs.ManagedThreadId);
                        }
                    }
                    finally
                    {
                        semaphoreSlim.Release();
                    }
                }
            };

            logger.LogInformation("[TestInboxConcurrency] : starting service bus");

            Assert.That(busConfiguration.Inbox!.WorkTransport!.Type, Is.EqualTo(TransportType.Queue), "This test can only be run against queues.");

            logger.LogInformation("[TestInboxConcurrency] : enqueuing '{ThreadCount}' messages", threadCount);

            for (var i = 0; i < threadCount; i++)
            {
                await transportMessagePipeline.ExecuteAsync(new ConcurrentCommand { MessageIndex = i }, builder =>
                {
                    builder.WithRecipient(busConfiguration.Inbox.WorkTransport);
                }).ConfigureAwait(false);

                var transportMessage = transportMessagePipeline.State.GetTransportMessage()!;

                await busConfiguration.Inbox.WorkTransport.SendAsync(await serializer.SerializeAsync(transportMessage).ConfigureAwait(false), transportMessagePipeline.State).ConfigureAwait(false);
            }

            var timeout = DateTimeOffset.UtcNow.Add(timeoutTimeSpan ?? expectedCompletionTimeSpan.Add(TimeSpan.FromSeconds(5)));
            var timedOut = false;

            logger.LogInformation($"[TestInboxConcurrency] : waiting till {timeout:O} for all pipelines to become idle");

            await busControl.StartAsync().ConfigureAwait(false);

            while (managedThreadIds.Count < threadCount && !timedOut)
            {
                await Task.Delay(30).ConfigureAwait(false);
                timedOut = DateTimeOffset.UtcNow >= timeout;
            }

            Assert.That(timedOut, Is.False, $"[TIMEOUT] : All pipelines did not become idle before {timeout:O} / idle threads = {managedThreadIds.Count}");
        }
        finally
        {
            await busControl.DisposeAsync().ConfigureAwait(false);
            await transportService.TryDeleteTransportsAsync(transportUriFormat).ConfigureAwait(false);
            await transportService.TryDisposeAsync().ConfigureAwait(false);
            await serviceProvider.StopHostedServicesAsync().ConfigureAwait(false);
        }

        Assert.That(feature.OnAfterGetMessageCount, Is.EqualTo(threadCount), $"Got {feature.OnAfterGetMessageCount} messages but {threadCount} were sent.");
        Assert.That(feature.AllMessagesReceivedWithinTimespan(expectedCompletionTimeSpan), Is.True, $"All dequeued messages have to be within {expectedCompletionTimeSpan} ms of first get message.");
    }

    protected async Task TestInboxDeferredAsync(IServiceCollection services, string transportUriFormat, TimeSpan deferDuration = default)
    {
        services.AddSingleton<InboxDeferredFeature>();

        services.AddHopper(options =>
            {
                options.Inbox = new()
                {
                    WorkTransportUri = new(string.Format(transportUriFormat, "test-inbox-work")),
                    ErrorTransportUri = new(string.Format(transportUriFormat, "test-error")),
                    IdleDurations = [TimeSpan.FromMilliseconds(5)],
                    IgnoreOnFailureDurations = [TimeSpan.FromMilliseconds(5)],
                    ThreadCount = 1
                };

                options.AutoStart = false;
            });

        var serviceProvider = await services.BuildServiceProvider().StartHostedServicesAsync().ConfigureAwait(false);

        var busControl = serviceProvider.GetRequiredService<IBusControl>();
        var bus = serviceProvider.GetRequiredService<IBus>();
        var busConfiguration = serviceProvider.GetRequiredService<IBusConfiguration>();
        var logger = serviceProvider.GetLogger<InboxFixture>();
        var transportService = serviceProvider.CreateTransportService();

        var messageType = Guard.AgainstEmpty(typeof(ReceivePipelineCommand).FullName);

        await ConfigureTransportsAsync(transportService, transportUriFormat, true).ConfigureAwait(false);

        try
        {
            var feature = serviceProvider.GetRequiredService<InboxDeferredFeature>();
            var deferDurationValue = deferDuration == TimeSpan.Zero ? TimeSpan.FromMilliseconds(50) : deferDuration;

            await busControl.StartAsync().ConfigureAwait(false);

            var ignoreTillDate = DateTimeOffset.UtcNow.Add(deferDurationValue);

            var transportMessage = await bus.SendAsync(new ReceivePipelineCommand(),
                builder =>
                {
                    builder
                        .DeferUntil(ignoreTillDate)
                        .WithRecipient(busConfiguration.Inbox!.WorkTransport!);
                }).ConfigureAwait(false);

            Assert.That(transportMessage, Is.Not.Null);

            logger.LogInformation($"[SENT (thread {Environment.CurrentManagedThreadId})] : message id = {transportMessage.MessageId} / deferred to = '{ignoreTillDate:O}'");

            var messageId = transportMessage.MessageId;

            var timeout = DateTimeOffset.UtcNow.Add(deferDurationValue.Multiply(2));
            var timedOut = false;

            while (feature.TransportMessage == null && !timedOut)
            {
                await Task.Delay(5).ConfigureAwait(false);
                timedOut = DateTimeOffset.UtcNow >= timeout;
            }

            Assert.That(timedOut, Is.False, "[TIMEOUT] : The deferred message was never received.");
            Assert.That(feature.TransportMessage, Is.Not.Null, "The InboxDeferredFeature.TransportMessage cannot be `null`.");
            Assert.That(feature.TransportMessage!.MessageId, Is.EqualTo(messageId), "The InboxDeferredFeature.TransportMessage.MessageId received is not the one sent.");
            Assert.That(feature.TransportMessage.MessageType, Is.EqualTo(messageType), "The InboxDeferredFeature.TransportMessage.MessageType is not the same as the one sent.");
        }
        finally
        {
            await busControl.DisposeAsync().ConfigureAwait(false);
            await transportService.TryDeleteTransportsAsync(transportUriFormat).ConfigureAwait(false);
            await transportService.TryDisposeAsync().ConfigureAwait(false);
            await serviceProvider.StopHostedServicesAsync().ConfigureAwait(false);
        }
    }

    protected async Task TestInboxErrorAsync(IServiceCollection services, string transportUriFormat, bool hasErrorTransport, TimeSpan? timeoutTimeSpan = null)
    {
        ConfigureServices(services, hasErrorTransport, 1, transportUriFormat, TimeSpan.FromMilliseconds(25));

        var serviceProvider = await services.BuildServiceProvider().StartHostedServicesAsync().ConfigureAwait(false);

        var busControl = serviceProvider.GetRequiredService<IBusControl>();
        var busConfiguration = serviceProvider.GetRequiredService<IBusConfiguration>();
        var logger = serviceProvider.GetLogger<InboxFixture>();
        var pipelineOptions = serviceProvider.GetRequiredService<IOptions<PipelineOptions>>();
        var transportMessagePipeline = serviceProvider.GetRequiredService<ITransportMessagePipeline>();
        var serializer = serviceProvider.GetRequiredService<ISerializer>();
        var transportService = serviceProvider.CreateTransportService();

        var inboxMessagePipelineObserver = new InboxMessagePipelineObserver(logger);

        pipelineOptions.Value.PipelineStarting += (eventArgs, _) =>
        {
            if (eventArgs.Pipeline.GetType() == typeof(InboxMessagePipeline))
            {
                eventArgs.Pipeline.AddObserver(inboxMessagePipelineObserver, ObserverPosition.End);
            }

            return Task.CompletedTask;
        };

        try
        {
            await busConfiguration.ConfigureAsync();
            await ConfigureTransportsAsync(transportService, transportUriFormat, hasErrorTransport).ConfigureAwait(false);

            await transportMessagePipeline.ExecuteAsync(new ErrorCommand(), builder =>
            {
                builder.WithRecipient(busConfiguration.Inbox!.WorkTransport!);
            }).ConfigureAwait(false);

            var transportMessage = transportMessagePipeline.State.GetTransportMessage()!;

            logger.LogInformation($"[enqueuing] : message id = '{transportMessage.MessageId}'");

            await busConfiguration.Inbox!.WorkTransport!.SendAsync(await serializer.SerializeAsync(transportMessage).ConfigureAwait(false), transportMessagePipeline.State).ConfigureAwait(false);

            logger.LogInformation($"[enqueued] : message id = '{transportMessage.MessageId}'");

            await busControl.StartAsync().ConfigureAwait(false);

            var timeout = DateTimeOffset.UtcNow.Add(timeoutTimeSpan ?? TimeSpan.FromSeconds(5));
            var timedOut = false;

            while (!inboxMessagePipelineObserver.HasReceivedPipelineException && !timedOut)
            {
                await Task.Delay(25).ConfigureAwait(false);
                timedOut = DateTimeOffset.UtcNow > timeout;
            }

            Assert.That(!timedOut, "Timed out before message was received.");

            await busControl.StopAsync().ConfigureAwait(false);

            if (hasErrorTransport)
            {
                Assert.That(await (await transportService.GetAsync(string.Format(transportUriFormat, "test-inbox-work"))).ReceiveAsync().ConfigureAwait(false), Is.Null, "Should not have a message in queue 'test-inbox-work'.");
                Assert.That(await (await transportService.GetAsync(string.Format(transportUriFormat, "test-error"))).ReceiveAsync().ConfigureAwait(false), Is.Not.Null, "Should have a message in queue 'test-error'.");
            }
            else
            {
                Assert.That(await (await transportService.GetAsync(string.Format(transportUriFormat, "test-inbox-work"))).ReceiveAsync().ConfigureAwait(false), Is.Not.Null, "Should have a message in queue 'test-inbox-work'.");
            }
        }
        finally
        {
            await busControl.DisposeAsync().ConfigureAwait(false);
            await transportService.DisposeAsync().ConfigureAwait(false);
            await serviceProvider.StopHostedServicesAsync().ConfigureAwait(false);
            await transportService.TryDeleteTransportsAsync(transportUriFormat).ConfigureAwait(false);
        }
    }

    protected async Task TestInboxExpiryAsync(IServiceCollection services, string transportUriFormat, TimeSpan? expiryDuration = null)
    {
        expiryDuration ??= TimeSpan.FromMilliseconds(500);

        services
            .AddHopper(options =>
            {
                options.AutoStart = false;
            });

        var serviceProvider = await services.BuildServiceProvider().StartHostedServicesAsync().ConfigureAwait(false);

        var transportMessagePipeline = serviceProvider.GetRequiredService<ITransportMessagePipeline>();
        var serializer = serviceProvider.GetRequiredService<ISerializer>();

        var transportService = serviceProvider.CreateTransportService();

        try
        {
            var transport = await transportService.GetAsync(string.Format(transportUriFormat, "test-inbox-work"));

            await transport.TryDeleteAsync().ConfigureAwait(false);
            await transport.TryCreateAsync().ConfigureAwait(false);
            await transport.TryPurgeAsync().ConfigureAwait(false);

            void Builder(TransportMessageBuilder builder)
            {
                builder.ExpiresIn(expiryDuration.Value);
                builder.WithRecipient(transport);
            }

            await transportMessagePipeline.ExecuteAsync(new ReceivePipelineCommand(), Builder).ConfigureAwait(false);

            var transportMessage = transportMessagePipeline.State.GetTransportMessage()!;

            await transport.SendAsync(await serializer.SerializeAsync(transportMessage).ConfigureAwait(false), transportMessagePipeline.State).ConfigureAwait(false);

            Assert.That(transportMessage, Is.Not.Null, "TransportMessage may not be null.");
            Assert.That(transportMessage.HasExpired(), Is.False, "The message has already expired before being processed.");

            // wait until the message expires
            await Task.Delay(expiryDuration.Value.Add(TimeSpan.FromMilliseconds(50))).ConfigureAwait(false);

            Assert.That(await transport.ReceiveAsync().ConfigureAwait(false), Is.Null, "The message did not expire.  Call this test only if your queue actually supports message expiry internally.");

            await transport.TryDeleteAsync().ConfigureAwait(false);
        }
        finally
        {
            await transportService.DisposeAsync().ConfigureAwait(false);
            await serviceProvider.StopHostedServicesAsync().ConfigureAwait(false);
        }
    }

    protected async Task TestInboxThroughputAsync(IServiceCollection services, string transportUriFormat, int messageCount, int threadCount, TimeSpan? timeoutTimeSpan = null)
    {
        if (messageCount < 1)
        {
            messageCount = 1;
        }

        if (threadCount < 1)
        {
            threadCount = 1;
        }

        var timeoutTimeSpanValue = timeoutTimeSpan ?? TimeSpan.FromMilliseconds(messageCount / threadCount * 50);

        ConfigureServices(Guard.AgainstNull(services), true, threadCount, transportUriFormat, TimeSpan.FromMilliseconds(25));

        var serviceProvider = await services.BuildServiceProvider().StartHostedServicesAsync().ConfigureAwait(false);

        var pipelineOptions = serviceProvider.GetRequiredService<IOptions<PipelineOptions>>();

        var throughputObserver = new ThroughputObserver();

        pipelineOptions.Value.PipelineStarting += (eventArgs, _) =>
        {
            if (eventArgs.Pipeline.GetType() == typeof(InboxMessagePipeline))
            {
                eventArgs.Pipeline.AddObserver(throughputObserver);
            }

            return Task.CompletedTask;
        };

        var transportMessagePipeline = serviceProvider.GetRequiredService<ITransportMessagePipeline>();
        var serializer = serviceProvider.GetRequiredService<ISerializer>();
        var logger = serviceProvider.GetLogger<InboxFixture>();
        var transportService = serviceProvider.CreateTransportService();
        var busControl = serviceProvider.GetRequiredService<IBusControl>();
        var busConfiguration = serviceProvider.GetRequiredService<IBusConfiguration>();

        var sw = new Stopwatch();
        var timedOut = false;

        await ConfigureTransportsAsync(transportService, transportUriFormat, true).ConfigureAwait(false);
        await busConfiguration.ConfigureAsync();

        try
        {
            logger.LogInformation($"Sending {messageCount} messages to input queue '{busConfiguration.Inbox!.WorkTransport!.Uri}'.");

            sw.Start();

            for (var i = 0; i < messageCount; i++)
            {
                await transportMessagePipeline.ExecuteAsync(new SimpleCommand("command " + i) { Context = "TestInboxThroughput" }, builder =>
                {
                    builder.WithRecipient(busConfiguration.Inbox.WorkTransport);
                }).ConfigureAwait(false);

                var transportMessage = transportMessagePipeline.State.GetTransportMessage()!;

                await busConfiguration.Inbox.WorkTransport.SendAsync(await serializer.SerializeAsync(transportMessage).ConfigureAwait(false), transportMessagePipeline.State).ConfigureAwait(false);
            }

            sw.Stop();

            logger.LogInformation("Took {0} ms to send {1} messages.  Starting processing.", sw.ElapsedMilliseconds, messageCount);

            sw.Reset();

            await busControl.StartAsync().ConfigureAwait(false);

            logger.LogInformation($"[starting] : {DateTimeOffset.UtcNow:HH:mm:ss.fff}");

            var timeout = DateTimeOffset.UtcNow.Add(timeoutTimeSpanValue);

            sw.Start();

            while (throughputObserver.HandledMessageCount < messageCount && !timedOut)
            {
                await Task.Delay(25).ConfigureAwait(false);

                timedOut = DateTimeOffset.UtcNow > timeout;
            }

            sw.Stop();

            logger.LogInformation($"[stopped] : {DateTimeOffset.UtcNow:HH:mm:ss.fff}");

            await transportService.TryDeleteTransportsAsync(transportUriFormat).ConfigureAwait(false);
        }
        finally
        {
            await busControl.DisposeAsync().ConfigureAwait(false);
            await transportService.TryDisposeAsync().ConfigureAwait(false);
            await serviceProvider.StopHostedServicesAsync().ConfigureAwait(false);
        }

        if (!timedOut)
        {
            Assert.That(sw.Elapsed < timeoutTimeSpanValue, Is.True, $"Should be able to process at least {messageCount} messages in {timeoutTimeSpanValue} but it took {sw.Elapsed}.");
        }
        else
        {
            Assert.Fail($"Timed out before processing {messageCount} messages.  Only processed {throughputObserver.HandledMessageCount} messages in {sw.Elapsed}.");
        }
    }
}