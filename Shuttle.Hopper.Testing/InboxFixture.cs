using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NUnit.Framework;
using Shuttle.Core.Contract;
using Shuttle.Core.Pipelines;
using Shuttle.Core.Reflection;
using Shuttle.Core.Serialization;
using Shuttle.Core.TransactionScope;

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

public class InboxMessagePipelineObserver(ILogger<InboxFixture> logger) : IPipelineObserver<PipelineFailed>
{
    private readonly ILogger<InboxFixture> _logger = Guard.AgainstNull(logger);

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

    private static void ConfigureServices(IServiceCollection services, string test, bool hasErrorTransport, int threadCount, bool isTransactional, string transportUriFormat, TimeSpan durationToSleepWhenIdle)
    {
        Guard.AgainstNull(services);

        services.AddTransactionScope(builder =>
        {
            builder.Options.Enabled = isTransactional;
        });

        var serviceBusOptions = new ServiceBusOptions
        {
            Inbox = new()
            {
                WorkTransportUri = new(string.Format(transportUriFormat, "test-inbox-work")),
                ErrorTransportUri = hasErrorTransport ? new(string.Format(transportUriFormat, "test-error")) : null,
                IdleDurations = [durationToSleepWhenIdle],
                IgnoreOnFailureDurations = [TimeSpan.FromMilliseconds(25)],
                ThreadCount = threadCount,
                MaximumFailureCount = 0
            }
        };

        services.AddServiceBus(builder =>
        {
            builder.Options = serviceBusOptions;
            builder.SuppressHostedService();
        });

        services.ConfigureLogging(test);
    }

    // NOT APPLICABLE TO STREAMS
    protected async Task TestInboxConcurrencyAsync(IServiceCollection services, string transportUriFormat, int msToComplete, bool isTransactional)
    {
        const int threadCount = 3;

        var semaphoreSlim = new SemaphoreSlim(1, 1);

        ConfigureServices(services, nameof(TestInboxConcurrencyAsync), true, threadCount, isTransactional, transportUriFormat, TimeSpan.FromMilliseconds(25));

        services.AddSingleton<InboxConcurrencyFeature>();

        var serviceProvider = await services.BuildServiceProvider().StartHostedServicesAsync().ConfigureAwait(false);

        var pipelineFactory = serviceProvider.GetRequiredService<IPipelineFactory>();
        var transportMessagePipeline = await pipelineFactory.GetPipelineAsync<TransportMessagePipeline>();
        var serializer = serviceProvider.GetRequiredService<ISerializer>();
        var feature = serviceProvider.GetRequiredService<InboxConcurrencyFeature>();
        var logger = serviceProvider.GetLogger<InboxFixture>();
        var transportService = serviceProvider.CreateTransportService();
        var serviceBus = serviceProvider.GetRequiredService<IServiceBus>();
        var serviceBusOptions = serviceProvider.GetRequiredService<IOptions<ServiceBusOptions>>();
        var serviceBusConfiguration = serviceProvider.GetRequiredService<IServiceBusConfiguration>();

        logger.LogInformation("[TestInboxConcurrency] : thread count = '{ThreadCount}'", threadCount);

        try
        {
            await serviceBusConfiguration.ConfigureAsync();
            await ConfigureTransportsAsync(transportService, transportUriFormat, true).ConfigureAwait(false);

            var idlePipelines = new List<Guid>();

            serviceBusOptions.Value.ThreadWorking += (eventArgs, _) =>
            {
                logger.LogInformation("[TestInboxConcurrency] : pipeline = '{FullName}' / pipeline id '{PipelineId}' is performing work", eventArgs.Pipeline.GetType().FullName, eventArgs.Pipeline.Id);

                return Task.CompletedTask;
            };

            serviceBusOptions.Value.ThreadWaiting += async (eventArgs, cancellationToken) =>
            {
                await semaphoreSlim.WaitAsync(cancellationToken);

                try
                {
                    if (!idlePipelines.Contains(eventArgs.Pipeline.Id))
                    {
                        logger.LogInformation($"[TestInboxConcurrency] : pipeline = '{eventArgs.Pipeline.GetType().FullName}' / pipeline id '{eventArgs.Pipeline.Id}' is idle");

                        idlePipelines.Add(eventArgs.Pipeline.Id);
                    }
                }
                finally
                {
                    semaphoreSlim.Release();
                }
            };

            logger.LogInformation("[TestInboxConcurrency] : starting service bus");

            Assert.That(serviceBusConfiguration.Inbox!.WorkTransport!.Type, Is.EqualTo(TransportType.Queue), "This test can only be run against queues.");

            logger.LogInformation("[TestInboxConcurrency] : enqueuing '{ThreadCount}' messages", threadCount);

            for (var i = 0; i < threadCount; i++)
            {
                await transportMessagePipeline.ExecuteAsync(new ConcurrentCommand { MessageIndex = i }, null, builder =>
                {
                    builder.WithRecipient(serviceBusConfiguration.Inbox.WorkTransport);
                }).ConfigureAwait(false);

                var transportMessage = transportMessagePipeline.State.GetTransportMessage()!;

                await serviceBusConfiguration.Inbox.WorkTransport.SendAsync(transportMessage, await serializer.SerializeAsync(transportMessage).ConfigureAwait(false)).ConfigureAwait(false);
            }

            var timeout = DateTime.Now.AddSeconds(5);
            var timedOut = false;

            logger.LogInformation($"[TestInboxConcurrency] : waiting till {timeout:O} for all pipelines to become idle");

            await serviceBus.StartAsync().ConfigureAwait(false);

            while (idlePipelines.Count < threadCount && !timedOut)
            {
                await Task.Delay(30).ConfigureAwait(false);
                timedOut = DateTime.Now >= timeout;
            }

            Assert.That(timedOut, Is.False, $"[TIMEOUT] : All pipelines did not become idle before {timeout:O} / idle threads = {idlePipelines.Count}");
        }
        finally
        {
            await serviceBus.DisposeAsync().ConfigureAwait(false);
            await transportService.TryDeleteTransportsAsync(transportUriFormat).ConfigureAwait(false);
            await transportService.TryDisposeAsync().ConfigureAwait(false);
            await serviceProvider.StopHostedServicesAsync().ConfigureAwait(false);
        }

        Assert.That(feature.OnAfterGetMessageCount, Is.EqualTo(threadCount), $"Got {feature.OnAfterGetMessageCount} messages but {threadCount} were sent.");
        Assert.That(feature.AllMessagesReceivedWithinTimespan(msToComplete), Is.True, $"All dequeued messages have to be within {msToComplete} ms of first get message.");
    }

    protected async Task TestInboxDeferredAsync(IServiceCollection services, string transportUriFormat, TimeSpan deferDuration = default)
    {
        var serviceBusOptions = new ServiceBusOptions
        {
            Inbox = new()
            {
                WorkTransportUri = new(string.Format(transportUriFormat, "test-inbox-work")),
                ErrorTransportUri = new(string.Format(transportUriFormat, "test-error")),
                IdleDurations = [TimeSpan.FromMilliseconds(5)],
                IgnoreOnFailureDurations = [TimeSpan.FromMilliseconds(5)],
                ThreadCount = 1
            }
        };

        services.AddSingleton<InboxDeferredFeature>();

        services.AddServiceBus(builder =>
        {
            builder.Options = serviceBusOptions;
            builder.SuppressHostedService();
        });

        services.ConfigureLogging(nameof(TestInboxDeferredAsync));

        var serviceProvider = await services.BuildServiceProvider().StartHostedServicesAsync().ConfigureAwait(false);

        var serviceBus = serviceProvider.GetRequiredService<IServiceBus>();
        var serviceBusConfiguration = serviceProvider.GetRequiredService<IServiceBusConfiguration>();
        var logger = serviceProvider.GetLogger<InboxFixture>();
        var transportService = serviceProvider.CreateTransportService();

        var messageType = Guard.AgainstEmpty(typeof(ReceivePipelineCommand).FullName);

        await ConfigureTransportsAsync(transportService, transportUriFormat, true).ConfigureAwait(false);
        
        try
        {
            var feature = serviceProvider.GetRequiredService<InboxDeferredFeature>();
            var deferDurationValue = deferDuration == TimeSpan.Zero ? TimeSpan.FromMilliseconds(50) : deferDuration;

            await serviceBus.StartAsync().ConfigureAwait(false);

            var ignoreTillDate = DateTime.Now.Add(deferDurationValue);

            var transportMessage = await serviceBus.SendAsync(new ReceivePipelineCommand(),
                builder =>
                {
                    builder
                        .Defer(ignoreTillDate)
                        .WithRecipient(serviceBusConfiguration.Inbox!.WorkTransport!);
                }).ConfigureAwait(false);

            Assert.That(transportMessage, Is.Not.Null);

            logger.LogInformation($"[SENT (thread {Environment.CurrentManagedThreadId})] : message id = {transportMessage.MessageId} / deferred to = '{ignoreTillDate:O}'");

            var messageId = transportMessage.MessageId;

            var timeout = DateTime.Now.Add(deferDurationValue.Multiply(2));
            var timedOut = false;

            while (feature.TransportMessage == null && !timedOut)
            {
                await Task.Delay(5).ConfigureAwait(false);
                timedOut = DateTime.Now >= timeout;
            }

            Assert.That(timedOut, Is.False, "[TIMEOUT] : The deferred message was never received.");
            Assert.That(feature.TransportMessage, Is.Not.Null, "The InboxDeferredFeature.TransportMessage cannot be `null`.");
            Assert.That(feature.TransportMessage!.MessageId, Is.EqualTo(messageId), "The InboxDeferredFeature.TransportMessage.MessageId received is not the one sent.");
            Assert.That(feature.TransportMessage.MessageType, Is.EqualTo(messageType), "The InboxDeferredFeature.TransportMessage.MessageType is not the same as the one sent.");
        }
        finally
        {
            await serviceBus.TryDisposeAsync().ConfigureAwait(false);
            await transportService.TryDeleteTransportsAsync(transportUriFormat).ConfigureAwait(false);
            await transportService.TryDisposeAsync().ConfigureAwait(false);
            await serviceProvider.StopHostedServicesAsync().ConfigureAwait(false);
        }
    }

    protected async Task TestInboxErrorAsync(IServiceCollection services, string transportUriFormat, bool hasErrorTransport, bool isTransactional)
    {
        ConfigureServices(services, nameof(TestInboxErrorAsync), hasErrorTransport, 1, isTransactional, transportUriFormat, TimeSpan.FromMilliseconds(25));

        var serviceProvider = await services.BuildServiceProvider().StartHostedServicesAsync().ConfigureAwait(false);

        var serviceBus = serviceProvider.GetRequiredService<IServiceBus>();
        var serviceBusConfiguration = serviceProvider.GetRequiredService<IServiceBusConfiguration>();
        var logger = serviceProvider.GetLogger<InboxFixture>();
        var pipelineFactory = serviceProvider.GetRequiredService<IPipelineFactory>();
        var pipelineOptions = serviceProvider.GetRequiredService<IOptions<PipelineOptions>>();
        var transportMessagePipeline = await pipelineFactory.GetPipelineAsync<TransportMessagePipeline>();
        var serializer = serviceProvider.GetRequiredService<ISerializer>();
        var transportService = serviceProvider.CreateTransportService();

        var inboxMessagePipelineObserver = new InboxMessagePipelineObserver(logger);

        pipelineOptions.Value.PipelineCreated += (eventArgs, _) =>
        {
            if (eventArgs.Pipeline.GetType() == typeof(InboxMessagePipeline))
            {
                eventArgs.Pipeline.AddObserver(inboxMessagePipelineObserver);
            }

            return Task.CompletedTask;
        };

        try
        {
            await serviceBusConfiguration.ConfigureAsync();
            await ConfigureTransportsAsync(transportService, transportUriFormat, hasErrorTransport).ConfigureAwait(false);

            await transportMessagePipeline.ExecuteAsync(new ErrorCommand(), null, builder =>
            {
                builder.WithRecipient(serviceBusConfiguration.Inbox!.WorkTransport!);
            }).ConfigureAwait(false);

            var transportMessage = transportMessagePipeline.State.GetTransportMessage()!;

            logger.LogInformation($"[enqueuing] : message id = '{transportMessage.MessageId}'");

            await serviceBusConfiguration.Inbox!.WorkTransport!.SendAsync(transportMessage, await serializer.SerializeAsync(transportMessage).ConfigureAwait(false)).ConfigureAwait(false);

            logger.LogInformation($"[enqueued] : message id = '{transportMessage.MessageId}'");

            await serviceBus.StartAsync().ConfigureAwait(false);

            var timeout = DateTime.Now.AddSeconds(150);
            var timedOut = false;

            while (!inboxMessagePipelineObserver.HasReceivedPipelineException && !timedOut)
            {
                await Task.Delay(25).ConfigureAwait(false);
                timedOut = DateTime.Now > timeout;
            }

            Assert.That(!timedOut, "Timed out before message was received.");

            await serviceBus.StopAsync().ConfigureAwait(false);

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
            await serviceBus.DisposeAsync().ConfigureAwait(false);
            await transportService.DisposeAsync().ConfigureAwait(false);
            await serviceProvider.StopHostedServicesAsync().ConfigureAwait(false);
            await transportService.TryDeleteTransportsAsync(transportUriFormat).ConfigureAwait(false);
        }
    }

    protected async Task TestInboxExpiryAsync(IServiceCollection services, string transportUriFormat, TimeSpan? expiryDuration = null)
    {
        expiryDuration ??= TimeSpan.FromMilliseconds(500);

        services
            .AddServiceBus(builder =>
            {
                builder.SuppressHostedService();
            })
            .ConfigureLogging(nameof(TestInboxExpiryAsync));

        var serviceProvider = await services.BuildServiceProvider().StartHostedServicesAsync().ConfigureAwait(false);

        var pipelineFactory = serviceProvider.GetRequiredService<IPipelineFactory>();
        var transportMessagePipeline = await pipelineFactory.GetPipelineAsync<TransportMessagePipeline>();
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
                builder.WillExpire(expiryDuration.Value);
                builder.WithRecipient(transport);
            }

            await transportMessagePipeline.ExecuteAsync(new ReceivePipelineCommand(), null, Builder).ConfigureAwait(false);

            var transportMessage = transportMessagePipeline.State.GetTransportMessage()!;

            await transport.SendAsync(transportMessage, await serializer.SerializeAsync(transportMessage).ConfigureAwait(false)).ConfigureAwait(false);

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

    protected async Task TestInboxThroughputAsync(IServiceCollection services, string transportUriFormat, int timeoutMilliseconds, int messageCount, int threadCount, bool isTransactional)
    {
        if (threadCount < 1)
        {
            threadCount = 1;
        }

        ConfigureServices(Guard.AgainstNull(services), nameof(TestInboxThroughputAsync), true, threadCount, isTransactional, transportUriFormat, TimeSpan.FromMilliseconds(25));

        var serviceProvider = await services.BuildServiceProvider().StartHostedServicesAsync().ConfigureAwait(false);

        var pipelineFactory = serviceProvider.GetRequiredService<IPipelineFactory>();
        var pipelineOptions = serviceProvider.GetRequiredService<IOptions<PipelineOptions>>();

        var throughputObserver = new ThroughputObserver();

        pipelineOptions.Value.PipelineCreated += (eventArgs, _) =>
        {
            if (eventArgs.Pipeline.GetType() == typeof(InboxMessagePipeline))
            {
                eventArgs.Pipeline.AddObserver(throughputObserver);
            }

            return Task.CompletedTask;
        };

        var transportMessagePipeline = await pipelineFactory.GetPipelineAsync<TransportMessagePipeline>();
        var serializer = serviceProvider.GetRequiredService<ISerializer>();
        var logger = serviceProvider.GetLogger<InboxFixture>();
        var transportService = serviceProvider.CreateTransportService();
        var serviceBus = serviceProvider.GetRequiredService<IServiceBus>();
        var serviceBusConfiguration = serviceProvider.GetRequiredService<IServiceBusConfiguration>();

        var sw = new Stopwatch();
        var timedOut = false;

        await ConfigureTransportsAsync(transportService, transportUriFormat, true).ConfigureAwait(false);
        await serviceBusConfiguration.ConfigureAsync();

        try
        {
            logger.LogInformation($"Sending {messageCount} messages to input queue '{serviceBusConfiguration.Inbox!.WorkTransport!.Uri}'.");

            sw.Start();

            for (var i = 0; i < messageCount; i++)
            {
                await transportMessagePipeline.ExecuteAsync(new SimpleCommand("command " + i) { Context = "TestInboxThroughput" }, null, builder =>
                {
                    builder.WithRecipient(serviceBusConfiguration.Inbox.WorkTransport);
                }).ConfigureAwait(false);

                var transportMessage = transportMessagePipeline.State.GetTransportMessage()!;

                await serviceBusConfiguration.Inbox.WorkTransport.SendAsync(transportMessage, await serializer.SerializeAsync(transportMessage).ConfigureAwait(false)).ConfigureAwait(false);
            }

            sw.Stop();

            logger.LogInformation("Took {0} ms to send {1} messages.  Starting processing.", sw.ElapsedMilliseconds, messageCount);

            sw.Reset();

            await serviceBus.StartAsync().ConfigureAwait(false);

            logger.LogInformation($"[starting] : {DateTime.Now:HH:mm:ss.fff}");

            var timeout = DateTime.Now.AddSeconds(5);

            sw.Start();

            while (throughputObserver.HandledMessageCount < messageCount && !timedOut)
            {
                await Task.Delay(25).ConfigureAwait(false);

                timedOut = DateTime.Now > timeout;
            }

            sw.Stop();

            logger.LogInformation($"[stopped] : {DateTime.Now:HH:mm:ss.fff}");

            await transportService.TryDeleteTransportsAsync(transportUriFormat).ConfigureAwait(false);
        }
        finally
        {
            await serviceBus.DisposeAsync().ConfigureAwait(false);
            await transportService.TryDisposeAsync().ConfigureAwait(false);
            await serviceProvider.StopHostedServicesAsync().ConfigureAwait(false);
        }

        var ms = sw.ElapsedMilliseconds;

        if (!timedOut)
        {
            logger.LogInformation($"Processed {messageCount} messages in {ms} ms");

            Assert.That(ms < timeoutMilliseconds, Is.True, $"Should be able to process at least {messageCount} messages in {timeoutMilliseconds} ms but it took {ms} ms.");
        }
        else
        {
            Assert.Fail($"Timed out before processing {messageCount} messages.  Only processed {throughputObserver.HandledMessageCount} messages in {ms}.");
        }
    }
}