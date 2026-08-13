using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using NUnit.Framework;
using Shuttle.Contract;
using Shuttle.Pipelines;
using Shuttle.Reflection;

namespace Shuttle.Hopper.Testing;

public class OutboxObserver : IPipelineObserver<MessageAcknowledged>
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

public abstract class OutboxFixture : IntegrationFixture
{
    private async Task ConfigureTransportsAsync(IServiceProvider serviceProvider, string transportUriFormat, string errorTransportUriFormat)
    {
        var transportService = serviceProvider.GetRequiredService<ITransportService>();
        var outboxWorkQueue = await transportService.GetAsync(string.Format(transportUriFormat, "test-outbox-work"));

        Assert.That(outboxWorkQueue.Type, Is.EqualTo(TransportType.Queue), "This test can only be applied to queues.");

        var errorTransport = await transportService.GetAsync(string.Format(errorTransportUriFormat, "test-error"));

        var receiverWorkQueue = await transportService.GetAsync(string.Format(transportUriFormat, "test-receiver-work"));

        await outboxWorkQueue.TryDeleteAsync().ConfigureAwait(false);
        await receiverWorkQueue.TryDeleteAsync().ConfigureAwait(false);
        await errorTransport.TryDeleteAsync().ConfigureAwait(false);

        await outboxWorkQueue.TryCreateAsync().ConfigureAwait(false);
        await receiverWorkQueue.TryCreateAsync().ConfigureAwait(false);
        await errorTransport.TryCreateAsync().ConfigureAwait(false);

        await outboxWorkQueue.TryPurgeAsync().ConfigureAwait(false);
        await receiverWorkQueue.TryPurgeAsync().ConfigureAwait(false);
        await errorTransport.TryPurgeAsync().ConfigureAwait(false);
    }

    protected async Task TestOutboxSendingAsync(IServiceCollection services, string workTransportUriFormat, int threadCount)
    {
        await TestOutboxSendingAsync(services, workTransportUriFormat, workTransportUriFormat, threadCount).ConfigureAwait(false);
    }

    protected async Task TestOutboxSendingAsync(IServiceCollection services, string workTransportUriFormat, string errorTransportUriFormat, int threadCount)
    {
        Guard.AgainstNull(services);

        const int count = 100;

        if (threadCount < 1)
        {
            threadCount = 1;
        }

        var workTransportUri = string.Format(workTransportUriFormat, "test-outbox-work");
        var receiverWorkTransportUri = string.Format(workTransportUriFormat, "test-receiver-work");
        var messageRouteProvider = new Mock<IMessageRouteProvider>();

        messageRouteProvider.Setup(m => m.GetRouteUrisAsync(It.IsAny<string>(), CancellationToken.None)).Returns(Task.FromResult<IEnumerable<string>>([receiverWorkTransportUri]));

        services.AddSingleton(messageRouteProvider.Object);

        services.AddHopper(options =>
        {
            options.Outbox = new()
            {
                WorkTransportUri = new(workTransportUri),
                IdleDurations = [TimeSpan.FromMilliseconds(25)],
                ThreadCount = threadCount
            };

            options.AutoStart = false;
        });

        var serviceProvider = await services.BuildServiceProvider().StartHostedServicesAsync().ConfigureAwait(false);

        var logger = serviceProvider.GetLogger<OutboxFixture>();
        var pipelineOptions = serviceProvider.GetRequiredService<IOptions<PipelineOptions>>();

        var outboxObserver = new OutboxObserver();

        pipelineOptions.Value.PipelineStarting += (eventArgs, _) =>
        {
            if (eventArgs.Pipeline.GetType() == typeof(OutboxPipeline))
            {
                eventArgs.Pipeline.AddObserver(outboxObserver);
            }

            return Task.CompletedTask;
        };

        var transportService = serviceProvider.CreateTransportService();

        await ConfigureTransportsAsync(serviceProvider, workTransportUriFormat, errorTransportUriFormat).ConfigureAwait(false);

        logger.LogInformation("Sending {0} messages.", count);

        var busControl = serviceProvider.GetRequiredService<IBusControl>();
        var bus = serviceProvider.GetRequiredService<IBus>();

        try
        {
            await busControl.StartAsync().ConfigureAwait(false);

            var command = new SimpleCommand { Context = "TestOutboxSending" };

            for (var i = 0; i < count; i++)
            {
                await bus.SendAsync(command).ConfigureAwait(false);
            }

            var receiverWorkQueue = await transportService.GetAsync(receiverWorkTransportUri);
            var timedOut = false;
            var timeout = DateTimeOffset.UtcNow.AddSeconds(150);

            while (outboxObserver.HandledMessageCount < count && !timedOut)
            {
                await Task.Delay(25).ConfigureAwait(false);

                timedOut = timeout < DateTimeOffset.UtcNow;
            }

            Assert.That(timedOut, Is.False, $"Timed out before processing {count} messages.");

            var pipeline = serviceProvider.CreatePipeline();

            for (var i = 0; i < count; i++)
            {
                var receivedMessage = await receiverWorkQueue.ReceiveAsync(pipeline).ConfigureAwait(false);

                Assert.That(receivedMessage, Is.Not.Null);

                await receiverWorkQueue.AcknowledgeAsync(receivedMessage!.AcknowledgementToken, pipeline).ConfigureAwait(false);
            }

            await receiverWorkQueue.TryDisposeAsync().ConfigureAwait(false);
            await receiverWorkQueue.TryDeleteAsync().ConfigureAwait(false);
        }
        finally
        {
            await busControl.DisposeAsync().ConfigureAwait(false);
        }

        await serviceProvider.StopHostedServicesAsync().ConfigureAwait(false);

        transportService = services.BuildServiceProvider().CreateTransportService();

        var outboxWorkQueue = await transportService.GetAsync(workTransportUri);

        Assert.That(await outboxWorkQueue.HasPendingAsync().ConfigureAwait(true), Is.False);

        await outboxWorkQueue.TryDisposeAsync().ConfigureAwait(false);

        var errorTransport = await transportService.GetAsync(string.Format(errorTransportUriFormat, "test-error"));

        await outboxWorkQueue.TryDeleteAsync().ConfigureAwait(false);
        await errorTransport.TryDeleteAsync().ConfigureAwait(false);
    }
}