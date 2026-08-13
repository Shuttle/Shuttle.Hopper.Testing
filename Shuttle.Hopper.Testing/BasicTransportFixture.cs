using Microsoft.Extensions.DependencyInjection;
using NUnit.Framework;
using Shuttle.Contract;
using Shuttle.Pipelines;
using Shuttle.Reflection;

namespace Shuttle.Hopper.Testing;

public class BasicTransportFixture : IntegrationFixture
{
    private void ConfigureServices(IServiceCollection services, int threadCount, string transportUriFormat)
    {
        Guard.AgainstNull(services);

        services.AddHopper(options =>
        {
            options.Inbox = new()
            {
                WorkTransportUri = new(string.Format(transportUriFormat, "test-inbox-work")),
                ErrorTransportUri = new(string.Format(transportUriFormat, "test-error")),
                IdleDurations = [TimeSpan.FromMilliseconds(25)],
                IgnoreOnFailureDurations = [TimeSpan.FromMilliseconds(25)],
                ThreadCount = threadCount
            };
        });
    }

    private async Task<ITransport> CreateWorkTransportAsync(ITransportService transportService, string workTransportUriFormat, bool refresh)
    {
        var workTransport = await Guard.AgainstNull(transportService).GetAsync(string.Format(workTransportUriFormat, "test-work"));

        if (refresh)
        {
            await workTransport.TryDeleteAsync().ConfigureAwait(false);
            await workTransport.TryCreateAsync().ConfigureAwait(false);
            await workTransport.TryPurgeAsync().ConfigureAwait(false);
        }

        return workTransport;
    }

    protected async Task TestReleaseMessageAsync(IServiceCollection services, string transportUriFormat)
    {
        ConfigureServices(Guard.AgainstNull(services), 1, transportUriFormat);

        var serviceProvider = services.BuildServiceProvider();
        var transportService = serviceProvider.CreateTransportService();
        var workTransport = await CreateWorkTransportAsync(transportService, transportUriFormat, true).ConfigureAwait(false);
        var pipeline = serviceProvider.CreatePipeline();

        try
        {
            pipeline.State.SetTransportMessage(new() { MessageId = Guid.NewGuid() });

            await workTransport.SendAsync(new MemoryStream("message-body"u8.ToArray()), pipeline).ConfigureAwait(false);

            var receivedMessage = await workTransport.ReceiveAsync(pipeline).ConfigureAwait(false);

            Assert.That(receivedMessage, Is.Not.Null);
            Assert.That(await workTransport.ReceiveAsync(pipeline).ConfigureAwait(false), Is.Null);

            await workTransport.ReleaseAsync(receivedMessage!.AcknowledgementToken, pipeline).ConfigureAwait(false);

            receivedMessage = await workTransport.ReceiveAsync(pipeline).ConfigureAwait(false);

            Assert.That(receivedMessage, Is.Not.Null);
            Assert.That(await workTransport.ReceiveAsync(pipeline).ConfigureAwait(false), Is.Null);

            await workTransport.AcknowledgeAsync(receivedMessage!.AcknowledgementToken, pipeline).ConfigureAwait(false);

            Assert.That(await workTransport.ReceiveAsync(pipeline).ConfigureAwait(false), Is.Null);

            await workTransport.TryDeleteAsync().ConfigureAwait(false);
        }
        finally
        {
            await workTransport.TryDisposeAsync().ConfigureAwait(false);
            await transportService.TryDisposeAsync().ConfigureAwait(false);
        }
    }

    protected async Task TestSimpleSendAndReceiveAsync(IServiceCollection services, string transportUriFormat)
    {
        ConfigureServices(Guard.AgainstNull(services), 1, transportUriFormat);

        var serviceProvider = services.BuildServiceProvider();
        var transportService = serviceProvider.CreateTransportService();
        var workTransport = await CreateWorkTransportAsync(transportService, transportUriFormat, true).ConfigureAwait(false);
        var pipeline = serviceProvider.CreatePipeline();

        try
        {
            var stream = new MemoryStream();

            stream.WriteByte(100);

            pipeline.State.SetTransportMessage(new() { MessageId = Guid.NewGuid() });

            await workTransport.SendAsync(stream, pipeline).ConfigureAwait(false);

            var receivedMessage = await workTransport.ReceiveAsync(pipeline).ConfigureAwait(false);

            Assert.That(receivedMessage, Is.Not.Null, "It appears as though the test transport message was not enqueued or was somehow removed before it could be dequeued.");
            Assert.That(receivedMessage!.Stream.ReadByte(), Is.EqualTo(100));
            Assert.That(await workTransport.ReceiveAsync(pipeline).ConfigureAwait(false), Is.Null);

            await workTransport.AcknowledgeAsync(receivedMessage.AcknowledgementToken, pipeline).ConfigureAwait(false);

            Assert.That(await workTransport.ReceiveAsync(pipeline).ConfigureAwait(false), Is.Null);

            await workTransport.TryDeleteAsync().ConfigureAwait(false);
        }
        finally
        {
            await workTransport.TryDisposeAsync().ConfigureAwait(false);
            await transportService.TryDisposeAsync().ConfigureAwait(false);
        }
    }

    protected async Task TestUnacknowledgedMessageAsync(IServiceCollection services, string transportUriFormat)
    {
        ConfigureServices(Guard.AgainstNull(services), 1, transportUriFormat);

        var serviceProvider = services.BuildServiceProvider();
        var transportService = serviceProvider.CreateTransportService();
        var pipeline = serviceProvider.CreatePipeline();

        var workTransport = await CreateWorkTransportAsync(transportService, transportUriFormat, true).ConfigureAwait(false);

        pipeline.State.SetTransportMessage(new() { MessageId = Guid.NewGuid() });

        await workTransport.SendAsync(new MemoryStream("message-body"u8.ToArray()), pipeline).ConfigureAwait(false);

        Assert.That(await workTransport.ReceiveAsync(pipeline).ConfigureAwait(false), Is.Not.Null);
        Assert.That(await workTransport.ReceiveAsync(pipeline).ConfigureAwait(false), Is.Null);

        await transportService.TryDisposeAsync().ConfigureAwait(false);

        serviceProvider = services.BuildServiceProvider();
        transportService = serviceProvider.CreateTransportService();
        pipeline = serviceProvider.CreatePipeline();

        workTransport = await CreateWorkTransportAsync(transportService, transportUriFormat, false).ConfigureAwait(false);

        var receivedMessage = await workTransport.ReceiveAsync(pipeline).ConfigureAwait(false);

        Assert.That(receivedMessage, Is.Not.Null);
        Assert.That(await workTransport.ReceiveAsync(pipeline).ConfigureAwait(false), Is.Null);

        await workTransport.AcknowledgeAsync(receivedMessage!.AcknowledgementToken, pipeline).ConfigureAwait(false);
        await workTransport.TryDisposeAsync().ConfigureAwait(false);

        workTransport = await CreateWorkTransportAsync(transportService, transportUriFormat, false).ConfigureAwait(false);

        Assert.That(await workTransport.ReceiveAsync(pipeline).ConfigureAwait(false), Is.Null);

        await workTransport.TryDeleteAsync().ConfigureAwait(false);

        await workTransport.TryDisposeAsync().ConfigureAwait(false);
        await transportService.TryDisposeAsync().ConfigureAwait(false);
    }
}