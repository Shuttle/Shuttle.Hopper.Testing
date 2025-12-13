using Microsoft.Extensions.DependencyInjection;
using NUnit.Framework;
using Shuttle.Core.Contract;
using Shuttle.Core.Reflection;
using Shuttle.Core.TransactionScope;

namespace Shuttle.Hopper.Testing;

public class BasicTransportFixture : IntegrationFixture
{
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
                    ErrorTransportUri = new(string.Format(transportUriFormat, "test-error")),
                    DurationToSleepWhenIdle = [TimeSpan.FromMilliseconds(25)],
                    DurationToIgnoreOnFailure = [TimeSpan.FromMilliseconds(25)],
                    ThreadCount = threadCount
                }
            };
        });

        services.ConfigureLogging(test);
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
        ConfigureServices(Guard.AgainstNull(services), nameof(TestReleaseMessageAsync), 1, false, transportUriFormat);

        var serviceProvider = services.BuildServiceProvider();
        var transportService = serviceProvider.CreateTransportService();
        var workTransport = await CreateWorkTransportAsync(transportService, transportUriFormat, true).ConfigureAwait(false);

        try
        {
            await workTransport.SendAsync(new() { MessageId = Guid.NewGuid() }, new MemoryStream("message-body"u8.ToArray())).ConfigureAwait(false);

            var receivedMessage = await workTransport.ReceiveAsync().ConfigureAwait(false);

            Assert.That(receivedMessage, Is.Not.Null);
            Assert.That(await workTransport.ReceiveAsync().ConfigureAwait(false), Is.Null);

            await workTransport.ReleaseAsync(receivedMessage!.AcknowledgementToken).ConfigureAwait(false);

            receivedMessage = await workTransport.ReceiveAsync().ConfigureAwait(false);

            Assert.That(receivedMessage, Is.Not.Null);
            Assert.That(await workTransport.ReceiveAsync().ConfigureAwait(false), Is.Null);

            await workTransport.AcknowledgeAsync(receivedMessage!.AcknowledgementToken).ConfigureAwait(false);

            Assert.That(await workTransport.ReceiveAsync().ConfigureAwait(false), Is.Null);

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
        ConfigureServices(Guard.AgainstNull(services), nameof(TestSimpleSendAndReceiveAsync), 1, false, transportUriFormat);

        var serviceProvider = services.BuildServiceProvider();
        var transportService = serviceProvider.CreateTransportService();
        var workTransport = await CreateWorkTransportAsync(transportService, transportUriFormat, true).ConfigureAwait(false);

        try
        {
            var stream = new MemoryStream();

            stream.WriteByte(100);

            await workTransport.SendAsync(new()
            {
                MessageId = Guid.NewGuid()
            }, stream).ConfigureAwait(false);

            var receivedMessage = await workTransport.ReceiveAsync().ConfigureAwait(false);

            Assert.That(receivedMessage, Is.Not.Null, "It appears as though the test transport message was not enqueued or was somehow removed before it could be dequeued.");
            Assert.That(receivedMessage!.Stream.ReadByte(), Is.EqualTo(100));
            Assert.That(await workTransport.ReceiveAsync().ConfigureAwait(false), Is.Null);

            await workTransport.AcknowledgeAsync(receivedMessage.AcknowledgementToken).ConfigureAwait(false);

            Assert.That(await workTransport.ReceiveAsync().ConfigureAwait(false), Is.Null);

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
        ConfigureServices(Guard.AgainstNull(services), nameof(TestUnacknowledgedMessageAsync), 1, false, transportUriFormat);

        var transportService = services.BuildServiceProvider().CreateTransportService();

        var workTransport = await CreateWorkTransportAsync(transportService, transportUriFormat, true).ConfigureAwait(false);

        await workTransport.SendAsync(new()
        {
            MessageId = Guid.NewGuid()
        }, new MemoryStream("message-body"u8.ToArray())).ConfigureAwait(false);

        Assert.That(await workTransport.ReceiveAsync().ConfigureAwait(false), Is.Not.Null);
        Assert.That(await workTransport.ReceiveAsync().ConfigureAwait(false), Is.Null);

        await transportService.TryDisposeAsync().ConfigureAwait(false);
        transportService = services.BuildServiceProvider().CreateTransportService();

        workTransport = await CreateWorkTransportAsync(transportService, transportUriFormat, false).ConfigureAwait(false);

        var receivedMessage = await workTransport.ReceiveAsync().ConfigureAwait(false);

        Assert.That(receivedMessage, Is.Not.Null);
        Assert.That(await workTransport.ReceiveAsync().ConfigureAwait(false), Is.Null);

        await workTransport.AcknowledgeAsync(receivedMessage!.AcknowledgementToken).ConfigureAwait(false);
        await workTransport.TryDisposeAsync().ConfigureAwait(false);

        workTransport = await CreateWorkTransportAsync(transportService, transportUriFormat, false).ConfigureAwait(false);

        Assert.That(await workTransport.ReceiveAsync().ConfigureAwait(false), Is.Null);

        await workTransport.TryDeleteAsync().ConfigureAwait(false);

        await workTransport.TryDisposeAsync().ConfigureAwait(false);
        await transportService.TryDisposeAsync().ConfigureAwait(false);
    }
}