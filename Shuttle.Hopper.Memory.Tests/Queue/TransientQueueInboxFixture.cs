using NUnit.Framework;
using Shuttle.Hopper.Testing;

namespace Shuttle.Hopper.Memory.Tests;

public class TransientQueueInboxFixture : InboxFixture
{
    [TestCase(true)]
    [TestCase(false)]
    public async Task Should_be_able_handle_errors_async(bool hasErrorTransport)
    {
        await TestInboxErrorAsync(TransientQueueConfiguration.GetServiceCollection(), "transient-queue://./{0}", hasErrorTransport);
    }

    [Test]
    public async Task Should_be_able_to_expire_a_message_async()
    {
        await TestInboxExpiryAsync(TransientQueueConfiguration.GetServiceCollection(), "transient-queue://./{0}");
    }

    [Test]
    public async Task Should_be_able_to_handle_a_deferred_message_async()
    {
        await TestInboxDeferredAsync(TransientQueueConfiguration.GetServiceCollection(), "transient-queue://./{0}");
    }

    [Test]
    public async Task Should_be_able_to_process_messages_concurrently_async()
    {
        await TestInboxConcurrencyAsync(TransientQueueConfiguration.GetServiceCollection(), "transient-queue://./{0}", TimeSpan.FromSeconds(25), TimeSpan.FromSeconds(30));
    }

    [Test]
    public async Task Should_be_able_to_process_queue_timeously_async()
    {
        await TestInboxThroughputAsync(TransientQueueConfiguration.GetServiceCollection(), "transient-queue://./{0}", 1000, 5, TimeSpan.FromSeconds(5));
    }
}