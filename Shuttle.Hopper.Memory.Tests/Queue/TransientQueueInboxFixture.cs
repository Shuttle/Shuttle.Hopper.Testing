using NUnit.Framework;
using Shuttle.Hopper.Testing;

namespace Shuttle.Hopper.Memory.Tests;

public class TransientQueueInboxFixture : InboxFixture
{
    [TestCase(true, true)]
    [TestCase(true, false)]
    [TestCase(false, true)]
    [TestCase(false, false)]
    public async Task Should_be_able_handle_errors_async(bool hasErrorTransport, bool isTransactional)
    {
        await TestInboxErrorAsync(TransientQueueConfiguration.GetServiceCollection(), "transient-queue://./{0}", hasErrorTransport, isTransactional);
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

    [TestCase(250, false)]
    [TestCase(250, true)]
    public async Task Should_be_able_to_process_messages_concurrently_async(int msToComplete, bool isTransactional)
    {
        await TestInboxConcurrencyAsync(TransientQueueConfiguration.GetServiceCollection(), "transient-queue://./{0}", msToComplete, isTransactional);
    }

    [TestCase(100, true)]
    [TestCase(100, false)]
    public async Task Should_be_able_to_process_queue_timeously_async(int count, bool isTransactional)
    {
        await TestInboxThroughputAsync(TransientQueueConfiguration.GetServiceCollection(), "transient-queue://./{0}", 1000, count, 5, isTransactional);
    }
}