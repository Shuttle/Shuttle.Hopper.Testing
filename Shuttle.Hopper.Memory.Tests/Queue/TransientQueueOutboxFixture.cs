using NUnit.Framework;
using Shuttle.Hopper.Testing;

namespace Shuttle.Hopper.Memory.Tests;

public class TransientQueueOutboxFixture : OutboxFixture
{
    [TestCase(true)]
    [TestCase(false)]
    public async Task Should_be_able_to_use_outbox_async(bool isTransactionalEndpoint)
    {
        await TestOutboxSendingAsync(TransientQueueConfiguration.GetServiceCollection(), "transient-queue://./{0}", 3, isTransactionalEndpoint);
    }
}