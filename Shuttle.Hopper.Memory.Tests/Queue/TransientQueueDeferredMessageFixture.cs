using NUnit.Framework;
using Shuttle.Hopper.Testing;

namespace Shuttle.Hopper.Memory.Tests;

public class TransientQueueDeferredMessageFixture : DeferredFixture
{
    [Test]
    [TestCase(false)]
    [TestCase(true)]
    public async Task Should_be_able_to_perform_full_processing_async(bool isTransactional)
    {
        await TestDeferredProcessingAsync(TransientQueueConfiguration.GetServiceCollection(), "transient-queue://./{0}", isTransactional);
    }
}