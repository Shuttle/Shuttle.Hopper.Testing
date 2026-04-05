using NUnit.Framework;
using Shuttle.Hopper.Testing;

namespace Shuttle.Hopper.Memory.Tests;

public class TransientStreamInboxFixture : InboxFixture
{
    [TestCase(true)]
    [TestCase(false)]
    public async Task Should_be_able_handle_errors_async(bool hasErrorTransport)
    {
        await TestInboxErrorAsync(TransientStreamConfiguration.GetServiceCollection(), "transient-stream://./{0}", hasErrorTransport);
    }

    [Test]
    public async Task Should_be_able_to_process_queue_timeously_async()
    {
        await TestInboxThroughputAsync(TransientStreamConfiguration.GetServiceCollection(), "transient-stream://./{0}", 1000, 10);
    }
}