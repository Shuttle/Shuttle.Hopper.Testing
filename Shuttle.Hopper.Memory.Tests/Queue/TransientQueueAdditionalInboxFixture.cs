using NUnit.Framework;
using Shuttle.Hopper.Testing;

namespace Shuttle.Hopper.Memory.Tests;

public class TransientQueueAdditionalInboxFixture : AdditionalInboxFixture
{
    [TestCase(false)]
    [TestCase(true)]
    public async Task Should_be_able_to_process_messages_from_an_additional_inbox_async(bool useOwnDeferredTransport)
    {
        await TestAdditionalInboxAsync(TransientQueueConfiguration.GetServiceCollection(), "transient-queue://./{0}", useOwnDeferredTransport);
    }
}
