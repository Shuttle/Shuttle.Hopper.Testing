using NUnit.Framework;
using Shuttle.Hopper.Testing;

namespace Shuttle.Hopper.Memory.Tests;

[TestFixture]
public class TransientQueueFixture : BasicTransportFixture
{
    [Test]
    public void Should_be_able_to_create_a_new_queue_from_a_given_uri()
    {
        Assert.That(new TransientQueue(new(), new("transient-queue://./input-queue")).Uri.ToString(), Is.EqualTo($"transient-queue://{Environment.MachineName.ToLower()}/input-queue"));
    }

    [Test]
    public void Should_use_default_queue_name_for_empty_local_path()
    {
        Assert.That(new TransientQueue(new(), new("transient-queue://.")).Uri.ToString(), Is.EqualTo($"transient-queue://{Environment.MachineName.ToLower()}/default"));
    }

    [Test]
    public void Should_throw_exception_when_trying_to_create_a_queue_with_incorrect_scheme()
    {
        Assert.Throws<InvalidSchemeException>(() => _ = new TransientQueue(new(), new("sql://./inputqueue")));
    }

    [Test]
    public void Should_throw_exception_when_trying_to_create_a_queue_with_incorrect_format()
    {
        Assert.Throws<UriFormatException>(() => _ = new TransientQueue(new(), new("transient-queue://notthismachine")));
    }

    [Test]
    public async Task Should_be_able_to_perform_simple_send_and_receive_async()
    {
        await TestSimpleSendAndReceiveAsync(TransientQueueConfiguration.GetServiceCollection(), "transient-queue://./{0}");
        await TestSimpleSendAndReceiveAsync(TransientQueueConfiguration.GetServiceCollection(), "transient-queue://./{0}-transient");
    }

    [Test]
    public async Task Should_be_able_to_release_a_message_async()
    {
        await TestReleaseMessageAsync(TransientQueueConfiguration.GetServiceCollection(), "transient-queue://./{0}");
    }

    [Test]
    public async Task Should_be_able_to_get_message_again_when_not_acknowledged_before_queue_is_disposed_async()
    {
        await TestUnacknowledgedMessageAsync(TransientQueueConfiguration.GetServiceCollection(), "transient-queue://./{0}");
    }
}