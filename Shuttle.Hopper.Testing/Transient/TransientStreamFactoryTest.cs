using Microsoft.Extensions.Options;
using NUnit.Framework;

namespace Shuttle.Hopper.Testing;

[TestFixture]
public class TransientStreamFactoryTest
{
    [Test]
    public async Task Should_be_able_to_create_a_new_queue_from_a_given_uri_async()
    {
        Assert.That(await new TransientStreamFactory(Options.Create(new HopperOptions())).CreateAsync(new("transient-stream://./input")), Is.Not.Null);
    }
}