using Microsoft.Extensions.Options;
using Shuttle.Core.Contract;

namespace Shuttle.Hopper.Testing;

public class TransientQueueFactory(IOptions<HopperOptions> hopperOptions) : ITransportFactory
{
    private readonly HopperOptions _hopperOptions = Guard.AgainstNull(Guard.AgainstNull(hopperOptions).Value);

    public string Scheme => TransientQueue.Scheme;

    public Task<ITransport> CreateAsync(Uri uri, CancellationToken cancellationToken = default)
    {
        return Task.FromResult<ITransport>(new TransientQueue(_hopperOptions, Guard.AgainstNull(uri)));
    }
}