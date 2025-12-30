using Shuttle.Core.Contract;
using Microsoft.Extensions.Options;

namespace Shuttle.Hopper.Testing;

public class TransientStreamFactory(IOptions<HopperOptions> hopperOptions) : ITransportFactory
{
    public Task<ITransport> CreateAsync(Uri uri, CancellationToken cancellationToken = default)
    {
        return Task.FromResult<ITransport>(new TransientStream(Guard.AgainstNull(Guard.AgainstNull(hopperOptions).Value), Guard.AgainstNull(uri)));
    }

    public string Scheme => TransientStream.Scheme;
}