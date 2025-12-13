using Shuttle.Core.Contract;
using Microsoft.Extensions.Options;

namespace Shuttle.Hopper.Testing;

public class TransientStreamFactory(IOptions<ServiceBusOptions> serviceBusOptions) : ITransportFactory
{
    public Task<ITransport> CreateAsync(Uri uri, CancellationToken cancellationToken = new CancellationToken())
    {
        return Task.FromResult<ITransport>(new TransientStream(Guard.AgainstNull(Guard.AgainstNull(serviceBusOptions).Value), Guard.AgainstNull(uri)));
    }

    public string Scheme => TransientStream.Scheme;
}