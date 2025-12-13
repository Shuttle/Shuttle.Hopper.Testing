using Microsoft.Extensions.Options;
using Shuttle.Core.Contract;

namespace Shuttle.Hopper.Testing;

public class TransientQueueFactory(IOptions<ServiceBusOptions> serviceBusOptions) : ITransportFactory
{
    private readonly ServiceBusOptions _serviceBusOptions = Guard.AgainstNull(Guard.AgainstNull(serviceBusOptions).Value);

    public string Scheme => TransientQueue.Scheme;

    public Task<ITransport> CreateAsync(Uri uri, CancellationToken cancellationToken = default)
    {
        return Task.FromResult<ITransport>(new TransientQueue(_serviceBusOptions, Guard.AgainstNull(uri)));
    }
}