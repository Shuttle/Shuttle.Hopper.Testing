using Shuttle.Core.Contract;

namespace Shuttle.Hopper.Testing;

public static class TransportServiceExtensions
{
    private static readonly List<string> TransportUris =
    [
        "test-worker-work",
        "test-distributor-work",
        "test-distributor-control",
        "test-inbox-work",
        "test-inbox-deferred",
        "test-outbox-work",
        "test-error"
    ];

    extension(ITransportService transportService)
    {
        public async Task TryDeleteTransportsAsync(string transportUriFormat)
        {
            Guard.AgainstNull(transportService);
            Guard.AgainstEmpty(transportUriFormat);

            foreach (var transportUri in TransportUris)
            {
                var uri = string.Format(transportUriFormat, transportUri);

                if (!await transportService.ContainsAsync(uri).ConfigureAwait(false))
                {
                    continue;
                }

                await (await transportService.GetAsync(uri).ConfigureAwait(false)).TryDeleteAsync().ConfigureAwait(false);
            }
        }
    }
}