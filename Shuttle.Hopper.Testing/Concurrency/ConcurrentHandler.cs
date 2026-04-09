using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Shuttle.Contract;

namespace Shuttle.Hopper.Testing;

public class ConcurrentHandler(ILogger<ConcurrentHandler>? logger = null) : IMessageHandler<ConcurrentCommand>
{
    private readonly ILogger<ConcurrentHandler> _logger = logger ?? NullLogger<ConcurrentHandler>.Instance;

    public async Task HandleAsync(ConcurrentCommand message, CancellationToken cancellationToken = default)
    {
        _logger.LogInformation($"[ConcurrentHandler:ConcurrentCommand] : index = {message.MessageIndex}");

        await Task.Delay(500, cancellationToken).ConfigureAwait(false);
    }
}