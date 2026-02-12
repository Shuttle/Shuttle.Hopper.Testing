using Microsoft.Extensions.Logging;
using Shuttle.Core.Contract;

namespace Shuttle.Hopper.Testing;

public class ConcurrentHandler(ILogger<ConcurrentHandler> logger) : IMessageHandler<ConcurrentCommand>
{
    private readonly ILogger<ConcurrentHandler> _logger = Guard.AgainstNull(logger);

    public async Task ProcessMessageAsync(ConcurrentCommand message, CancellationToken cancellationToken = default)
    {
        _logger.LogInformation($"[ConcurrentHandler:ConcurrentCommand] : index = {message.MessageIndex}");

        await Task.Delay(500, cancellationToken).ConfigureAwait(false);
    }
}