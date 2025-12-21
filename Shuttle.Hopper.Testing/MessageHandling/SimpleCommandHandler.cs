using Microsoft.Extensions.Logging;
using Shuttle.Core.Contract;
using System;

namespace Shuttle.Hopper.Testing;

public class SimpleCommandHandler(ILogger<SimpleCommandHandler> logger) : IDirectMessageHandler<SimpleCommand>
{
    private readonly ILogger<SimpleCommandHandler> _logger = Guard.AgainstNull(logger);

    public Task ProcessMessageAsync(SimpleCommand message, CancellationToken cancellationToken = default)
    {
        _logger.LogInformation($"[SimpleCommandHandler:SimpleCommand (thread {Environment.CurrentManagedThreadId})] : name = '{message.Name}' / context = '{message.Context}'");

        return Task.CompletedTask;
    }
}