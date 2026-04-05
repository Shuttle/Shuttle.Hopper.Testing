using Microsoft.Extensions.Logging;
using Shuttle.Core.Contract;
using System;
using Microsoft.Extensions.Logging.Abstractions;

namespace Shuttle.Hopper.Testing;

public class SimpleCommandHandler(ILogger<SimpleCommandHandler>? logger = null) : IMessageHandler<SimpleCommand>
{
    private readonly ILogger<SimpleCommandHandler> _logger = logger ?? NullLogger<SimpleCommandHandler>.Instance;

    public Task HandleAsync(SimpleCommand message, CancellationToken cancellationToken = default)
    {
        _logger.LogInformation($"[SimpleCommandHandler:SimpleCommand (thread {Environment.CurrentManagedThreadId})] : name = '{message.Name}' / context = '{message.Context}'");

        return Task.CompletedTask;
    }
}