namespace Shuttle.Hopper.Testing;

public class ErrorCommandHandler : IMessageHandler<ErrorCommand>
{
    public Task HandleAsync(ErrorCommand message, CancellationToken cancellationToken = default)
    {
        throw new ApplicationException("[testing exception handling]");
    }
}