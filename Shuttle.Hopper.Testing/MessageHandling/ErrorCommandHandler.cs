namespace Shuttle.Hopper.Testing;

public class ErrorCommandHandler : IDirectMessageHandler<ErrorCommand>
{
    public Task ProcessMessageAsync(ErrorCommand message, CancellationToken cancellationToken = default)
    {
        throw new ApplicationException("[testing exception handling]");
    }
}