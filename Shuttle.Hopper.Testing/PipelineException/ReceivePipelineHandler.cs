namespace Shuttle.Hopper.Testing;

public class ReceivePipelineHandler : IDirectMessageHandler<ReceivePipelineCommand>
{
    public Task ProcessMessageAsync(ReceivePipelineCommand message, CancellationToken cancellationToken = default)
    {
        return Task.CompletedTask;
    }
}