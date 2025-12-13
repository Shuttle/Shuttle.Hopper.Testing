namespace Shuttle.Hopper.Testing;

public class ReceivePipelineHandler : IMessageHandler<ReceivePipelineCommand>
{
    public Task ProcessMessageAsync(ReceivePipelineCommand message, CancellationToken cancellationToken = default)
    {
        return Task.CompletedTask;
    }
}