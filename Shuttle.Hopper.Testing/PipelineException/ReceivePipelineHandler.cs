namespace Shuttle.Hopper.Testing;

public class ReceivePipelineHandler : IMessageHandler<ReceivePipelineCommand>
{
    public Task HandleAsync(ReceivePipelineCommand message, CancellationToken cancellationToken = default)
    {
        return Task.CompletedTask;
    }
}