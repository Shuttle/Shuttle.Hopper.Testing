using Microsoft.Extensions.Options;
using Shuttle.Core.Contract;
using Shuttle.Core.Pipelines;

namespace Shuttle.Hopper.Testing;

public class InboxDeferredFeature :
    IPipelineObserver<TransportMessageDeserialized>,
    IDisposable
{
    private readonly PipelineOptions _pipelineOptions;

    public InboxDeferredFeature(IOptions<PipelineOptions> pipelineOptions)
    {
        _pipelineOptions = Guard.AgainstNull(Guard.AgainstNull(pipelineOptions).Value);
        
        _pipelineOptions.PipelineCreated += OnPipelineCreated;
    }

    private Task OnPipelineCreated(PipelineEventArgs eventArgs, CancellationToken cancellationToken)
    {
        if (eventArgs.Pipeline.GetType() == typeof(InboxMessagePipeline))
        {
            eventArgs.Pipeline.AddObserver(this);
        }

        return Task.CompletedTask;
    }

    public TransportMessage? TransportMessage { get; private set; }

    public async Task ExecuteAsync(IPipelineContext<TransportMessageDeserialized> pipelineContext, CancellationToken cancellationToken = default)
    {
        TransportMessage = Guard.AgainstNull(Guard.AgainstNull(pipelineContext).Pipeline.State.GetTransportMessage());

        await Task.CompletedTask.ConfigureAwait(false);
    }

    public void Dispose()
    {
        _pipelineOptions.PipelineCreated -= OnPipelineCreated;
    }
}