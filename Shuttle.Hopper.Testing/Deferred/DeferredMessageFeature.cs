using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Shuttle.Core.Contract;
using Shuttle.Core.Pipelines;
using System.Threading;

namespace Shuttle.Hopper.Testing;

public class DeferredMessageFeature :
    IPipelineObserver<MessageHandled>,
    IPipelineObserver<DeferredMessageProcessed>,
    IDisposable
{
    private readonly PipelineOptions _pipelineOptions;
    private readonly int _deferredMessageCount;
    private readonly SemaphoreSlim _lock = new(1, 1);
    private readonly ILogger<DeferredMessageFeature> _logger;

    public DeferredMessageFeature(ILogger<DeferredMessageFeature> logger, IOptions<MessageCountOptions> messageCountOptions, IOptions<PipelineOptions> pipelineOptions)
    {
        _logger = Guard.AgainstNull(logger);
        _deferredMessageCount = Guard.AgainstNull(Guard.AgainstNull(messageCountOptions).Value).MessageCount;
        _pipelineOptions = Guard.AgainstNull(Guard.AgainstNull(pipelineOptions).Value);

        _pipelineOptions.PipelineCreated += OnPipelineCreated;
    }

    private Task OnPipelineCreated(PipelineEventArgs eventArgs, CancellationToken cancellationToken)
    {
        if (eventArgs.Pipeline.GetType() == typeof(InboxMessagePipeline) || eventArgs.Pipeline.GetType() == typeof(DeferredMessagePipeline))
        {
            eventArgs.Pipeline.AddObserver(this);
        }

        return Task.CompletedTask;
    }

    public int NumberOfDeferredMessagesReturned { get; private set; }
    public int NumberOfMessagesHandled { get; private set; }

    public async ValueTask<bool> HasPendingDeferredMessagesAsync()
    {
        await _lock.WaitAsync();

        try
        {
            return NumberOfMessagesHandled < _deferredMessageCount;
        }
        finally
        {
            _lock.Release();
        }
    }

    public async Task ExecuteAsync(IPipelineContext<MessageHandled> pipelineContext, CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("[OnAfterHandleMessage]");

        await _lock.WaitAsync(cancellationToken);

        try
        {
            NumberOfMessagesHandled++;
        }
        finally
        {
            _lock.Release();
        }

        await Task.CompletedTask.ConfigureAwait(false);
    }

    public async Task ExecuteAsync(IPipelineContext<DeferredMessageProcessed> pipelineContext, CancellationToken cancellationToken = default)
    {
        _logger.LogInformation($"[OnAfterProcessDeferredMessage] : deferred message returned = '{pipelineContext.Pipeline.State.GetDeferredMessageReturned()}'");

        if (!pipelineContext.Pipeline.State.GetDeferredMessageReturned())
        {
            return;
        }

        await _lock.WaitAsync(cancellationToken);

        try
        {
            NumberOfDeferredMessagesReturned++;
        }
        finally
        {
            _lock.Release();
        }

        await Task.CompletedTask.ConfigureAwait(false);
    }

    public void Dispose()
    {
        _pipelineOptions.PipelineCreated -= OnPipelineCreated;
    }
}