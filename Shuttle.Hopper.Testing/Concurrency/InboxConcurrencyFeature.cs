using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Shuttle.Core.Contract;
using Shuttle.Core.Pipelines;

namespace Shuttle.Hopper.Testing;

public class InboxConcurrencyFeature : IPipelineObserver<MessageReceived>, IDisposable
{
    private readonly List<DateTime> _datesAfterGetMessage = [];
    private readonly SemaphoreSlim _lock = new(1, 1);
    private readonly ILogger<InboxConcurrencyFeature> _logger;
    private DateTime _firstDateAfterGetMessage = DateTime.MinValue;
    private readonly PipelineOptions _pipelineOptions;

    public InboxConcurrencyFeature(ILogger<InboxConcurrencyFeature> logger, IOptions<PipelineOptions> pipelineOptions)
    {
        _logger = Guard.AgainstNull(logger);
        _pipelineOptions = Guard.AgainstNull(Guard.AgainstNull(pipelineOptions).Value);

        _pipelineOptions.PipelineCreated += OnPipelineCreate;
    }

    private Task OnPipelineCreate(PipelineEventArgs eventArgs, CancellationToken cancellationToken)
    {
        if (eventArgs.Pipeline.GetType() == typeof(InboxMessagePipeline))
        {
            eventArgs.Pipeline.AddObserver(this);
        }

        return Task.CompletedTask;
    }

    public int OnAfterGetMessageCount => _datesAfterGetMessage.Count;

    public bool AllMessagesReceivedWithinTimespan(int msToComplete)
    {
        return
            _datesAfterGetMessage.All(dateTime => dateTime.Subtract(_firstDateAfterGetMessage) <=
                                                  TimeSpan.FromMilliseconds(msToComplete));
    }

    public async Task ExecuteAsync(IPipelineContext<MessageReceived> pipelineContext, CancellationToken cancellationToken = default)
    {
        await _lock.WaitAsync(cancellationToken);

        try
        {
            var dateTime = DateTime.Now;

            if (_firstDateAfterGetMessage == DateTime.MinValue)
            {
                _firstDateAfterGetMessage = DateTime.Now;

                _logger.LogInformation("Offset date: {0:yyyy-MM-dd HH:mm:ss.fff}", _firstDateAfterGetMessage);
            }

            _datesAfterGetMessage.Add(dateTime);

            _logger.LogInformation("Dequeued date: {0:yyyy-MM-dd HH:mm:ss.fff}", dateTime);
        }
        finally
        {
            _lock.Release();
        }

        await Task.CompletedTask.ConfigureAwait(false);
    }

    public void Dispose()
    {
        _pipelineOptions.PipelineCreated -= OnPipelineCreate;
    }
}