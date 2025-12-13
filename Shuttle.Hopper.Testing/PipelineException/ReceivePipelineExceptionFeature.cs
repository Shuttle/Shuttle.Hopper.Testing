using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NUnit.Framework;
using Shuttle.Core.Contract;
using Shuttle.Core.Pipelines;
using Shuttle.Core.Reflection;

namespace Shuttle.Hopper.Testing;

public class ReceivePipelineExceptionFeature :
    IPipelineObserver<ReceiveMessage>,
    IPipelineObserver<MessageReceived>,
    IPipelineObserver<DeserializeTransportMessage>,
    IPipelineObserver<TransportMessageDeserialized>,
    IDisposable
{
    private readonly PipelineOptions _pipelineOptions;
    private static readonly SemaphoreSlim Lock = new(1, 1);

    private readonly List<ExceptionAssertion> _assertions = [];
    private readonly ILogger<ReceivePipelineExceptionFeature> _logger;
    private readonly IServiceBusConfiguration _serviceBusConfiguration;
    private string _assertionName = string.Empty;
    private volatile bool _failed;
    private int _pipelineCount;

    public ReceivePipelineExceptionFeature(ILogger<ReceivePipelineExceptionFeature> logger, IOptions<PipelineOptions> pipelineOptions, IServiceBusConfiguration serviceBusConfiguration)
    {
        _pipelineOptions = Guard.AgainstNull(Guard.AgainstNull(pipelineOptions).Value);
        
        _pipelineOptions.PipelineCreated += OnPipelineCreated;
        _pipelineOptions.PipelineReleased += OnPipelineReleased;
        _pipelineOptions.PipelineObtained += OnPipelineObtained;

        _logger = Guard.AgainstNull(logger);
        _serviceBusConfiguration = Guard.AgainstNull(serviceBusConfiguration);

        AddAssertion("OnGetMessage");
        AddAssertion("OnAfterGetMessage");
        AddAssertion("OnDeserializeTransportMessage");
        AddAssertion("OnAfterDeserializeTransportMessage");
    }

    private Task OnPipelineObtained(PipelineEventArgs eventArgs, CancellationToken cancellationToken)
    {
        _pipelineCount += 1;
        _assertionName = string.Empty;

        _logger.LogInformation("[ReceivePipelineExceptionModule:PipelineObtained] : count = {PipelineCount}", _pipelineCount);

        return Task.CompletedTask;
    }

    private async Task OnPipelineReleased(PipelineEventArgs eventArgs, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(_assertionName))
        {
            return;
        }

        await Lock.WaitAsync(cancellationToken);

        try
        {
            var assertion = Guard.AgainstNull(GetAssertion(_assertionName));

            if (assertion.HasRun)
            {
                return;
            }

            _logger.LogInformation($"[ReceivePipelineExceptionModule:Invoking] : assertion = '{assertion.Name}'.");

            try
            {
                var receivedMessage = await _serviceBusConfiguration.Inbox!.WorkTransport!.ReceiveAsync(cancellationToken);

                Assert.That(receivedMessage, Is.Not.Null);

                await _serviceBusConfiguration.Inbox.WorkTransport.ReleaseAsync(receivedMessage!.AcknowledgementToken, cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogInformation(ex.AllMessages());

                _failed = true;
            }

            assertion.MarkAsRun();

            _logger.LogInformation("[ReceivePipelineExceptionModule:Invoked] : assertion = '{AssertionName}'.", assertion.Name);
        }
        finally
        {
            Lock.Release();
        }
    }

    private Task OnPipelineCreated(PipelineEventArgs eventArgs, CancellationToken cancellationToken)
    {
        if (eventArgs.Pipeline.GetType() == typeof(InboxMessagePipeline))
        {
            eventArgs.Pipeline.AddObserver(this);
        }

        return Task.CompletedTask;
    }

    private void AddAssertion(string name)
    {
        Lock.Wait();

        try
        {
            _assertions.Add(new(name));

            _logger.LogInformation($"[ReceivePipelineExceptionModule:Added] : assertion = '{name}'.");
        }
        finally
        {
            Lock.Release();
        }
    }

    public async Task ExecuteAsync(IPipelineContext<TransportMessageDeserialized> pipelineContext, CancellationToken cancellationToken = default)
    {
        ThrowException("OnAfterDeserializeTransportMessage");

        await Task.CompletedTask.ConfigureAwait(false);
    }

    public async Task ExecuteAsync(IPipelineContext<MessageReceived> pipelineContext, CancellationToken cancellationToken = default)
    {
        ThrowException("OnAfterGetMessage");

        await Task.CompletedTask.ConfigureAwait(false);
    }

    public async Task ExecuteAsync(IPipelineContext<DeserializeTransportMessage> pipelineContext, CancellationToken cancellationToken = default)
    {
        ThrowException("OnDeserializeTransportMessage");

        await Task.CompletedTask.ConfigureAwait(false);
    }

    public async Task ExecuteAsync(IPipelineContext<ReceiveMessage> pipelineContext, CancellationToken cancellationToken = default)
    {
        ThrowException("OnGetMessage");

        await Task.CompletedTask.ConfigureAwait(false);
    }

    private ExceptionAssertion? GetAssertion(string name)
    {
        return _assertions.Find(item => item.Name.Equals(name, StringComparison.InvariantCultureIgnoreCase));
    }

    public bool ShouldWait()
    {
        Lock.Wait();

        try
        {
            return !_failed && _assertions.Find(item => !item.HasRun) != null;
        }
        finally
        {
            Lock.Release();
        }
    }

    private void ThrowException(string name)
    {
        Lock.Wait();

        try
        {
            _assertionName = name;

            var assertion = Guard.AgainstNull(GetAssertion(_assertionName));

            if (assertion.HasRun)
            {
                return;
            }

            throw new AssertionException($"Testing assertion for '{name}'.");
        }
        finally
        {
            Lock.Release();
        }
    }

    public void Dispose()
    {
        _pipelineOptions.PipelineCreated -= OnPipelineCreated;
        _pipelineOptions.PipelineReleased -= OnPipelineReleased;
        _pipelineOptions.PipelineObtained -= OnPipelineObtained;
    }
}