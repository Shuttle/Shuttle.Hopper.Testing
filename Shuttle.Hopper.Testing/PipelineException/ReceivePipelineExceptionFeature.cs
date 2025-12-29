using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NUnit.Framework;
using Shuttle.Core.Contract;
using Shuttle.Core.Pipelines;
using Shuttle.Core.Reflection;
using Shuttle.Core.Threading;

namespace Shuttle.Hopper.Testing;

public class ReceivePipelineExceptionFeature :
    IPipelineObserver<ReceiveMessage>,
    IPipelineObserver<MessageReceived>,
    IPipelineObserver<DeserializeTransportMessage>,
    IPipelineObserver<TransportMessageDeserialized>,
    IDisposable
{
    private static readonly SemaphoreSlim Lock = new(1, 1);

    private readonly List<ExceptionAssertion> _assertions = [];
    private readonly ILogger<ReceivePipelineExceptionFeature> _logger;
    private readonly PipelineOptions _pipelineOptions;
    private readonly IServiceBusConfiguration _serviceBusConfiguration;
    private readonly ThreadingOptions _threadingOptions;
    private string _assertionName = string.Empty;
    private volatile bool _failed;

    public ReceivePipelineExceptionFeature(ILogger<ReceivePipelineExceptionFeature> logger, IOptions<ThreadingOptions> threadingOptions, IOptions<PipelineOptions> pipelineOptions, IServiceBusConfiguration serviceBusConfiguration)
    {
        _logger = Guard.AgainstNull(logger);
        _pipelineOptions = Guard.AgainstNull(Guard.AgainstNull(pipelineOptions).Value);
        _threadingOptions = Guard.AgainstNull(Guard.AgainstNull(threadingOptions).Value);
        _serviceBusConfiguration = Guard.AgainstNull(serviceBusConfiguration);

        _pipelineOptions.PipelineCreated += PipelineCreated;

        _threadingOptions.ProcessorExecuted += ProcessorExecuted;

        AddAssertion("ReceiveMessage");
        AddAssertion("MessageReceived");
        AddAssertion("DeserializeTransportMessage");
        AddAssertion("TransportMessageDeserialized");
    }

    public void Dispose()
    {
        _pipelineOptions.PipelineCreated -= PipelineCreated;
        _threadingOptions.ProcessorExecuted -= ProcessorExecuted;
    }

    public async Task ExecuteAsync(IPipelineContext<DeserializeTransportMessage> pipelineContext, CancellationToken cancellationToken = default)
    {
        ThrowException("DeserializeTransportMessage");

        await Task.CompletedTask.ConfigureAwait(false);
    }

    public async Task ExecuteAsync(IPipelineContext<MessageReceived> pipelineContext, CancellationToken cancellationToken = default)
    {
        ThrowException("MessageReceived");

        await Task.CompletedTask.ConfigureAwait(false);
    }

    public async Task ExecuteAsync(IPipelineContext<ReceiveMessage> pipelineContext, CancellationToken cancellationToken = default)
    {
        ThrowException("ReceiveMessage");

        await Task.CompletedTask.ConfigureAwait(false);
    }

    public async Task ExecuteAsync(IPipelineContext<TransportMessageDeserialized> pipelineContext, CancellationToken cancellationToken = default)
    {
        ThrowException("TransportMessageDeserialized");

        await Task.CompletedTask.ConfigureAwait(false);
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

    private ExceptionAssertion? GetAssertion(string name)
    {
        return _assertions.Find(item => item.Name.Equals(name, StringComparison.InvariantCultureIgnoreCase));
    }

    private Task PipelineCreated(PipelineEventArgs eventArgs, CancellationToken cancellationToken)
    {
        if (eventArgs.Pipeline.GetType() == typeof(InboxMessagePipeline))
        {
            eventArgs.Pipeline.AddObserver(this);
        }

        return Task.CompletedTask;
    }

    private async Task ProcessorExecuted(ProcessorExecutedEventArgs eventArgs, CancellationToken cancellationToken)
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
}