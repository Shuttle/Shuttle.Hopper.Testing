using Shuttle.Core.Contract;
using Shuttle.Core.Streams;

namespace Shuttle.Hopper.Testing;

public class TransientQueue : ITransport, ICreateTransport, IPurgeTransport
{
    internal const string Scheme = "transient-queue";

    private static readonly SemaphoreSlim Lock = new(1, 1);
    private static readonly Dictionary<string, Dictionary<int, TransientMessage>> Queues = new();
    private static int _itemId;
    private readonly ServiceBusOptions _serviceBusOptions;

    private readonly List<int> _unacknowledgedMessageIds = [];

    public TransientQueue(ServiceBusOptions serviceBusOptions, Uri uri)
    {
        Guard.AgainstNull(uri);

        if (!uri.Scheme.Equals(Scheme, StringComparison.InvariantCultureIgnoreCase))
        {
            throw new InvalidSchemeException(Scheme, uri.ToString());
        }

        _serviceBusOptions = Guard.AgainstNull(serviceBusOptions);

        var builder = new UriBuilder(uri);

        if (uri.Host.Equals("."))
        {
            builder.Host = Environment.MachineName.ToLower();
        }

        if (uri.LocalPath == "/")
        {
            builder.Path = "/default";
        }

        Uri = new(builder.Uri);

        if (Uri.Uri.Host != Environment.MachineName.ToLower())
        {
            throw new UriFormatException(string.Format(Resources.UriFormatException, $"memory://{{.|{Environment.MachineName.ToLower()}}}/{{name}}", uri));
        }

        CreateAsync().GetAwaiter().GetResult();
    }

    public async ValueTask<bool> HasPendingAsync(CancellationToken cancellationToken = default)
    {
        await Lock.WaitAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            return Queues[Uri.ToString()].Count > 0;
        }
        finally
        {
            Lock.Release();
        }
    }

    public async Task AcknowledgeAsync(object acknowledgementToken, CancellationToken cancellationToken = default)
    {
        var itemId = (int)acknowledgementToken;

        await Lock.WaitAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            var queue = Queues[Uri.ToString()];

            if (!queue.ContainsKey(itemId) || !_unacknowledgedMessageIds.Contains(itemId))
            {
                return;
            }

            queue.Remove(itemId);

            _unacknowledgedMessageIds.Remove(itemId);
        }
        finally
        {
            Lock.Release();
        }

        await _serviceBusOptions.MessageAcknowledged.InvokeAsync(new(this, acknowledgementToken), cancellationToken);
    }

    public async Task<ReceivedMessage?> ReceiveAsync(CancellationToken cancellationToken = default)
    {
        ReceivedMessage? result = null;

        await Lock.WaitAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            foreach (var candidate in Queues)
            {
                foreach (var itemId in candidate.Value.Values.Where(item => item.TransportMessage.ExpiryDateTime <= DateTimeOffset.UtcNow).Select(item => item.ItemId))
                {
                    candidate.Value.Remove(itemId);
                }
            }

            var queue = Queues[Uri.ToString()];

            var index = 0;

            while (index < queue.Count)
            {
                var pair = queue.ElementAt(index);

                if (!_unacknowledgedMessageIds.Contains(pair.Value.ItemId))
                {
                    _unacknowledgedMessageIds.Add(pair.Value.ItemId);

                    result = new(pair.Value.Stream, pair.Value.ItemId);

                    break;
                }

                index++;
            }
        }
        finally
        {
            Lock.Release();
        }

        if (result != null)
        {
            await _serviceBusOptions.MessageReceived.InvokeAsync(new(this, result), cancellationToken);
        }

        return result;
    }

    public async Task ReleaseAsync(object acknowledgementToken, CancellationToken cancellationToken = default)
    {
        var itemId = (int)acknowledgementToken;

        await Lock.WaitAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            var queue = Queues[Uri.ToString()];

            if (!queue.ContainsKey(itemId) || !_unacknowledgedMessageIds.Contains(itemId))
            {
                return;
            }

            if (queue.ContainsKey(itemId))
            {
                var message = queue[itemId];

                queue.Remove(itemId);

                queue.Add(itemId, message);
            }

            _unacknowledgedMessageIds.Remove(itemId);
        }
        finally
        {
            Lock.Release();
        }

        await _serviceBusOptions.MessageReleased.InvokeAsync(new(this, acknowledgementToken), cancellationToken);
    }

    public async Task SendAsync(TransportMessage transportMessage, Stream stream, CancellationToken cancellationToken = default)
    {
        await Lock.WaitAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            _itemId++;

            Queues[Uri.ToString()].Add(_itemId, new(_itemId, transportMessage, await stream.CopyAsync().ConfigureAwait(false)));
        }
        finally
        {
            Lock.Release();
        }

        await _serviceBusOptions.MessageSent.InvokeAsync(new(this, transportMessage, stream), cancellationToken);
    }

    public TransportType Type { get; } = TransportType.Queue;
    public TransportUri Uri { get; }

    public async Task CreateAsync(CancellationToken cancellationToken = default)
    {
        await _serviceBusOptions.TransportOperation.InvokeAsync(new(this, "[create/starting]"), cancellationToken);

        await Lock.WaitAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            if (!Queues.ContainsKey(Uri.ToString()))
            {
                Queues.Add(Uri.ToString(), new());
            }
        }
        finally
        {
            Lock.Release();
        }

        await _serviceBusOptions.TransportOperation.InvokeAsync(new(this, "[create/completed]"), cancellationToken);
    }

    public async Task PurgeAsync(CancellationToken cancellationToken = default)
    {
        await _serviceBusOptions.TransportOperation.InvokeAsync(new(this, "[purge/starting]"), cancellationToken);

        await Lock.WaitAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            Queues[Uri.ToString()].Clear();
        }
        finally
        {
            Lock.Release();
        }

        await _serviceBusOptions.TransportOperation.InvokeAsync(new(this, "[purge/completed]"), cancellationToken);
    }
}