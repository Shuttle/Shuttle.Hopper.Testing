using Shuttle.Core.Contract;

namespace Shuttle.Hopper.Testing;

internal class TransientMessage(int index, TransportMessage transportMessage, Stream stream)
{
    public int ItemId { get; } = index;
    public Stream Stream { get; } = Guard.AgainstNull(stream);
    public TransportMessage TransportMessage { get; } = Guard.AgainstNull(transportMessage);
}