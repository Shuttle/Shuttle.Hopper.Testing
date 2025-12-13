using Shuttle.Core.Contract;

namespace Shuttle.Hopper.Testing;

public class SimpleCommand(string name) : object
{
    public SimpleCommand()
        : this(Guard.AgainstEmpty(typeof(SimpleCommand).FullName))
    {
    }

    public string Context { get; set; } = string.Empty;

    public string Name { get; set; } = name;
}