using Microsoft.Extensions.Logging;

namespace Shuttle.Hopper.Testing;

public class ConsoleLoggerProvider : ILoggerProvider
{
    public ILogger CreateLogger(string categoryName)
    {
        return new ConsoleLogger();
    }

    public void Dispose()
    {
    }
}