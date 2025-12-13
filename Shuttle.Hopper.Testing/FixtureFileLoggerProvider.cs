using Microsoft.Extensions.Logging;

namespace Shuttle.Hopper.Testing;

public class FixtureFileLoggerProvider(string name) : ILoggerProvider
{
    private readonly FixtureFileLogger _logger = new(name);

    public void Dispose()
    {
        _logger.Dispose();
    }

    public ILogger CreateLogger(string categoryName)
    {
        return _logger;
    }
}