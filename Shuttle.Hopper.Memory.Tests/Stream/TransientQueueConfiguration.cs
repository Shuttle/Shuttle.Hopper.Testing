using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Shuttle.Hopper.Testing;

namespace Shuttle.Hopper.Memory.Tests;

public static class TransientStreamConfiguration
{
    public static IServiceCollection GetServiceCollection()
    {
        var services = new ServiceCollection();

        services.AddSingleton<IConfiguration>(new ConfigurationBuilder().Build());

        services.AddTransientStreams();

        return services;
    }
}