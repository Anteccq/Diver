using System;
using Microsoft.Extensions.DependencyInjection;

namespace Diver.Client;

internal sealed class ViewModelResolver
{
    private readonly IServiceProvider _serviceProvider;

    public static readonly ViewModelResolver Resolver = new();

    private ViewModelResolver()
    {
        _serviceProvider = ConfigureServices(services =>
        {
        });
    }

    private static IServiceProvider ConfigureServices(Action<IServiceCollection> services)
    {
        var serviceCollection = new ServiceCollection();
        services(serviceCollection);
        return serviceCollection.BuildServiceProvider();
    }

    public T Get<T>() where T : class
        => _serviceProvider.GetRequiredService<T>();
}