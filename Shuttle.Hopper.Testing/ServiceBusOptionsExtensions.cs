using Microsoft.Extensions.Logging;
using Shuttle.Core.Contract;

namespace Shuttle.Hopper.Testing;

public static class ServiceBusOptionsExtensions
{
    extension(ServiceBusOptions serviceBusOptions)
    {
        public void AddTransportEvents(ILogger logger)
        {
            Guard.AgainstNull(serviceBusOptions);

            serviceBusOptions.TransportCreated += (eventArgs, _) =>
            {
                logger.LogInformation("[TransportCreated] : queue = '{TransportName}'", eventArgs.Transport.Uri.TransportName);

                return Task.CompletedTask;
            };

            serviceBusOptions.TransportDisposing += (eventArgs, _) =>
            {
                logger.LogInformation("[TransportDisposing] : queue = '{TransportName}'", eventArgs.Transport.Uri.TransportName);

                return Task.CompletedTask;
            };

            serviceBusOptions.TransportDisposed += (eventArgs, _) =>
            {
                logger.LogInformation("[TransportDisposed] : queue = '{TransportName}'", eventArgs.Transport.Uri.TransportName);

                return Task.CompletedTask;
            };

            serviceBusOptions.MessageAcknowledged += (eventArgs, _) =>
            {
                logger.LogInformation("[{Scheme}.MessageAcknowledged] : queue = '{TransportName}'", eventArgs.Transport.Uri.Uri.Scheme, eventArgs.Transport.Uri.TransportName);

                return Task.CompletedTask;
            };

            serviceBusOptions.MessageSent += (eventArgs, _) =>
            {
                logger.LogInformation("[{Scheme}.MessageSent] : queue = '{TransportName}' / type = '{MessageType}'", eventArgs.Transport.Uri.Uri.Scheme, eventArgs.Transport.Uri.TransportName, eventArgs.TransportMessage.MessageType);

                return Task.CompletedTask;
            };

            serviceBusOptions.MessageReceived += (eventArgs, _) =>
            {
                logger.LogInformation("[{Scheme}.MessageReceived] : queue = '{TransportName}'", eventArgs.Transport.Uri.Uri.Scheme, eventArgs.Transport.Uri.TransportName);

                return Task.CompletedTask;
            };

            serviceBusOptions.MessageReleased += (eventArgs, _) =>
            {
                logger.LogInformation("[{Scheme}.MessageReleased] : queue = '{TransportName}'", eventArgs.Transport.Uri.Uri.Scheme, eventArgs.Transport.Uri.TransportName);

                return Task.CompletedTask;
            };

            serviceBusOptions.TransportOperation += (eventArgs, _) =>
            {
                logger.LogInformation("[{Scheme}.Operation] : queue = '{TransportName}' / operation = '{Operation}'", eventArgs.Transport.Uri.Uri.Scheme, eventArgs.Transport.Uri.TransportName, eventArgs.Operation);

                return Task.CompletedTask;
            };
        }
    }
}