namespace Diver.Core;

internal static class AzureResource
{
    internal static class ServiceBus
    {
        internal static string GetFullyQualifiedNamespace(string serviceBusName)
            => $"{serviceBusName}.servicebus.windows.net";

        internal static string GetDeadletterQueueName(string queueName)
            => $"{queueName}/$deadletterqueue";
    }

}