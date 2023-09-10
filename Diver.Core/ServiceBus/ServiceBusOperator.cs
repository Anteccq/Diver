using Azure.Core;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using Diver.Core.ServiceBus.Models;

namespace Diver.Core.ServiceBus;

public class ServiceBusOperator : IServiceBusOperator
{
    private readonly ServiceBusAdministrationClient _adminClient;
    private readonly ServiceBusClient _client;


    public ServiceBusOperator(ServiceBusAdministrationClient adminClient, ServiceBusClient client)
    {
        _adminClient = adminClient;
        _client = client;
    }

    public ServiceBusOperator(string serviceBusName, TokenCredential credential)
    {
        var serviceBusFullyQualifiedNamespace = AzureResource.ServiceBus.GetFullyQualifiedNamespace(serviceBusName);
        _adminClient = new ServiceBusAdministrationClient(serviceBusFullyQualifiedNamespace, credential);
        _client = new ServiceBusClient(serviceBusFullyQualifiedNamespace, credential);
    }

    public IAsyncEnumerable<ServiceBusQueueInfo> GetAllQueueAsync(CancellationToken cancellationToken=default)
        => _adminClient.GetQueuesRuntimePropertiesAsync(cancellationToken)
            .Select(x => new ServiceBusQueueInfo(x.Name, x.ActiveMessageCount, x.DeadLetterMessageCount));

    public Task<IReadOnlyList<ServiceBusReceivedMessage>> GetActiveQueueMassagesAsync(string queueName, int maxMessageSize, CancellationToken cancellationToken = default)
        => GetMessagesAsync(queueName, maxMessageSize, cancellationToken);

    public Task<IReadOnlyList<ServiceBusReceivedMessage>> GetDeadLetterMessagesAsync(string queueName,
        int maxMessageSize, CancellationToken cancellationToken = default)
        => GetMessagesAsync(AzureResource.ServiceBus.GetDeadletterQueueName(queueName), maxMessageSize, cancellationToken);

    public IAsyncEnumerable<IReadOnlyList<ServiceBusReceivedMessage>> GetPagedActiveQueueMessagesAsync(string queueName, int maxMessageSize, long? sequenceNumber, CancellationToken cancellationToken = default)
        => GetPagedMessagesAsync(queueName, maxMessageSize, sequenceNumber, cancellationToken);

    public IAsyncEnumerable<IReadOnlyList<ServiceBusReceivedMessage>> GetPagedDeadLetterQueueMessagesAsync(string queueName, int maxMessageSize, long? sequenceNumber, CancellationToken cancellationToken = default)
        => GetPagedMessagesAsync(AzureResource.ServiceBus.GetDeadletterQueueName(queueName), maxMessageSize, sequenceNumber, cancellationToken);
    
    public IAsyncEnumerable<ServiceBusReceivedMessage> SearchActiveQueueMessagesAsync(string queueName, string condition, int limit, long? sequenceNumber, CancellationToken cancellationToken = default)
        => SearchMessagesAsync(queueName, condition, limit, sequenceNumber, cancellationToken);

    public IAsyncEnumerable<ServiceBusReceivedMessage> SearchDeadLetterQueueMessagesAsync(string queueName, string condition, int limit, long? sequenceNumber, CancellationToken cancellationToken = default)
        => SearchMessagesAsync(AzureResource.ServiceBus.GetDeadletterQueueName(queueName), condition, limit, sequenceNumber, cancellationToken);

    public async Task SendMessageAsync(string queueName, string message, string contentType, CancellationToken cancellationToken=default)
    {
        await using var sender = _client.CreateSender(queueName);
        await sender.SendMessageAsync(new ServiceBusMessage(BinaryData.FromString(message)) { ContentType = contentType }, cancellationToken);
    }

    public async Task SendMessagesAsync(string queueName, IEnumerable<string> messages, string contentType, CancellationToken cancellationToken = default)
    {
        await using var sender = _client.CreateSender(queueName);
        await sender.SendMessagesAsync(messages.Select(x => new ServiceBusMessage(BinaryData.FromString(x)) { ContentType = contentType }), cancellationToken);
    }

    private async Task<IReadOnlyList<ServiceBusReceivedMessage>> GetMessagesAsync(string queueName, int maxMessageSize, CancellationToken cancellationToken=default)
    {
        await using var receiver = _client.CreateReceiver(queueName, new ServiceBusReceiverOptions { ReceiveMode = ServiceBusReceiveMode.PeekLock });
        return await receiver.PeekMessagesAsync(maxMessageSize, null, cancellationToken);
    }

    private async IAsyncEnumerable<IReadOnlyList<ServiceBusReceivedMessage>> GetPagedMessagesAsync(string queueName, int maxMessageSize, long? sequenceNumber=null, CancellationToken cancellationToken = default)
    {
        if (maxMessageSize < 1)
            throw new ArgumentOutOfRangeException(nameof(maxMessageSize), "maxMessageSize must be greater than or equal to 1");

        await using var receiver = _client.CreateReceiver(queueName, new ServiceBusReceiverOptions { ReceiveMode = ServiceBusReceiveMode.PeekLock });
        bool continuable;
        
        do
        {
            var messages = await receiver.PeekMessagesAsync(maxMessageSize, sequenceNumber, cancellationToken);
            yield return messages;
            continuable = messages.Count == maxMessageSize;

            if(continuable)
                sequenceNumber = messages[^1].SequenceNumber+1;
        } while (continuable);
    }

    private async IAsyncEnumerable<ServiceBusReceivedMessage> SearchMessagesAsync(string queueName, string condition, int limit, long? sequenceNumber = null, CancellationToken cancellationToken = default)
    {
        if (limit < 1)
            throw new ArgumentOutOfRangeException(nameof(limit), "maxMessageSize must be greater than or equal to 1");

        const int maxMessageSize = 100;

        await using var receiver = _client.CreateReceiver(queueName, new ServiceBusReceiverOptions { ReceiveMode = ServiceBusReceiveMode.PeekLock });
        bool continuable;

        long hitCount = 0;

        do
        {
            var messages = await receiver.PeekMessagesAsync(maxMessageSize, sequenceNumber, cancellationToken);

            foreach (var message in messages.Where(x => x.Body.ToString().Contains(condition)))
            {
                hitCount++;
                yield return message;
            }

            continuable = messages.Count == maxMessageSize && hitCount < limit;

            if (continuable)
                sequenceNumber = messages[^1].SequenceNumber + 1;
        } while (continuable);
    }
}
