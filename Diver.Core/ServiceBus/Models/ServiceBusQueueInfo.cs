namespace Diver.Core.ServiceBus.Models;

public record ServiceBusQueueInfo(string QueueName, long ActiveMessageCount, long DeadLetterMessageCount);