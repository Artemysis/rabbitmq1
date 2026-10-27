using System.Text;
using System.Text.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

const string ExchangeName = "oil.price.changes";
var host = Environment.GetEnvironmentVariable("RABBIT_HOST") ?? "localhost";
var department = Environment.GetEnvironmentVariable("DEPARTMENT_NAME") ?? "Отдел";
var queueName = Environment.GetEnvironmentVariable("DEPARTMENT_QUEUE") ?? "price.default";

var factory = new ConnectionFactory { HostName = host };
await using var connection = await factory.CreateConnectionAsync();
await using var channel = await connection.CreateChannelAsync();

await channel.ExchangeDeclareAsync(
    exchange: ExchangeName,
    type: ExchangeType.Fanout,
    durable: true,
    autoDelete: false);

await channel.QueueDeclareAsync(
    queue: queueName,
    durable: true,
    exclusive: false,
    autoDelete: false,
    arguments: null);

await channel.QueueBindAsync(queue: queueName, exchange: ExchangeName, routingKey: string.Empty);

var consumer = new AsyncEventingBasicConsumer(channel);
consumer.ReceivedAsync += async (_, ea) =>
{
    var json = Encoding.UTF8.GetString(ea.Body.ToArray());
    var evt = JsonSerializer.Deserialize<PriceChanged>(json)!;

    Console.WriteLine($"[{department}] Получено: {evt.OldPrice} -> {evt.NewPrice} {evt.Currency} (id {evt.Id.ToString()[..8]})");

    await channel.BasicAckAsync(ea.DeliveryTag, multiple: false);
};

await channel.BasicConsumeAsync(queue: queueName, autoAck: false, consumer: consumer);

Console.WriteLine($"[{department}] Подписан на {ExchangeName}, очередь {queueName}");
await Task.Delay(Timeout.Infinite);

public record PriceChanged(Guid Id, decimal OldPrice, decimal NewPrice, string Currency, DateTime ChangedAt);
