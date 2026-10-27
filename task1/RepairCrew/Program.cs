using System.Text;
using System.Text.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

const string QueueName = "well.repair.requests";
var host = Environment.GetEnvironmentVariable("RABBIT_HOST") ?? "localhost";
var crew = Environment.GetEnvironmentVariable("CREW_NAME") ?? "Бригада";

var factory = new ConnectionFactory { HostName = host };
await using var connection = await factory.CreateConnectionAsync();
await using var channel = await connection.CreateChannelAsync();

await channel.QueueDeclareAsync(
    queue: QueueName,
    durable: true,
    exclusive: false,
    autoDelete: false,
    arguments: null);

await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false);

var consumer = new AsyncEventingBasicConsumer(channel);
consumer.ReceivedAsync += async (_, ea) =>
{
    var json = Encoding.UTF8.GetString(ea.Body.ToArray());
    var request = JsonSerializer.Deserialize<RepairRequest>(json)!;

    Console.WriteLine($"[{crew}] ВЗЯЛА заявку {request.Id.ToString()[..8]}: {request.WellId} — {request.WorkType}");

    await Task.Delay(Random.Shared.Next(2000, 5000));

    await channel.BasicAckAsync(ea.DeliveryTag, multiple: false);

    Console.WriteLine($"[{crew}] ВЫПОЛНИЛА заявку {request.Id.ToString()[..8]} ({request.WellId})");
};

await channel.BasicConsumeAsync(queue: QueueName, autoAck: false, consumer: consumer);

Console.WriteLine($"[{crew}] Готова к работе, жду заявки...");
await Task.Delay(Timeout.Infinite);

public record RepairRequest(Guid Id, string WellId, string WorkType, DateTime CreatedAt);
