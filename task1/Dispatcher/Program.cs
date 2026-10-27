using System.Text.Json;
using RabbitMQ.Client;


const string QueueName = "well.repair.requests";
var host = Environment.GetEnvironmentVariable("RABBIT_HOST") ?? "localhost";

var factory = new ConnectionFactory { HostName = host };
await using var connection = await factory.CreateConnectionAsync();
await using var channel = await connection.CreateChannelAsync();

await channel.QueueDeclareAsync(
    queue: QueueName,
    durable: true,
    exclusive: false,
    autoDelete: false,
    arguments: null);

string[] wells = { "Скв-101", "Скв-102", "Скв-205", "Скв-310", "Скв-417" };
string[] works = { "Замена насоса", "Обслуживание устья", "Промывка скважины", "Ремонт трубопровода" };

var props = new BasicProperties
{
    Persistent = true,
    ContentType = "application/json"
};

Console.WriteLine("[Диспетчерская] Запущена, отправляю заявки...");

while (true)
{
    var request = new RepairRequest(
        Guid.NewGuid(),
        wells[Random.Shared.Next(wells.Length)],
        works[Random.Shared.Next(works.Length)],
        DateTime.UtcNow);

    var body = JsonSerializer.SerializeToUtf8Bytes(request);

    await channel.BasicPublishAsync(
        exchange: string.Empty,
        routingKey: QueueName,
        mandatory: false,
        basicProperties: props,
        body: body);

    Console.WriteLine($"[Диспетчерская] Заявка {request.Id.ToString()[..8]}: {request.WellId} — {request.WorkType}");

    await Task.Delay(1500);
}

public record RepairRequest(Guid Id, string WellId, string WorkType, DateTime CreatedAt);
