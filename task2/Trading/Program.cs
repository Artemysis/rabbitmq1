using System.Text.Json;
using RabbitMQ.Client;

const string ExchangeName = "oil.price.changes";
var host = Environment.GetEnvironmentVariable("RABBIT_HOST") ?? "localhost";

var factory = new ConnectionFactory { HostName = host };
await using var connection = await factory.CreateConnectionAsync();
await using var channel = await connection.CreateChannelAsync();

await channel.ExchangeDeclareAsync(
    exchange: ExchangeName,
    type: ExchangeType.Fanout,
    durable: true,
    autoDelete: false);

await Task.Delay(3000);

var props = new BasicProperties { Persistent = true, ContentType = "application/json" };
decimal price = 80.00m;

Console.WriteLine("[Трейдинг] Запущен, публикую изменения цены...");

while (true)
{
    var oldPrice = price;
    price = Math.Round(price + (decimal)(Random.Shared.NextDouble() * 4 - 2), 2);

    var evt = new PriceChanged(Guid.NewGuid(), oldPrice, price, "USD", DateTime.UtcNow);
    var body = JsonSerializer.SerializeToUtf8Bytes(evt);

    await channel.BasicPublishAsync(
        exchange: ExchangeName,
        routingKey: string.Empty,
        mandatory: false,
        basicProperties: props,
        body: body);

    Console.WriteLine($"[Трейдинг] Цена барреля: {evt.OldPrice} -> {evt.NewPrice} {evt.Currency}");

    await Task.Delay(3000);
}

public record PriceChanged(Guid Id, decimal OldPrice, decimal NewPrice, string Currency, DateTime ChangedAt);
