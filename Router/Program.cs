using Common;
using RabbitMQ.Client;


await using var connection = await Rabbit.ConnectAsync();
await using var channel = await connection.CreateChannelAsync();

await Rabbit.DeclareQueueAsync(channel, Rabbit.PartsQueue);

await channel.ExchangeDeclareAsync(exchange: Rabbit.WorksExchange, type: ExchangeType.Direct, durable: true, autoDelete: false);

foreach (var workType in Rabbit.WorkTypes)
{
    await Rabbit.DeclareQueueAsync(channel, Rabbit.WorkQueue(workType));
    await channel.QueueBindAsync(queue: Rabbit.WorkQueue(workType), exchange: Rabbit.WorksExchange, routingKey: workType);
}

await Rabbit.ConsumeAsync(channel, Rabbit.PartsQueue, async ea =>
{
    var part = Rabbit.Read<WorkPart>(ea);

    if (!Rabbit.WorkTypes.Contains(part.WorkType))
    {
        Console.WriteLine($"[Маршрутизатор] Неизвестный вид работ '{part.WorkType}', часть пропущена");
        return;
    }

    var props = Rabbit.Props(ea.BasicProperties.CorrelationId, ea.BasicProperties.ReplyTo);
    await Rabbit.PublishAsync(channel, Rabbit.WorksExchange, part.WorkType, part, props);

    Console.WriteLine($"[Маршрутизатор] Заявка {part.OrderId.ToString()[..8]}: {part.WorkType} -> {Rabbit.WorkQueue(part.WorkType)}");
});

Console.WriteLine("[Маршрутизатор] Готов...");
await Task.Delay(Timeout.Infinite);
