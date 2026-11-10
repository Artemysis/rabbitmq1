using Common;


await using var connection = await Rabbit.ConnectAsync();
await using var channel = await connection.CreateChannelAsync();

await Rabbit.DeclareQueueAsync(channel, Rabbit.RequestsQueue);
await Rabbit.DeclareQueueAsync(channel, Rabbit.PartsQueue);

await Rabbit.ConsumeAsync(channel, Rabbit.RequestsQueue, async ea =>
{
    var order = Rabbit.Read<RepairOrder>(ea);
    var replyTo = ea.BasicProperties.ReplyTo;

    Console.WriteLine($"[Разделитель] Заявка {order.Id.ToString()[..8]} -> {order.WorkTypes.Length} частей");

    foreach (var workType in order.WorkTypes)
    {
        var part = new WorkPart(order.Id, order.WellId, workType, order.WorkTypes.Length);

        var props = Rabbit.Props(order.Id.ToString(), replyTo);
        await Rabbit.PublishAsync(channel, string.Empty, Rabbit.PartsQueue, part, props);
    }
});

Console.WriteLine("[Разделитель] Готов, жду заявки...");
await Task.Delay(Timeout.Infinite);
