using Common;

await using var connection = await Rabbit.ConnectAsync();
await using var channel = await connection.CreateChannelAsync();

await Rabbit.DeclareQueueAsync(channel, Rabbit.RequestsQueue);
await Rabbit.DeclareQueueAsync(channel, Rabbit.SummaryQueue);

await Rabbit.ConsumeAsync(channel, Rabbit.SummaryQueue, ea =>
{
    var s = Rabbit.Read<Summary>(ea);
    Console.WriteLine($"[Инициатор] СМЕТА по заявке {s.OrderId.ToString()[..8]} (скважина {s.WellId}):");
    foreach (var e in s.Estimates)
        Console.WriteLine($"    {e.WorkType,-11} {e.Cost,9:N0} руб., {e.Days,2} дн. ({e.Executor})");
    Console.WriteLine($"    ИТОГО: {s.TotalCost:N0} руб., срок (макс.): {s.TotalDays} дн.");
    return Task.CompletedTask;
});

string[] wells = { "Скв-101", "Скв-205", "Скв-310", "Скв-417" };

while (true)
{
    var order = new RepairOrder(Guid.NewGuid(), wells[Random.Shared.Next(wells.Length)], Rabbit.WorkTypes, DateTime.UtcNow);

    var props = Rabbit.Props(order.Id.ToString(), Rabbit.SummaryQueue);
    await Rabbit.PublishAsync(channel, string.Empty, Rabbit.RequestsQueue, order, props);

    Console.WriteLine($"[Инициатор] Отправлена заявка {order.Id.ToString()[..8]} на ремонт {order.WellId}: {string.Join(", ", order.WorkTypes)}");

    await Task.Delay(10000);
}
