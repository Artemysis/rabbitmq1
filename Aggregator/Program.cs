using Common;

await using var connection = await Rabbit.ConnectAsync();
await using var channel = await connection.CreateChannelAsync();

await Rabbit.DeclareQueueAsync(channel, Rabbit.EstimatesQueue);
await Rabbit.DeclareQueueAsync(channel, Rabbit.SummaryQueue);

var pending = new Dictionary<Guid, List<Estimate>>();
var gate = new object();

await Rabbit.ConsumeAsync(channel, Rabbit.EstimatesQueue, async ea =>
{
    var estimate = Rabbit.Read<Estimate>(ea);
    var replyTo = ea.BasicProperties.ReplyTo ?? Rabbit.SummaryQueue;

    Summary? summary = null;

    lock (gate)
    {
        if (!pending.TryGetValue(estimate.OrderId, out var list))
            pending[estimate.OrderId] = list = new List<Estimate>();

        if (list.All(e => e.WorkType != estimate.WorkType))
            list.Add(estimate);

        Console.WriteLine($"[Агрегатор] Заявка {estimate.OrderId.ToString()[..8]}: получено {list.Count} из {estimate.TotalParts} ({estimate.WorkType})");

        if (list.Count >= estimate.TotalParts)
        {
            summary = new Summary(
                estimate.OrderId,
                estimate.WellId,
                list,
                list.Sum(e => e.Cost),
                list.Max(e => e.Days)); // работы идут параллельно — срок по самой долгой

            pending.Remove(estimate.OrderId);
        }
    }

    if (summary is not null)
    {
        var props = Rabbit.Props(summary.OrderId.ToString(), null);
        await Rabbit.PublishAsync(channel, string.Empty, replyTo, summary, props);
        Console.WriteLine($"[Агрегатор] Смета по заявке {summary.OrderId.ToString()[..8]} отправлена: {summary.TotalCost:N0} руб., {summary.TotalDays} дн.");
    }
});

Console.WriteLine("[Агрегатор] Готов, жду расчёты...");
await Task.Delay(Timeout.Infinite);
