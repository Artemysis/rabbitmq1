using Common;

var workType = Environment.GetEnvironmentVariable("WORK_TYPE")
    ?? throw new InvalidOperationException("Не задана переменная WORK_TYPE");
var executorName = $"исполнитель-{workType}";
var (baseCost, minDays, maxDays) = workType switch
{
    "drilling" => (500_000m, 10, 20),
    "electrical" => (120_000m, 3, 7),
    "logistics" => (60_000m, 2, 5),
    _ => throw new InvalidOperationException($"Неизвестный вид работ: {workType}")
};

await using var connection = await Rabbit.ConnectAsync();
await using var channel = await connection.CreateChannelAsync();

await Rabbit.DeclareQueueAsync(channel, Rabbit.WorkQueue(workType));
await Rabbit.DeclareQueueAsync(channel, Rabbit.EstimatesQueue);

await Rabbit.ConsumeAsync(channel, Rabbit.WorkQueue(workType), async ea =>
{
    var part = Rabbit.Read<WorkPart>(ea);
    Console.WriteLine($"[{executorName}] Считаю заявку {part.OrderId.ToString()[..8]} ({part.WellId})...");

    await Task.Delay(Random.Shared.Next(1000, 3000));

    var cost = Math.Round(baseCost * (decimal)(0.8 + Random.Shared.NextDouble() * 0.4), 0);
    var days = Random.Shared.Next(minDays, maxDays + 1);

    var estimate = new Estimate(part.OrderId, part.WellId, workType, cost, days, part.TotalParts, executorName);

    var props = Rabbit.Props(ea.BasicProperties.CorrelationId, ea.BasicProperties.ReplyTo);
    await Rabbit.PublishAsync(channel, string.Empty, Rabbit.EstimatesQueue, estimate, props);

    Console.WriteLine($"[{executorName}] Расчёт готов: {cost:N0} руб., {days} дн.");
});

Console.WriteLine($"[{executorName}] Готов, жду заявки...");
await Task.Delay(Timeout.Infinite);
