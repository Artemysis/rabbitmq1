using System.Text.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Common;

public static class Rabbit
{
    public const string RequestsQueue = "repair.requests";   // Инициатор -> Разделитель
    public const string PartsQueue = "repair.parts";         // Разделитель -> Маршрутизатор
    public const string WorksExchange = "repair.works";      // Маршрутизатор -> исполнители (direct)
    public const string EstimatesQueue = "repair.estimates"; // исполнители -> Агрегатор
    public const string SummaryQueue = "repair.summary";     // Агрегатор -> Инициатор (ответ)

    public static readonly string[] WorkTypes = { "drilling", "electrical", "logistics" };

    public static string WorkQueue(string workType) => $"works.{workType}";

    public static async Task<IConnection> ConnectAsync()
    {
        var factory = new ConnectionFactory
        {
            HostName = Environment.GetEnvironmentVariable("RABBIT_HOST") ?? "localhost"
        };

        for (var attempt = 1; ; attempt++)
        {
            try
            {
                return await factory.CreateConnectionAsync();
            }
            catch when (attempt < 10)
            {
                Console.WriteLine($"RabbitMQ недоступен, попытка {attempt}...");
                await Task.Delay(2000);
            }
        }
    }

    public static Task DeclareQueueAsync(IChannel channel, string queue) =>
        channel.QueueDeclareAsync(queue: queue, durable: true, exclusive: false, autoDelete: false, arguments: null);

    public static BasicProperties Props(string? correlationId, string? replyTo) => new()
    {
        Persistent = true,
        ContentType = "application/json",
        CorrelationId = correlationId,
        ReplyTo = replyTo
    };

    public static Task PublishAsync<T>(IChannel channel, string exchange, string routingKey, T message, BasicProperties props) =>
        channel.BasicPublishAsync(
            exchange: exchange,
            routingKey: routingKey,
            mandatory: false,
            basicProperties: props,
            body: JsonSerializer.SerializeToUtf8Bytes(message)).AsTask();

    public static T Read<T>(BasicDeliverEventArgs ea) => JsonSerializer.Deserialize<T>(ea.Body.Span)!;

    public static async Task ConsumeAsync(IChannel channel, string queue, Func<BasicDeliverEventArgs, Task> handler)
    {
        await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false);

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += async (_, ea) =>
        {
            try
            {
                await handler(ea);
                await channel.BasicAckAsync(ea.DeliveryTag, multiple: false);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Ошибка обработки сообщения: {ex.Message}");
                await channel.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: false);
            }
        };

        await channel.BasicConsumeAsync(queue: queue, autoAck: false, consumer: consumer);
    }
}
