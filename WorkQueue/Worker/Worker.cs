using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using System.Collections.Generic;
using System.Reflection.Metadata.Ecma335;

internal class Program
{
    private const string MainQueue = "x_main_queue";
    private const string RetryQueue = "x_retry_queue";
    private const string DlQueue = "x_dl_queue";

    public static string Actor = "Worker" + new Random().Next(1, 100);

    private const int MaxRetries = 5;

    private static readonly int[] _attemptDelay = new int[] { 5, 10, 20, 40, 80, 160 };

    private static async global::System.Threading.Tasks.Task Main(string[] args)
    {
        var factory = new ConnectionFactory { HostName = "localhost" };
        using var connection = await factory.CreateConnectionAsync();
        using var channel = await connection.CreateChannelAsync();

        if ((args.Length > 0) && (args[0].ToLower() == "setup"))
        {
            await SetupQueues(channel);
            Console.WriteLine(" [*] Queues setup done");
            return;
        }

        // Código original, sem retry
        //await channel.QueueDeclareAsync(queue: "task_queue", durable: true, exclusive: false, autoDelete: false, arguments: null);

        await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false);

        Console.WriteLine($" [*] {Actor}: Waiting for messages.");

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += async (model, ea) =>
        {
            var body = ea.Body.ToArray();
            var message = Encoding.UTF8.GetString(body);

            // Tentativa atual
            int retryCount;
            if (ea.BasicProperties.Headers != null && ea.BasicProperties.Headers.TryGetValue("x-retry-count", out var retryCountObj))
                retryCount = Convert.ToInt32(retryCountObj);
            else
                retryCount = 0;

            string retryText = (retryCount == 0) ? "" : $"Retry {retryCount}";
            Console.WriteLine($"{DateTime.Now.ToString("HH:mm:ss:fff")}\t{Program.Actor}\tGot\t{message}\t{retryText}");

            // Duração da execução
            int dots = message.Split('.').Length - 1;

            try
            {
                // Simulando a execução de alguma tarefa
                await RunProcess(dots);

                Console.WriteLine($"{DateTime.Now.ToString("HH:mm:ss:fff")}\t{Program.Actor}\tDone\t{message}");

                // Commit
                await channel.BasicAckAsync(deliveryTag: ea.DeliveryTag, multiple: false);
            }
            catch (System.Exception e)
            {
                Console.WriteLine($"{DateTime.Now.ToString("HH:mm:ss:fff")}\t{Program.Actor}\tFail\t{message}\t{e.Message}");

                // Retry
                if (retryCount < MaxRetries)
                {
                    retryCount++;

                    Console.WriteLine($"{DateTime.Now.ToString("HH:mm:ss:fff")}\t{Program.Actor}\tRetry {retryCount}\t{message}");

                    // Reenviar para a fila de retry
                    var properties = new BasicProperties
                    {
                        Persistent = true
                    };
                    properties.Headers = ea.BasicProperties.Headers ?? new Dictionary<string, object?>();
                    properties.Headers["x-retry-count"] = retryCount;

                    await channel.BasicPublishAsync(exchange: string.Empty, routingKey: RetryQueue + $"_{retryCount}", mandatory: true, basicProperties: properties, body: body);

                    await channel.BasicAckAsync(deliveryTag: ea.DeliveryTag, multiple: false);
                }
                else  // DLQ
                {
                    Console.WriteLine($"{DateTime.Now.ToString("HH:mm:ss:fff")}\t{Program.Actor}\tDLQ\t{message}");

                    await channel.BasicRejectAsync(deliveryTag: ea.DeliveryTag, false);
                }
            }              

        };

        // A fila que será consumida é a MainQueue
        await channel.BasicConsumeAsync(MainQueue, autoAck: false, consumer: consumer);

        Console.WriteLine(" [*] Press [enter] to exit.");
        Console.ReadLine();   
        Console.WriteLine(" [*] Exiting.");
    }

    private static async Task SetupQueues(IChannel channel)
    {
        // Dead Letter Queue
        await channel.QueueDeclareAsync(DlQueue, durable: true, exclusive: false, autoDelete: false);

        // Retry Queues
        for (int i = 0; i < MaxRetries; i++)
        {
            await channel.QueueDeleteAsync(RetryQueue, ifEmpty: true);
            await channel.QueueDeclareAsync(RetryQueue + $"_{i + 1}", durable: true, exclusive: false, autoDelete: false, arguments: new Dictionary<string, object?>
            {
                { "x-dead-letter-exchange", "" },
                { "x-dead-letter-routing-key", MainQueue },
                { "x-message-ttl", (_attemptDelay[i] * 1000) }
            });
        }

         // Main
        await channel.QueueDeclareAsync(MainQueue, durable: true, exclusive: false, autoDelete: false, arguments: new Dictionary<string, object?>
        {
            { "x-dead-letter-exchange", "" },
            { "x-dead-letter-routing-key", DlQueue }
        });
    }

    private static async Task RunProcess(int times)
    {
        // Gerar um número randômico
        Random random = new Random();
        int randomValue = random.Next(0, 3);

        // Se o número for 2, gerar uma falha aleatória
        if (randomValue == 2)
        {
            throw new Exception("Connection lost");
        } else {
            // Simula uma execução
            await Task.Delay(times * 200);
        }
        
    }
}