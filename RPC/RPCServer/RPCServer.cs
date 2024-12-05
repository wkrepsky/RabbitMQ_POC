using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using System.Diagnostics;

internal class Program
{
    private static async Task Main(string[] args)
    {
        const string QUEUE_NAME = "rpc_queue";

        var factory = new ConnectionFactory { HostName = "localhost" };
        using var connection = await factory.CreateConnectionAsync();
        using var channel = await connection.CreateChannelAsync();

        await channel.QueueDeclareAsync(queue: QUEUE_NAME, durable: false, exclusive: false,
            autoDelete: false, arguments: null);

        await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false);

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += async (sender, ea) =>
        {
            AsyncEventingBasicConsumer cons = (AsyncEventingBasicConsumer)sender;
            IChannel ch = cons.Channel;
            string response = string.Empty;

            byte[] body = ea.Body.ToArray();
            IReadOnlyBasicProperties props = ea.BasicProperties;
            var replyProps = new BasicProperties
            {
                CorrelationId = props.CorrelationId
            };

            try
            {
                var message = Encoding.UTF8.GetString(body);
                int n = int.Parse(message);

                Console.WriteLine($" [.] Start:  Fib({message})");
                var stopwatch = Stopwatch.StartNew();

                response = Fib(n).ToString();

                stopwatch.Stop();
                Console.WriteLine($" [.] Finish: Fib({message}) in {stopwatch.ElapsedMilliseconds}ms");
            }
            catch (Exception e)
            {
                Console.WriteLine($" [.] {e.Message}");
                response = string.Empty;
            }
            finally
            {
                var responseBytes = Encoding.UTF8.GetBytes(response);
                await ch.BasicPublishAsync(exchange: string.Empty, routingKey: props.ReplyTo!,
                    mandatory: true, basicProperties: replyProps, body: responseBytes);
                await ch.BasicAckAsync(deliveryTag: ea.DeliveryTag, multiple: false);
            }
        };

        await channel.BasicConsumeAsync(QUEUE_NAME, false, consumer);
        Console.WriteLine(" [x] Awaiting RPC requests");
        Console.WriteLine(" Press [enter] to exit.");
        Console.ReadLine();

        // Assumes only valid positive integer input.
        // Don't expect this one to work for big numbers,
        // and it's probably the slowest recursive implementation possible.
        static int Fib(int n)
        {
            if (n is 0 or 1)
            {
                return n;
            }

            return Fib(n - 1) + Fib(n - 2);
        }
    }
}