using RabbitMQ.Client;
using System.Text;

internal class Program
{
    private static async global::System.Threading.Tasks.Task Main(string[] args)
    {
        int messages = 1;
        string message = "blank";

        if (args.Length > 0)
        {
            messages = int.Parse(args[0]);
        }

        if (args.Length > 1)
        {
            message = args[1];
        }

        var factory = new ConnectionFactory { HostName = "localhost" };
        using var connection = await factory.CreateConnectionAsync();
        using var channel = await connection.CreateChannelAsync();

        await channel.QueueDeclareAsync(queue: "fila1", durable: false, exclusive: false, autoDelete: false, arguments: null);

        for (int i = 1; i <= messages; i++)
        {
            string bodyString = $"Message {i} of {messages}: {message}";
            var body = Encoding.UTF8.GetBytes(bodyString);

            await channel.BasicPublishAsync(exchange: string.Empty, routingKey: "fila1", body: body);
            //Console.WriteLine($"- Sent {bodyString}");
        }

        Console.WriteLine($"{messages} messages sent");
    }
}