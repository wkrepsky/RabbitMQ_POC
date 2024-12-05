using RabbitMQ.Client;
using System.Text;

internal class Program
{
    private static async global::System.Threading.Tasks.Task Main(string[] args)
    {
        var factory = new ConnectionFactory { HostName = "localhost" };
        using var connection = await factory.CreateConnectionAsync();
        using var channel = await connection.CreateChannelAsync();

        await channel.ExchangeDeclareAsync(exchange: "logs", type: ExchangeType.Fanout);

        var message = GetMessage(args);
        int messages = GetMessages(args);

        for (int i = 1; i <= messages; i++)
        {
            string bodyString = $"Message {i} of {messages}: {message}";
            var body = Encoding.UTF8.GetBytes(bodyString);
        
            await channel.BasicPublishAsync(exchange: "logs", routingKey: string.Empty, body: body);
            Console.WriteLine($" [x] Sent {bodyString}");
        }

        Console.WriteLine(" Done.");

        static string GetMessage(string[] args)
        {               
            return ((args.Length > 1) ? string.Join(" ", args.Skip(1)) : ".");
        }

        static int GetMessages(string[] args)
        {
            return ((args.Length > 0) ? int.Parse(args[0]) : 1);
        }

    }

}