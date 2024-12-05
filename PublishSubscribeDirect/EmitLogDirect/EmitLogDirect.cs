using RabbitMQ.Client;
using System.Text;

internal class Program
{
    private static async global::System.Threading.Tasks.Task Main(string[] args)
    {
        var factory = new ConnectionFactory { HostName = "localhost" };
        using var connection = await factory.CreateConnectionAsync();
        using var channel = await connection.CreateChannelAsync();

        await channel.ExchangeDeclareAsync(exchange: "direct_logs", type: ExchangeType.Direct);

        var severity = (args.Length > 0) ? args[0] : "info";
        var messages = (args.Length > 1) ? int.Parse(args[1]) : 1;
        var message = (args.Length > 2) ? string.Join(" ", args.Skip(2).ToArray()) : "Log A.";

        for (int i = 1; i <= messages; i++)
        {
            string bodyString = $"Message {i} of {messages}: {message}";
            var body = Encoding.UTF8.GetBytes(bodyString);
        
            await channel.BasicPublishAsync(exchange: "direct_logs", routingKey: severity, body: body);
            Console.WriteLine($" [x] Sent '{severity}':'{bodyString}'");
        }

        Console.WriteLine(" Done.");
    }

}