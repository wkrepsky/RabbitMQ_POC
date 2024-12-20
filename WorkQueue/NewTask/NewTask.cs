using RabbitMQ.Client;
using System.Text;
using System.Collections.Generic;
using System.Security;

internal class Program
{
    private const string MainQueue = "x_main_queue";

    public static string Actor = "Tasker" + new Random().Next(1, 100);

    private static async global::System.Threading.Tasks.Task Main(string[] args)
    {
        // Parâmetros de entrada (linha de comando)
        int messagesToSend = (args.Length > 0) ? int.Parse(args[0]) : 1;              // Qtd de mensagens
        var message = (args.Length > 1) ? string.Join(" ", args.Skip(1)) : "Msg.";    // Título da mensagem

        // Conta os pontos no bodyString, cada ponto representa 200ms de processamento, e remove os pontos
        int dots = message.Count(c => c == '.');
        message = message.Replace(".", "");

        // RabbitMQ channel
        var factory = new ConnectionFactory { HostName = "localhost" };
        using var connection = await factory.CreateConnectionAsync();
        using var channel = await connection.CreateChannelAsync();

        // Código sem retry
        //await channel.QueueDeclareAsync(queue: "task_queue", durable: true, exclusive: false, autoDelete: false, arguments: null);

        var properties = new BasicProperties
        {
            Persistent = true
        };

        for (int i = 1; i <= messagesToSend; i++)
        {
            string bodyString = $"{message}{i}/{messagesToSend}";
            var body = Encoding.UTF8.GetBytes(bodyString);

            await channel.BasicPublishAsync(string.Empty, MainQueue, mandatory: true, basicProperties: properties, body: body);

            Console.WriteLine($"{DateTime.Now.ToString("HH:mm:ss:fff")}\t{Program.Actor}\tSent\t{bodyString}");

            await Task.Delay(20);
        }
    }
}