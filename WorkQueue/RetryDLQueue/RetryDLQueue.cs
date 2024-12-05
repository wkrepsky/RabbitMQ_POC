using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;
using System.Collections.Generic;

internal class Program
{
    private const string MainQueue = "x_main_queue";
    
    private const string DlQueue = "x_dl_queue";

    private static async Task Main(string[] args)
    {
        var factory = new ConnectionFactory { HostName = "localhost" };
        using var connection = await factory.CreateConnectionAsync();
        using var channel = await connection.CreateChannelAsync();

        await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false);

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += async (model, ea) =>
        {
            var body = ea.Body.ToArray();
            var message = Encoding.UTF8.GetString(body);

            Console.Write($" [x] Got from DLQ: {message}");

            try
            {
                // Republica a mensagem na MainQueue
                var properties = new BasicProperties
                {
                    Persistent = true
                };

                await channel.BasicPublishAsync(exchange: string.Empty, routingKey: MainQueue, mandatory: true, basicProperties: properties, body: body);
                Console.WriteLine($" - Movida para MainQueue");
                await channel.BasicAckAsync(deliveryTag: ea.DeliveryTag, multiple: false);
            }
            catch (Exception ex)
            {
                Console.WriteLine($" - Erro ao mover: {ex.Message}");
                
                await channel.BasicNackAsync(deliveryTag: ea.DeliveryTag, multiple: false, requeue: true);
            }
        };

        await channel.BasicConsumeAsync(queue: DlQueue, autoAck: false, consumer: consumer);

        Console.WriteLine(" Press [enter] to exit.");
        Console.ReadLine();   
    }
}