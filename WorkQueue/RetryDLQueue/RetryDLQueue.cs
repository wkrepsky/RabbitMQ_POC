﻿using RabbitMQ.Client;
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

        // Consumir mensagens até que a fila esteja vazia
        int transferedItems = 0;
        while (true)
        {
            var result = await channel.BasicGetAsync(DlQueue, autoAck: false);
            if (result == null)
            {
                // Se não houver mais mensagens, saia do loop
                break;
            }

            var body = result.Body.ToArray();
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
                await channel.BasicAckAsync(deliveryTag: result.DeliveryTag, multiple: false);
            }
            catch (Exception ex)
            {
                Console.WriteLine($" - Erro ao mover: {ex.Message}");
                
                await channel.BasicNackAsync(deliveryTag: result.DeliveryTag, multiple: false, requeue: true);
            }
        }

        Console.WriteLine($" Itens da Dead Letter Queue movidos para a MainQueue: {transferedItems}");
    }
}