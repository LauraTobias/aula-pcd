using System;
using Confluent.Kafka;

class Consumer
{
    static void Main(string[] args)
    {
        string bootstrapServers = "localhost:9092"; // Endereço do servidor Kafka
        string topic = "meu-topico"; // Nome do tópico Kafka

        // Configuração do consumidor
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = "meu-grupo",
            AutoOffsetReset = AutoOffsetReset.Earliest // Pode ser "Latest" para mensagens mais recentes
        };

        // Inicializar o consumidor
        using (var consumer = new ConsumerBuilder<Ignore, string>(consumerConfig).Build())
        {
            consumer.Subscribe(topic);

            CancellationTokenSource cts = new CancellationTokenSource();

            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true;
                cts.Cancel();
            };

            try
            {
                while (true)
                {
                    try
                    {
                        var consumeResult = consumer.Consume(cts.Token);
                        Console.WriteLine($"Recebido: {consumeResult.Message.Value}");
                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine($"Erro ao consumir a mensagem: {e.Error.Reason}");
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // O programa foi interrompido
            }
            finally
            {
                consumer.Close();
            }
        }
    }
}
