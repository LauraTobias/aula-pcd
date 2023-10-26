using Confluent.Kafka;

namespace Aula.Producer
{
    public class Producer
    {
        static void Main(string[] args)
        {
            string bootstrapServers = "localhost:9092"; // Endereço do servidor Kafka
            string topic = "meu-topico"; // Nome do tópico Kafka

            // Configuração do produtor
            var producerConfig = new ProducerConfig
            {
                BootstrapServers = bootstrapServers
            };

            // Inicializar o produtor
            using (var producer = new ProducerBuilder<Null, string>(producerConfig).Build())
            {
                // Publicar mensagens
                string value = "";

                do
                {
                    Console.WriteLine("Escreva uma mensagem para o consumidor:");
                    value = Console.ReadLine();

                    var message = new Message<Null, string> { Value = value };

                    producer.Produce(topic, message, deliveryReport =>
                    {
                        if (deliveryReport.Error is not null && deliveryReport.Error.Reason != "Success")
                            Console.WriteLine($"Erro: {deliveryReport.Error.Reason}");
                    });

                } while (value != "0");
                

                producer.Flush(TimeSpan.FromSeconds(10));

                Console.WriteLine("Producer desligado.");
            }
        }
    }
}
