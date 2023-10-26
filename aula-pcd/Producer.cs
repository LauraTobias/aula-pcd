using Confluent.Kafka;

class Producer
{
    static void Main(string[] args)
    {
        var config = new ProducerConfig { BootstrapServers = "localhost:9092" };

        using (var producer = new ProducerBuilder<Null, string>(config).Build())
        {
            var message = new Message<Null, string> { Value = "Mensagem de exemplo" };
            producer.Produce("seu-topico", message);
            producer.Flush(TimeSpan.FromSeconds(10));
        }
    }
}
