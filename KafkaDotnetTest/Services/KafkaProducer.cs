using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace KafkaDotnetTest.Services;

public class KafkaProducer(ILogger<KafkaProducer> logger, IConfiguration config) : BackgroundService
{
    private readonly string[] _users = ["eabara", "jsmith", "sgarcia", "jbernard", "htanaka", "awalther"];
    private readonly string[] _items = ["book", "alarm clock", "t-shirts", "gift card", "batteries"];
    private readonly ILogger<KafkaProducer> _logger = logger;
    private readonly IConfiguration _config = config;
    const string Topic = "test";

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var kvp1 = new KeyValuePair<string, string>("bootstrap.servers", _config["bootstrap.servers"]!);
        var kvp2 = new KeyValuePair<string, string>("security.protocol", _config["security.protocol"]!);
        var lst = new List<KeyValuePair<string, string>>{ kvp1, kvp2 };

        using (var producer = new ProducerBuilder<string, string>(lst).Build())
        {
            var numProduced = 0;
            var rnd = new Random();
            const int numMessages = 10;
            for (var i = 0; i < numMessages; ++i)
            {
                var user = _users[rnd.Next(_users.Length)];
                var item = _items[rnd.Next(_items.Length)];

                producer.Produce(Topic, new Message<string, string> { Key = user, Value = item },
                    (deliveryReport) =>
                    {
                        if (deliveryReport.Error.Code != ErrorCode.NoError)
                        {
                            _logger.LogInformation("Failed to deliver message: {reason}", deliveryReport.Error.Reason);
                        }
                        else
                        {
                            _logger.LogInformation("Produced event to topic {topic}: key = {user,-10} value = {item}",Topic, user, item);
                            numProduced += 1;
                        }
                    });
            }

            producer.Flush(TimeSpan.FromSeconds(10));
            Console.WriteLine($"{numProduced} messages were produced to topic {Topic}");
        }

        _logger.LogInformation("SampleHostedService started");
        return Task.CompletedTask;
    }
}