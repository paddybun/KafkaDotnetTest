using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace KafkaDotnetTest.Services;

public class KafkaConsumer(ILogger<KafkaConsumer> logger, IConfiguration configuration)
    : BackgroundService
{
    private readonly ILogger<KafkaConsumer> _logger = logger;
    private readonly IConfiguration _configuration = configuration;
    private const string Topic = "test";

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var kvp1 = new KeyValuePair<string, string>("bootstrap.servers", _configuration["bootstrap.servers"]!);
        var kvp2 = new KeyValuePair<string, string>("security.protocol", _configuration["security.protocol"]!);
        var kvp3 = new KeyValuePair<string, string>("group.id", "kafka-dotnet-getting-started");
        var kvp4 = new KeyValuePair<string, string>("auto.offset.reset", "earliest");
        var lst = new List<KeyValuePair<string, string>>{ kvp1, kvp2, kvp3, kvp4 };

        using (var consumer = new ConsumerBuilder<string, string>(
                   lst).Build())
        {
            consumer.Subscribe(Topic);
            try {
                while (true) {
                    var cr = consumer.Consume(stoppingToken);
                    _logger.LogInformation("Consumed event from topic {topic}: key = {msgKey,-10} value = {msgValue}", Topic, cr.Message.Key, cr.Message.Value);
                }
            }
            catch (OperationCanceledException) {
                // Ctrl-C was pressed.
                _logger.LogInformation("Stopping consumer, Ctrl+C requested.");
            }
            finally{
                consumer.Close();
            }
        }
        return Task.CompletedTask;
    }
}