using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Serilog;
using Serilog.Events;
using KafkaDotnetTest;
using KafkaDotnetTest.Services;


// Configure serilog
Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Debug()
    .MinimumLevel.Override("Microsoft", LogEventLevel.Information)
    .Enrich.FromLogContext()
    .WriteTo.Console()
    .CreateLogger();

var builder = Host.CreateDefaultBuilder()
    .ConfigureAppConfiguration((builder, config) =>
    {
        config.AddJsonFile("appsettings.json");
        config.AddJsonFile("appsettings.Development.json");
        config.AddIniFile("kafka.settings.ini");
    })
    .ConfigureServices((builder, services) =>
    {
        services.Configure<Settings>(builder.Configuration.GetSection("Settings"));
        services.AddHostedService<KafkaProducer>();
        services.AddHostedService<KafkaConsumer>();
        services.AddSingleton<CommandLine>();
    })
    .UseSerilog();

var host = builder.Build();
await host.StartAsync();

var commandLine = host.Services.GetService<CommandLine>()!;
await commandLine.ParseCommandLineAsync(args);

Console.Read();