using Microsoft.Extensions.Options;
using Nstech.Worker.Consumers;
using Nstech.Worker.Options;
using Nstech.Worker.Services;

namespace Nstech.Worker;

public static class Program
{
    public static void Main(string[] args) =>
        Host.CreateApplicationBuilder(args)
            .ConfigureServices()
            .Build()
            .Run();

    private static HostApplicationBuilder ConfigureServices(this HostApplicationBuilder builder)
    {
        builder.Services
            .AddOptions<KafkaOptions>()
            .Bind(builder.Configuration.GetSection(KafkaOptions.SectionName))
            .Validate(o => !string.IsNullOrWhiteSpace(o.BootstrapServers), "Kafka:BootstrapServers is required")
            .Validate(o => !string.IsNullOrWhiteSpace(o.GroupId), "Kafka:GroupId is required")
            .ValidateOnStart();

        builder.Services
            .AddOptions<ValkeyOptions>()
            .Bind(builder.Configuration.GetSection(ValkeyOptions.SectionName))
            .Validate(o => !string.IsNullOrWhiteSpace(o.ConnectionString), "Valkey:ConnectionString is required")
            .ValidateOnStart();

        builder.Services.AddSingleton<IConnectionMultiplexer>(sp =>
        {
            var options = sp.GetRequiredService<IOptions<ValkeyOptions>>().Value;
            var mux = ConnectionMultiplexer.Connect(options.ConnectionString);
            mux.ConnectionFailed += (_, e) =>
            {
                var log = sp.GetRequiredService<ILoggerFactory>().CreateLogger("Valkey");
                log.LogError("Valkey connection failed: {Message}", e.Exception?.Message);
            };
            return mux;
        });

        builder.Services.AddSingleton<IOrderingGuard, OrderingGuard>();
        builder.Services.AddSingleton<IPerVehicleProjector, PerVehicleProjector>();
        builder.Services.AddSingleton<ICacheWriter, ValkeyCacheWriter>();
        builder.Services.AddSingleton<ICdcPipeline, CdcPipeline>();

        builder.Services.AddHostedService<VehicleTopicConsumer>();
        builder.Services.AddHostedService<PositionTopicConsumer>();

        return builder;
    }
}
