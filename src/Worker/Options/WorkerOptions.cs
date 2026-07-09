namespace Nstech.Worker.Options;

public sealed class KafkaOptions
{
    public const string SectionName = "Kafka";

    public string BootstrapServers { get; set; } = "localhost:9092";
    public string GroupId { get; set; } = "nstech-worker";
    public string VehiclesTopic { get; set; } = "nstech.public.vehicles";
    public string PositionsTopic { get; set; } = "nstech.public.positions";
}

public sealed class ValkeyOptions
{
    public const string SectionName = "Valkey";

    public string ConnectionString { get; set; } = "localhost:6379";
}
