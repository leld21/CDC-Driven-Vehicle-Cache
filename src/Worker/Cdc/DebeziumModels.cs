using System.Text.Json.Serialization;

namespace Nstech.Worker.Cdc;

public sealed class DebeziumEnvelope<TAfter, TBefore>
{
    [JsonPropertyName("op")]
    public string? Op { get; init; }

    [JsonPropertyName("ts_ms")]
    public long? TsMs { get; init; }

    [JsonPropertyName("before")]
    public TBefore? Before { get; init; }

    [JsonPropertyName("after")]
    public TAfter? After { get; init; }

    [JsonPropertyName("source")]
    public DebeziumSource? Source { get; init; }
}

public sealed class DebeziumSource
{
    [JsonPropertyName("lsn")]
    public long? Lsn { get; init; }

    [JsonPropertyName("table")]
    public string? Table { get; init; }

    [JsonPropertyName("snapshot")]
    public string? Snapshot { get; init; }
}

public sealed class VehicleRow
{
    [JsonPropertyName("id")]
    public long Id { get; init; }

    [JsonPropertyName("state")]
    public string? State { get; init; }

    [JsonPropertyName("updated_at")]
    public string? UpdatedAt { get; init; }
}

public sealed class PositionRow
{
    [JsonPropertyName("id")]
    public long Id { get; init; }

    [JsonPropertyName("vehicle_id")]
    public long VehicleId { get; init; }

    [JsonPropertyName("lat")]
    public double Lat { get; init; }

    [JsonPropertyName("lng")]
    public double Lng { get; init; }

    [JsonPropertyName("recorded_at")]
    public string? RecordedAt { get; init; }
}

public sealed class DebeziumKey
{
    [JsonPropertyName("id")]
    public long Id { get; init; }
}

public enum CdcStream
{
    Vehicles,
    Positions
}
