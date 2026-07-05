using System.Text.Json;
using Nstech.Worker.Cdc;
using Nstech.Worker.Domain;

namespace Nstech.Worker.Services;

public interface IPerVehicleProjector
{
    CacheWriteRequest Project(CdcStream stream, string? valueJson, string? keyJson);
}

public sealed class PerVehicleProjector(IOrderingGuard orderingGuard) : IPerVehicleProjector
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true
    };

    public CacheWriteRequest Project(CdcStream stream, string? valueJson, string? keyJson)
    {
        if (valueJson is null)
            return ProjectTombstone(stream, keyJson);

        return stream switch
        {
            CdcStream.Vehicles => ProjectVehicle(valueJson),
            CdcStream.Positions => ProjectPosition(valueJson),
            _ => new CacheWriteRequest(CacheWriteKind.Ignored, 0, Reason: $"unknown stream {stream}")
        };
    }

    private CacheWriteRequest ProjectTombstone(CdcStream stream, string? keyJson)
    {
        if (stream != CdcStream.Vehicles)
            return new CacheWriteRequest(CacheWriteKind.Ignored, 0, Reason: "tombstone on non-vehicle topic");

        var vehicleId = ParseKeyVehicleId(keyJson);
        return new CacheWriteRequest(CacheWriteKind.TombstoneNoOp, vehicleId, Reason: "debezium tombstone");
    }

    private CacheWriteRequest ProjectVehicle(string valueJson)
    {
        var envelope = JsonSerializer.Deserialize<DebeziumEnvelope<VehicleRow, VehicleRow>>(valueJson, JsonOptions)
            ?? throw new InvalidOperationException("Failed to deserialize vehicle envelope.");

        var op = envelope.Op ?? string.Empty;
        var lsn = orderingGuard.CoalesceLsn(envelope.Source?.Lsn);

        return op switch
        {
            "c" or "u" or "r" when envelope.After is not null => new CacheWriteRequest(
                CacheWriteKind.VehicleUpsert,
                envelope.After.Id,
                Lsn: lsn,
                State: envelope.After.State ?? string.Empty,
                StateUpdatedAtMs: envelope.TsMs ?? 0),
            "d" when envelope.Before is not null => new CacheWriteRequest(
                CacheWriteKind.VehicleDelete,
                envelope.Before.Id,
                Lsn: lsn,
                Reason: "vehicle delete (before.id + source.lsn only)"),
            _ => new CacheWriteRequest(CacheWriteKind.Ignored, 0, Reason: $"unsupported vehicle op={op}")
        };
    }

    private CacheWriteRequest ProjectPosition(string valueJson)
    {
        var envelope = JsonSerializer.Deserialize<DebeziumEnvelope<PositionRow, PositionRow>>(valueJson, JsonOptions)
            ?? throw new InvalidOperationException("Failed to deserialize position envelope.");

        var op = envelope.Op ?? string.Empty;
        if (op is not ("c" or "r") || envelope.After is null)
            return new CacheWriteRequest(CacheWriteKind.Ignored, 0, Reason: $"unsupported position op={op}");

        return new CacheWriteRequest(
            CacheWriteKind.PositionUpdate,
            envelope.After.VehicleId,
            RecordedAtMs: orderingGuard.ParseEpochMilliseconds(envelope.After.RecordedAt),
            PositionLsn: orderingGuard.CoalesceLsn(envelope.Source?.Lsn),
            Lat: envelope.After.Lat,
            Lng: envelope.After.Lng);
    }

    private static long ParseKeyVehicleId(string? keyJson)
    {
        if (string.IsNullOrWhiteSpace(keyJson))
            throw new InvalidOperationException("Tombstone message is missing a key.");

        var key = JsonSerializer.Deserialize<DebeziumKey>(keyJson, JsonOptions)
            ?? throw new InvalidOperationException("Failed to deserialize tombstone key.");

        return key.Id;
    }
}
