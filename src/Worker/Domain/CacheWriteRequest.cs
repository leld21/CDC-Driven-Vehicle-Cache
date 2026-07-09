namespace Nstech.Worker.Domain;

public enum CacheWriteKind
{
    VehicleUpsert,
    VehicleDelete,
    PositionUpdate,
    TombstoneNoOp,
    Ignored
}

public sealed record CacheWriteRequest(
    CacheWriteKind Kind,
    long VehicleId,
    long? Lsn = null,
    string? State = null,
    long? StateUpdatedAtMs = null,
    long? RecordedAtMs = null,
    long? PositionLsn = null,
    double? Lat = null,
    double? Lng = null,
    string? Reason = null);

public enum WriteOutcome
{
    Applied,
    SkippedStale,
    NoOp,
    Ignored
}
