namespace Nstech.Worker.Services;

public interface IOrderingGuard
{
    long CoalesceLsn(long? lsn);

    long ParseEpochMilliseconds(string? timestamp);
}

public sealed class OrderingGuard : IOrderingGuard
{
    public long CoalesceLsn(long? lsn) => lsn ?? 0;

    public long ParseEpochMilliseconds(string? timestamp)
    {
        if (string.IsNullOrWhiteSpace(timestamp))
            throw new FormatException("Timestamp is required for position ordering.");

        return DateTimeOffset.Parse(
                timestamp,
                System.Globalization.CultureInfo.InvariantCulture,
                System.Globalization.DateTimeStyles.AssumeUniversal | System.Globalization.DateTimeStyles.AdjustToUniversal)
            .ToUnixTimeMilliseconds();
    }
}
