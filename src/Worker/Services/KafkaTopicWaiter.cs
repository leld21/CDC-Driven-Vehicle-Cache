using Confluent.Kafka;

namespace Nstech.Worker.Services;

public static class KafkaTopicWaiter
{
    public static async Task WaitForTopicAsync(
        string bootstrapServers,
        string topic,
        ILogger logger,
        CancellationToken cancellationToken)
    {
        using var admin = new AdminClientBuilder(new AdminClientConfig
        {
            BootstrapServers = bootstrapServers
        }).Build();

        for (var attempt = 1; !cancellationToken.IsCancellationRequested; attempt++)
        {
            try
            {
                var metadata = admin.GetMetadata(topic, TimeSpan.FromSeconds(5));
                var topicMeta = metadata.Topics.FirstOrDefault(t => t.Topic == topic);
                if (topicMeta is not null && topicMeta.Error.Code == ErrorCode.NoError && topicMeta.Partitions.Count > 0)
                {
                    logger.LogInformation("Kafka topic {Topic} is available", topic);
                    return;
                }
            }
            catch (Exception ex)
            {
                logger.LogDebug(ex, "Metadata poll failed for {Topic}", topic);
            }

            logger.LogWarning(
                "Kafka topic {Topic} not ready yet (attempt {Attempt}). " +
                "Ensure Debezium is running: just connector-register",
                topic,
                attempt);

            await Task.Delay(TimeSpan.FromSeconds(3), cancellationToken).ConfigureAwait(false);
        }

        throw new OperationCanceledException($"Stopped waiting for topic {topic}.");
    }
}
