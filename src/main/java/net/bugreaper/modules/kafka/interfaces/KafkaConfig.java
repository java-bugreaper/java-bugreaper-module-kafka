package net.bugreaper.modules.kafka.interfaces;


import net.bugreaper.modules.kafka.Kafka;

/**
 * Interface that defines helper configuration methods for helper operations.
 * Validates that all required methods are implemented.
 */
public interface KafkaConfig {


    /**
     * Configures the global await timeout for assertions and operations that use await.
     *
     * @param awaitMs await timeout in milliseconds
     * @return this instance for method chaining
     * @throws IllegalArgumentException if the provided timeout is invalid or less than 200 milliseconds
     */
    Kafka setAwaitMs(int awaitMs);

    /**
     * Sets the maximum number of latest messages to consume.
     *
     * @param maxMessages maximum number of messages to consume from the latest offset
     * @return this instance for method chaining
     * @throws IllegalArgumentException if maxMessages is less than 1
     */
    Kafka setMaxConsumeMessages(int maxMessages);

    /**
     * Sets the consumer timeout.
     *
     * @param consumerTimeoutMs maximum consumer execution time in milliseconds before interruption
     * @return this instance for method chaining
     * @throws IllegalArgumentException if the value is invalid
     */
    Kafka setConsumerTimeoutMs(int consumerTimeoutMs);

    /**
     * Enables or disables generating a unique consumer group ID.
     *
     * @param unique {@code true} to generate a unique consumer group ID,
     *               {@code false} to use the default group ID
     * @return this instance for method chaining
     */
    Kafka setUniqueConsumer(boolean unique);

    /**
     * Sets the message ordering behavior.
     *
     * <p>Controls whether consumed messages are returned in reverse order
     * (newest messages first).</p>
     *
     * @param reverseMessages {@code true} to return messages in reverse order,
     *                        {@code false} to keep the original order
     * @return this instance for method chaining
     */
    Kafka setReverseMessages(boolean reverseMessages);


    /**
     * Returns and logs (at INFO level) a human-readable summary of all resolved
     * configuration values.
     * <p>
     * The summary includes values loaded from the YAML configuration file as well as
     * any fields overridden programmatically after construction. Optional fields that
     * were not present in the configuration and resolved via default values may also
     * be included.
     *
     * @return String with summary
     */
    String getConfigSummary();

}
