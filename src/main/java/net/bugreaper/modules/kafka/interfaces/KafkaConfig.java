package net.bugreaper.modules.kafka.interfaces;


import net.bugreaper.modules.kafka.Kafka;

public interface KafkaConfig {


    /**
     * Overrides the awaitMs default
     * @param awaitMs ms wait for first message
     * @return this instance for method chaining
     * @throws IllegalArgumentException on invalid setup
     */
    Kafka setAwaitMs(int awaitMs);

    /**
     * Overrides the maxConsumedMessages default
     * @param maxMessages max count of messages from end that will be consumed before break
     * @return this instance for method chaining
     * @throws IllegalArgumentException on invalid setup
     */
    Kafka setMaxConsumeMessages(int maxMessages);

    /**
     * Overrides the consumerTimeoutMs default
     * @param consumerTimeoutMs ms timeout for consumer
     * @return this instance for method chaining
     * @throws IllegalArgumentException on invalid setup
     */
    Kafka setConsumerTimeoutMs(int consumerTimeoutMs);

    /**
     * Set switch for unique consumer groupId
     * @param unique (true = unique, false = not unique)
     * @return this instance for method chaining
     */
    Kafka setUniqueConsumer(boolean unique);

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
