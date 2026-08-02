package net.bugreaper.modules.kafka.interfaces;

import net.bugreaper.core.assertable.AssertableStringList;
import net.bugreaper.modules.kafka.exceptions.KafkaHelperException;
import org.awaitility.core.ConditionTimeoutException;

/**
 * Interface defines methods for facilitating helper interactions.
 * Validates that all required methods are implemented.
 */
public interface KafkaInt {

    //interaction

    /**
     * Creates a topic with one partition.
     *
     * @param topic topic name
     * @throws KafkaHelperException if topic creation fails
     *
     * <p>A warning is logged if the topic already exists.</p>
     */
    void createTopic(String topic);

    /**
     * Creates a topic with multiple partitions.
     *
     * @param topic            topic name
     * @param partitionsCount  number of partitions
     * @throws KafkaHelperException if topic creation fails
     *
     * <p>A warning is logged if the topic already exists.</p>
     */
    void createTopic(String topic, int partitionsCount);

    /**
     * Deletes a topic.
     *
     * @param topic topic name
     * @throws KafkaHelperException if topic deletion fails or the topic does not exist
     */
    void deleteTopic(String topic);

    /**
     * Purges a topic by removing messages from all partitions.
     *
     * @param topic topic name
     * @throws KafkaHelperException if topic purge fails or the topic does not exist
     */
    void purgeTopic(String topic);

    /**
     * Sends a message to a topic.
     *
     * <p>There may be a delay before the consumer can see the message,
     * therefore await is used when consuming messages.</p>
     *
     * @param topic   topic name
     * @param message message content
     * @throws KafkaHelperException if sending the message fails
     *
     * <p>The topic is created automatically if it does not exist.</p>
     */
    void sendToTopic(String topic, String message);

    /**
     * Sends a message with a key to a topic.
     *
     * <p>There may be a delay before the consumer can see the message</p>
     *
     * @param topic   topic name
     * @param key     message key included in the Kafka record
     * @param message message content
     * @throws KafkaHelperException if sending the message fails
     *
     * <p>The topic is created automatically if it does not exist.</p>
     */
    void sendToTopicWithKey(String topic, String key, String message);

    //get data

    /**
     * Grabs messages from a topic using a consumer without committing offsets.
     *
     * <p><b>Uses await until at least one message exists in the topic.</b></p>
     *
     * <p>The maximum number of messages is configured globally
     * (only the latest messages are grabbed).</p>
     *
     * <p>Messages are returned in reverse order (newest messages first)
     * to optimize processing when working with large amounts of data.</p>
     *
     * <p>Uses static consumer membership to reduce consumer rebalancing delays
     * after reconnecting.</p>
     *
     * <p>Example:
     * {@code grabMessagesFromTopic("test_queue").seeListAnyEquals("my message")}
     * </p>
     *
     * @param topic topic name
     * @return {@link AssertableStringList}
     * @throws ConditionTimeoutException if the topic remains empty until the await timeout expires
     * @throws KafkaHelperException if the topic does not exist or the consumer timeout is reached
     */
    AssertableStringList grabMessagesFromTopic(String topic);

    /**
     * Grabs messages from a topic using a consumer without committing offsets.
     *
     * <p><b>Uses await until at least one message exists in the topic.</b></p>
     *
     * <p>The maximum number of messages is configured globally
     * (only the latest messages are grabbed).</p>
     *
     * <p>Messages are returned in reverse order (newest messages first)
     * to optimize processing when working with large amounts of data.</p>
     *
     * <p>Uses static consumer membership to reduce consumer rebalancing delays
     * after reconnecting.</p>
     *
     * <p>Example:
     * {@code grabMessagesFromTopic("test_topic", "my-key").seeListAnyEquals("my message")}
     * </p>
     *
     * @param topic topic name
     * @param key   message key to filter consumed records
     * @return {@link AssertableStringList}
     * @throws ConditionTimeoutException if the topic remains empty until the await timeout expires
     * @throws KafkaHelperException if the topic does not exist or the consumer timeout is reached
     */
    AssertableStringList grabMessagesFromTopic(String topic, String key);

    /**
     * Returns a list of all topic names.
     *
     * <p>Example:
     * {@code getAllTopicsNames().seeListAnyContains("test_topic")}
     * </p>
     *
     * @return {@link AssertableStringList}
     */
    AssertableStringList getAllTopicsNames();

    /**
     * Returns the number of messages in a topic.
     *
     * @param topic topic name
     * @return number of messages in the topic
     * @throws KafkaHelperException if the topic does not exist
     */
    int getMessagesCountInTopic(String topic);

    /**
     * Returns the number of partitions in a topic.
     *
     * @param topic topic name
     * @return number of partitions in the topic
     * @throws KafkaHelperException if the topic does not exist
     */
    int getPartitionsCount(String topic);

}
