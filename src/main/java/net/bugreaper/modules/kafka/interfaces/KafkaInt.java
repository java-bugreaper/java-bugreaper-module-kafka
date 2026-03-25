package net.bugreaper.modules.kafka.interfaces;

import net.bugreaper.core.assertable.AssertableStringList;
import net.bugreaper.modules.kafka.exceptions.KafkaHelperException;
import org.awaitility.core.ConditionTimeoutException;


public interface KafkaInt {

    //interaction

    /**
     * Create topic with one partition
     *
     * @param topic topic name
     * @throws KafkaHelperException if failed to create topic
     * <p> Only WARNING if topic already exists
     */
    void createTopic(String topic);

    /**
     * Create topic with multiple partitions
     *
     * @param topic topic name
     * @param partitionsCount partitions count
     * @throws KafkaHelperException if failed to create topic
     *                              <p> Only WARNING if topic already exists
     */
    void createTopic(String topic, int partitionsCount);

    /**
     * Delete topic
     *
     * @param topic topic name
     * @throws KafkaHelperException if failed to delete topic
     */
    void deleteTopic(String topic);

    /**
     * Purge topic (clean all partitions)
     *
     * @param topic topic name
     * @throws KafkaHelperException if failed to purge topic
     *
     */
    void purgeTopic(String topic);

    /**
     * Push message to topic
     * <p>Can be lag for consumer to see message immediately - so await is used for consumer </p>
     *
     * @param topic   topic name
     * @param message    String with message
     * @throws KafkaHelperException if failed to push message.
     * <p> Create topic if not exists
     */
    void sendToTopic(String topic, String message);

    /**
     * Push message to topic (with key)
     * <p>Can be lag for consumer to see message immediately - so await is used for consumer </p>
     *
     * @param topic   topic name
     * @param message    String with message
     * @param key The key that will be included in the record
     * @throws KafkaHelperException if failed to push message.
     * <p> Create topic if not exists
     */
    void sendToTopicWithKey(String topic, String key, String message);

    //get data

    /**
     * Grab messages to list by consumer (not commit messages!)
     * <p><b>wait for first message</b>
     * <p> max list of messages set in config (grab last messages)</p>
     *
     * @param topic topic name
     * @return  {@link AssertableStringList}
     *
     * @throws ConditionTimeoutException if topic is empty (await)
     * @throws KafkaHelperException on topic not exist or consumer timeout reached
     * <p> EXAMPLE:
     * {@code grabMessagesFromTopic("test_queue").seeListAnyEquals("my message") }
     * <p> work with static membership for remove re-balancing lag after reconnect consumer
     */
    AssertableStringList grabMessagesFromTopic(String topic);

    /**
     * Return list of all topics names
     *
     * @return  {@link AssertableStringList}
     * <p> EXAMPLE:
     * {@code getAllTopicsNames().seeListAnyContains("test_topic")}
     */
    AssertableStringList getAllTopicsNames();

    /**
     * Count number of messages in topic
     *
     * @param topic topic name
     * @return an int with messages count
     * @throws KafkaHelperException on topic not exist
     */
    int getTopicMessageCount(String topic);

    /**
     * Count number of partitions in topic
     *
     * @param topic topic name
     * @return an int with partitions count
     * @throws KafkaHelperException on topic not exist
     */
    int getPartitionsCount(String topic);

}
