package net.bugreaper.modules.kafka.interfaces;

import net.bugreaper.modules.kafka.exceptions.KafkaHelperException;
import org.awaitility.core.ConditionTimeoutException;

/**
 * Interface describes methods responsible for helper assertions.
 * Validates that all required methods are implemented.
 */
public interface KafkaAsserts {


    /**
     * Asserts that the topic is empty.
     *
     * <p><b>Uses await.</b></p>
     *
     * @param topic topic name
     * @throws AssertionError if the assertion fails
     * @throws KafkaHelperException if the topic does not exist
     */
    void seeTopicIsEmpty(String topic);

    /**
     * Asserts that the topic is not empty
     *
     * <p><b>Uses await.</b></p>
     *
     * @param topic topic name
     * @throws AssertionError if the assertion fails
     * @throws KafkaHelperException if the topic does not exist
     */
    void seeTopicIsNotEmpty(String topic);

     /**
     * Asserts that the number of messages in the topic is exactly the expected count.
     *
     * <p><b>Uses await.</b></p>
     *
     * @param topic         topic name
     * @param expectedCount expected number of messages
     * @throws AssertionError if the assertion fails
      * @throws KafkaHelperException if the topic does not exist
     */
    void seeMessagesCountInTopicExactly(String topic, int expectedCount);

    /**
     * Asserts that at least one message in the topic is equal to the expected text.
     *
     * <p><b>Uses await until at least one message exists.</b></p>
     *
     * @param topic        topic name
     * @param expectedText expected message text
     * @throws AssertionError       if the assertion fails
     * @throws ConditionTimeoutException if the topic remains empty until the await timeout expires
     * @throws KafkaHelperException if the topic does not exist or consumer timeout is reached
     */
    void seeMessagesHaveEqualText(String topic, String expectedText);

    /**
     * Asserts that at least one of messages in topic contains the specified string.
     *
     * <p><b>Uses await until at least one message exists.</b></p>
     *
     * @param topic        topic name
     * @param expectedPart expected string part
     * @throws AssertionError       if the assertion fails
     * @throws ConditionTimeoutException if the topic remains empty until the await timeout expires
     * @throws KafkaHelperException if the topic does not exist or consumer timeout is reached
     */
    void seeMessagesContainText(String topic, String expectedPart);

    /**
     * * Asserts that at least one of messages in topic is equal to the specified JSON with strict array ordering.
     *
     * <p><b>Uses await until at least one message exists.</b></p>
     *
     * @param topic        topic name
     * @param expectedJson expected full JSON with strict array ordering
     * @throws AssertionError       if the assertion fails
     * @throws ConditionTimeoutException if the topic remains empty until the await timeout expires
     * @throws IllegalArgumentException if the provided data is not valid JSON
     * @throws KafkaHelperException if the topic does not exist or consumer timeout is reached
     */
    void seeMessagesHaveEqualJson(String topic, String expectedJson);

    /**
     * * Asserts that at least one of messages in topic contains the specified JSON without strict array ordering.
     *
     * <p><b>Uses await until at least one message exists.</b></p>
     *
     * @param topic            topic name
     * @param expectedJsonPart expected JSON subset (array order is ignored, but the number of elements must match if provided)
     * @throws AssertionError       if the assertion fails
     * @throws ConditionTimeoutException if the topic remains empty until the await timeout expires
     * @throws IllegalArgumentException if the provided data is not valid JSON
     * @throws KafkaHelperException if the topic does not exist or consumer timeout is reached
     */
    void seeMessagesContainJson(String topic, String expectedJsonPart);

}
