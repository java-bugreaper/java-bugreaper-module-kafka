package net.bugreaper.modules.kafka.interfaces;

import net.bugreaper.modules.kafka.exceptions.KafkaHelperException;

/**
 * Interface describes methods responsible for helper assertions.
 * Validates that all required methods are implemented.
 */
public interface KafkaAsserts {


    /**
     * Assert that topic is empty
     * <p><b>with await</b>
     *
     * @param topic topic name
     * @throws AssertionError on assert fail
     */
    void seeTopicIsEmpty(String topic);

    /**
     * Assert that topic is not empty
     * <p><b>with await</b>
     *
     * @param topic topic name
     * @throws AssertionError on assert fail
     */
    void seeTopicIsNotEmpty(String topic);

    /**
     * Assert that count of messages in topic is equal to expected
     * <p><b>with await</b>
     *
     * @param topic         topic name
     * @param expectedCount expected count
     * @throws AssertionError on assert fail
     * @throws KafkaHelperException      on topic not exist
     */
    void seeCountMessagesInTopicExactly(String topic, int expectedCount);

    /**
     * Assert that at least one of messages in topic equal to string
     * <p><b>wait for first message</b>
     *
     * @param topic        topic name
     * @param expectedText expected string
     * @throws AssertionError       on assert fail
     * @throws KafkaHelperException on topic not exist or consumer timeout reached
     */
    void seeMessagesHaveEqualText(String topic, String expectedText);

    /**
     * Assert that at least one of messages in topic contains string
     * <p><b>wait for first message</b>
     *
     * @param topic        topic name
     * @param expectedPart expected string part
     * @throws AssertionError       on assert fail
     * @throws KafkaHelperException on topic not exist or consumer timeout reached
     */
    void seeMessagesContainText(String topic, String expectedPart);

    /**
     * Assert that at least one of messages in topic equal expected JSON
     * <p><b>wait for first message</b>
     *
     * @param topic        topic name
     * @param expectedJson expected JSON
     * @throws AssertionError           on assert fail
     * @throws IllegalArgumentException on wrong JSON type
     * @throws KafkaHelperException     on topic not exist or consumer timeout reached
     */
    void seeMessagesHaveEqualJson(String topic, String expectedJson);

    /**
     * Assert that at least messages in topic contains JSON (part OR equal)
     * <p><b>wait for first message</b>
     *
     * @param topic            topic name
     * @param expectedJsonPart expected JSON part
     * @throws AssertionError           on assert fail
     * @throws IllegalArgumentException on wrong JSON type
     * @throws KafkaHelperException     on topic not exist or consumer timeout reached
     */
    void seeMessagesContainJson(String topic, String expectedJsonPart);

}
