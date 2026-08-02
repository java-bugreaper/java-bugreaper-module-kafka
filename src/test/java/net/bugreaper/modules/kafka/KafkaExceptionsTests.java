package net.bugreaper.modules.kafka;

import net.bugreaper.modules.kafka.exceptions.KafkaHelperException;
import org.awaitility.core.ConditionTimeoutException;
import org.hamcrest.MatcherAssert;
import org.hamcrest.core.StringContains;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;
import testcontainers.KafkaContainerSetup;

import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;


@Isolated
class KafkaExceptionsTests extends KafkaContainerSetup {

    private final Kafka kafkaCustom = getKafka()
            .setAwaitMs(400)
            .setConsumerTimeoutMs(1000)
            .setMaxConsumeMessages(3);


    @Test
    void purgeNotExistingTopicTest() {
        String topic = "topicNotExist1";

        Throwable exception = assertThrows(KafkaHelperException.class, () ->
                kafkaCustom.purgeTopic(topic));

        MatcherAssert.assertThat(
                "Exception on purge not existing topic",
                exception.getMessage(),
                is("Topic 'topicNotExist1' does not exist"));
    }

    @Test
    void seeMessagesCountInTopicExactlyNotExistingTopicTest() {
        String topic = "topicNotExist1";

        Throwable exception = assertThrows(KafkaHelperException.class, () ->
                kafkaCustom.seeMessagesCountInTopicExactly(topic, 2));

        assertEquals(
                "Topic '%s' does not exist".formatted(topic),
                exception.getMessage());
    }

    @Test
    void seeTopicIsNotEmptyNotExistingTopicTest() {
        String topic = "topicNotExist1";

        Throwable exception = assertThrows(KafkaHelperException.class, () ->
                kafkaCustom.seeTopicIsNotEmpty(topic));

        assertEquals(
                "Topic '%s' does not exist".formatted(topic),
                exception.getMessage());
    }

    @Test
    void seeTopicIsEmptyNotExistingTopicTest() {
        String topic = "topicNotExist1";

        Throwable exception = assertThrows(KafkaHelperException.class, () ->
                kafkaCustom.seeTopicIsEmpty(topic));

        assertEquals(
                "Topic '%s' does not exist".formatted(topic),
                exception.getMessage());
    }

    @Test
    void grubNotExistingTopicTest() {
        String topic = "topicNotExist2";

        Throwable exception = assertThrows(KafkaHelperException.class, () ->
                kafkaCustom.grabMessagesFromTopic(topic));

        assertEquals(
                "Topic '%s' does not exist".formatted(topic),
                exception.getMessage());

    }


    @Test
    void countNotExistingTopicTest() {
        String topic = "topicNotExist2";

        Throwable exception = assertThrows(KafkaHelperException.class, () ->
                kafkaCustom.seeMessagesCountInTopicExactly(topic, 1));

        MatcherAssert.assertThat(
                exception.getMessage(),
                is("Topic 'topicNotExist2' does not exist"));
    }

    @Test
    void grabFromEmptyTopicTest() {

        String topic = "topicEmpty";

        kafkaCustom.createTopic(topic);
        kafkaCustom.purgeTopic(topic);

        Throwable exception = assertThrows(ConditionTimeoutException.class, () ->
                kafkaCustom.grabMessagesFromTopic(topic));

        MatcherAssert.assertThat(
                exception.getMessage(),
                StringContains.containsString("No messages were received from topic 'topicEmpty' within 400 milliseconds"));
    }

    @Test
    void grabFromTopicConsumerTimeoutTest() {

        String topic = "consumeTimeout";

        final Kafka kafkaConsume = getKafka().setConsumerTimeoutMs(1);

        kafkaConsume.createTopic(topic);
        kafkaConsume.purgeTopic(topic);

        for (int i = 0; i < 100; i++) {
            kafkaConsume.sendToTopic(topic, "some message");
        }

        Throwable exception = assertThrows(KafkaHelperException.class, () ->
                kafkaConsume.grabMessagesFromTopic(topic));

        MatcherAssert.assertThat(
                exception.getMessage(),
                StringContains.containsString("Consuming interrupted after reaching the time limit: 1 millisecond. Increase the timeout using setConsumerTimeoutMs() or the max-consumer-timeout configuration if needed."));
    }

    @Test
    void createTopicWrongPartitionTest() {

        String topic = "wrongPart";

        Throwable exception = assertThrows(KafkaHelperException.class, () ->
                kafkaCustom.createTopic(topic, 0));

        assertEquals(
                "Failed to create topic: 'wrongPart'",
                exception.getMessage());
    }

    @Test
    void deleteWrongTopicTest() {
        String topic = "";

        Throwable exception = assertThrows(KafkaHelperException.class, () ->
                kafkaCustom.deleteTopic(topic));

        assertEquals(
                "Failed to delete topic: ''",
                exception.getMessage());
    }

    @Test
    void deleteNotExistTopicTest() {
        String topic = "not_exist_test";

        Throwable exception = assertThrows(KafkaHelperException.class, () ->
                kafkaCustom.deleteTopic(topic));

        assertEquals(
                "Failed to delete topic: 'not_exist_test'",
                exception.getMessage());
    }

}
