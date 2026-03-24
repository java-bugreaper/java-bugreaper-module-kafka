package net.bugreaper.modules.kafka;

import net.bugreaper.modules.kafka.exceptions.KafkaHelperException;
import org.awaitility.core.ConditionTimeoutException;
import org.hamcrest.MatcherAssert;
import org.hamcrest.core.StringContains;
import org.junit.jupiter.api.Test;
import testcontainers.KafkaSetup;

import java.text.MessageFormat;

import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;


class KafkaExceptionsTests {


    private final KafkaSetup setup = KafkaSetup.getInstance();
    private final Kafka kafkaCustom =
            setup.getKafka()
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
                is(MessageFormat.format("Topic <{0}> not exist", topic)));
    }

    @Test
    void grubNotExistingTopicTest() {
        String topic = "topicNotExist2";

        Throwable exception = assertThrows(KafkaHelperException.class, () ->
                kafkaCustom.grabMessagesFromTopic(topic));

        MatcherAssert.assertThat(
                exception.getMessage(),
                is(MessageFormat.format("Topic <{0}> not exist", topic)));
    }

    @Test
    void countNotExistingTopicTest() {
        String topic = "topicNotExist2";

        Throwable exception = assertThrows(KafkaHelperException.class, () ->
                kafkaCustom.seeCountMessagesInTopicExactly(topic, 1));

        MatcherAssert.assertThat(
                exception.getMessage(),
                is(MessageFormat.format("Topic <{0}> not exist", topic)));
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
                StringContains.containsString("No messages received within 400 milliseconds"));
    }

    @Test
    void grabFromTopicConsumerTimeoutTest() {

        String topic = "consumeTimeout";

        final Kafka kafkaConsume = setup.getKafka().setConsumerTimeoutMs(1);

        kafkaConsume.createTopic(topic);
        kafkaConsume.purgeTopic(topic);

        for (int i = 0; i < 100; i++) {
            kafkaConsume.sendToTopic(topic, "some message");
        }

        Throwable exception = assertThrows(KafkaHelperException.class, () ->
                kafkaConsume.grabMessagesFromTopic(topic));

        MatcherAssert.assertThat(
                exception.getMessage(),
                StringContains.containsString("Consuming stopped, time limit reached: 1 millisecond (use setConsumerTimeoutMs or config max-consumer-timeout if need)"));
    }

    @Test
    void createTopicWrongPartitionTest() {

        String topic = "wrongPart";

        assertThrows(KafkaHelperException.class, () ->
                kafkaCustom.createTopic(topic, 0));
    }

    @Test
    void deleteWrongTopicTest() {
        String topic = "";

        assertThrows(KafkaHelperException.class, () ->
                kafkaCustom.deleteTopic(topic));
    }

    @Test
    void deleteNotExistTopicTest() {
        String topic = "not_exist_test";

        assertThrows(KafkaHelperException.class, () ->
                kafkaCustom.deleteTopic(topic));
    }

}
