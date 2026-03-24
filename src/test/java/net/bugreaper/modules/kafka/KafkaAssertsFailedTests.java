package net.bugreaper.modules.kafka;

import org.awaitility.core.ConditionTimeoutException;
import org.hamcrest.MatcherAssert;
import org.hamcrest.core.StringContains;
import org.junit.jupiter.api.Test;
import testcontainers.KafkaSetup;

import static org.junit.jupiter.api.Assertions.assertThrows;


class KafkaAssertsFailedTests {


    private final KafkaSetup setup = KafkaSetup.getInstance();
    private final Kafka kafka =
            setup.getKafka().setAwaitMs(400);

    @Test
    void purgeTopicAndCheckMessageAssertTest() {

        String topic = "topicForPurge";
        String message = "test1";

        kafka.sendToTopic(topic, message);
        kafka.purgeTopic(topic);

        Throwable exception = assertThrows(ConditionTimeoutException.class, () ->
                kafka.seeMessagesHaveEqualText(topic, message));

        MatcherAssert.assertThat(
                exception.getMessage(),
                StringContains.containsString("No messages received within 400 milliseconds"));
    }


    @Test
    void countExactlyFailed0Test() {

        String topic = "count0";
        kafka.createTopic(topic);
        kafka.purgeTopic(topic);

        Throwable exception = assertThrows(AssertionError.class, () ->
                kafka.seeCountMessagesInTopicExactly(topic, 2));

        MatcherAssert.assertThat(
                exception.getMessage(),
                StringContains.containsString("Count messages from topic <count0> expected to be EXACTLY <2> but got <0> within 400 milliseconds"));
    }

    @Test
    void countExactlyFailed1Test() {

        Kafka kafkaCustom =
                setup.getKafka()
                        .setAwaitMs(1000);

        String topic = "count1";
        kafkaCustom.createTopic(topic);
        kafkaCustom.purgeTopic(topic);
        kafkaCustom.sendToTopic(topic, "some message");

        Throwable exception = assertThrows(AssertionError.class, () ->
                kafkaCustom.seeCountMessagesInTopicExactly(topic, 2));

        MatcherAssert.assertThat(
                exception.getMessage(),
                StringContains.containsString("Count messages from topic <count1> expected to be EXACTLY <2> but got <1> within 1 second"));
    }

    @Test
    void seeTopicIsEmptyFailedTest() {

        String topic = "notEmptyTopic";
        kafka.createTopic(topic);
        kafka.purgeTopic(topic);
        kafka.sendToTopic(topic, "some message");

        Throwable exception = assertThrows(AssertionError.class, () ->
                kafka.seeTopicIsEmpty(topic));

        MatcherAssert.assertThat(
                exception.getMessage(),
                StringContains.containsString("Topic <notEmptyTopic> expected to be empty but has <1> messages within 400 milliseconds"));
    }

    @Test
    void seeTopicIsNotEmptyFailedTest() {

        String topic = "emptyTopic";
        kafka.createTopic(topic);
        kafka.purgeTopic(topic);

        Throwable exception = assertThrows(AssertionError.class, () ->
                kafka.seeTopicIsNotEmpty(topic));

        MatcherAssert.assertThat(
                exception.getMessage(),
                StringContains.containsString("Topic <emptyTopic> expected to be not empty but has no messages within 400 milliseconds"));
    }

}
