package net.bugreaper.modules.kafka;

import org.awaitility.core.ConditionTimeoutException;
import org.hamcrest.MatcherAssert;
import org.hamcrest.core.StringContains;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;
import testcontainers.KafkaContainerSetup;
import static org.junit.jupiter.api.Assertions.assertThrows;


@Isolated
class KafkaAssertsFailedTests extends KafkaContainerSetup {

    private final Kafka kafka = getKafka().setAwaitMs(400);

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
                StringContains.containsString("No messages were received from topic 'topicForPurge' within 400 milliseconds"));
    }


    @Test
    void countExactlyFailed0Test() {

        String topic = "count0";
        kafka.createTopic(topic);
        kafka.purgeTopic(topic);

        Throwable exception = assertThrows(AssertionError.class, () ->
                kafka.seeMessagesCountInTopicExactly(topic, 2));

        MatcherAssert.assertThat(
                exception.getMessage(),
                StringContains.containsString("Expected EXACTLY <2> messages in topic 'count0', but got <0> within 400 milliseconds"));
    }

    @Test
    void countExactlyFailed1Test() {

        Kafka kafkaCustom = getKafka().setAwaitMs(1000);

        String topic = "count1";
        kafkaCustom.createTopic(topic);
        kafkaCustom.purgeTopic(topic);
        kafkaCustom.sendToTopic(topic, "some message");

        Throwable exception = assertThrows(AssertionError.class, () ->
                kafkaCustom.seeMessagesCountInTopicExactly(topic, 2));

        MatcherAssert.assertThat(
                exception.getMessage(),
                StringContains.containsString("Expected EXACTLY <2> messages in topic 'count1', but got <1> within 1 second"));
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
                StringContains.containsString("Expected topic 'notEmptyTopic' to be empty, but got <1> messages within 400 milliseconds"));
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
                StringContains.containsString("Expected topic 'emptyTopic' to be empty, but got no messages within 400 milliseconds"));
    }

}
