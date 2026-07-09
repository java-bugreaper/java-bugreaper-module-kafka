package net.bugreaper.modules.kafka;

import ch.qos.logback.classic.Level;
import net.bugreaper.core.utils.LogWatcher;
import net.bugreaper.modules.kafka.logger.Log;
import org.hamcrest.MatcherAssert;

import org.hamcrest.core.StringContains;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;
import testcontainers.KafkaContainerSetup;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import static org.hamcrest.core.StringRegularExpression.matchesRegex;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;


@SuppressWarnings("squid:S2699")
@Isolated
class KafkaOtherTests extends KafkaContainerSetup {
    private LogWatcher logWatcher;
    @BeforeEach
    void setup() {
        logWatcher = new LogWatcher("org.apache.kafka.clients.Metadata", Level.INFO);
    }

    @AfterEach
    void teardown() {
        logWatcher.detach();
    }


    @Test
    void multipleConsumersTest() {
        Kafka kafka1 = getKafka().setUniqueConsumer(false);
        Kafka kafka2 = getKafka().setUniqueConsumer(true);

        String topic = "multipleConsumers";

        kafka1.createTopic(topic, 3);

        assertEquals(
                3,
                kafka1.getPartitionsCount(topic));

        kafka1.sendToTopic(topic, "test_1");
        kafka2.sendToTopic(topic, "test_2");

        kafka1.seeCountMessagesInTopicExactly(topic, 2);
        kafka2.seeCountMessagesInTopicExactly(topic, 2);

        kafka1.seeMessagesHaveEqualText(topic, "test_2");
        kafka1.seeMessagesHaveEqualText(topic, "test_1");

        kafka2.seeMessagesHaveEqualText(topic, "test_1");

        //check consumer default
        MatcherAssert.assertThat(
                logWatcher.getLoggedEvents(Level.INFO).toString(),
                StringContains.containsString("groupId=bugreaper-consumer-group]"));

        //check unique
        MatcherAssert.assertThat(
                logWatcher.getLoggedEvents(Level.INFO).toString(),
                matchesRegex(".*groupId=bugreaper-consumer-group-[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\\].*"));

        kafka1.seeCountMessagesInTopicExactly(topic, 2);
        kafka2.seeCountMessagesInTopicExactly(topic, 2);

        kafka2.purgeTopic(topic);

        kafka2.seeTopicIsEmpty(topic);
        kafka1.seeTopicIsEmpty(topic);
    }

    @Test
    void staticClassLogTest() throws NoSuchMethodException {
        Constructor<Log> constructor = Log.class.getDeclaredConstructor();
        constructor.setAccessible(true);

        InvocationTargetException thrown = assertThrows(InvocationTargetException.class, constructor::newInstance);

        Throwable cause = thrown.getCause();
        assert (cause instanceof IllegalStateException);
        assert ("Utility class".equals(cause.getMessage()));
    }

}
