package net.bugreaper.modules.kafka;

import net.bugreaper.modules.kafka.logger.Log;
import org.junit.jupiter.api.Test;
import testcontainers.KafkaSetup;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;


@SuppressWarnings("squid:S2699")
class KafkaOtherTests {

    private final KafkaSetup setup = KafkaSetup.getInstance();


    @Test
    void multipleConsumersTest() {
        Kafka kafka1 = setup.getKafka().setUniqueConsumer(false);
        Kafka kafka2 = setup.getKafka().setUniqueConsumer(true);
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
