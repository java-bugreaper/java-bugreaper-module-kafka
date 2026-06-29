package net.bugreaper.modules.kafka.setup;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class KafkaConsumerHelperTest {

    private static final String EXPECTED_HOST = "localhost:9092";


    @Test
    void shutdownHookClosesConsumer() throws Exception {
        KafkaConsumerHelper helper = new TestKafkaConsumerHelper();
        CloseTrackingKafkaConsumer consumer = new CloseTrackingKafkaConsumer(false);
        createdConsumers(helper).add(consumer);

        Thread hook = ((TestKafkaConsumerHelper) helper).consumerShutdownHook();

        assertEquals("kafka-consumer-shutdown", hook.getName());

        hook.run();

        assertTrue(consumer.closed);
    }

    @Test
    void shutdownHookIgnoresConsumerCloseException() throws Exception {
        KafkaConsumerHelper helper = new TestKafkaConsumerHelper();
        CloseTrackingKafkaConsumer consumer = new CloseTrackingKafkaConsumer(true);
        createdConsumers(helper).add(consumer);

        assertDoesNotThrow(() -> ((TestKafkaConsumerHelper) helper).consumerShutdownHook().run());
        assertTrue(consumer.closed);
    }

    @Test
    void shutdownHookIgnoresMissingConsumer() throws Exception {
        KafkaConsumerHelper helper = new TestKafkaConsumerHelper();
        createdConsumers(helper).clear();

        assertDoesNotThrow(() -> ((TestKafkaConsumerHelper) helper).consumerShutdownHook().run());
    }

    @Test
    void consumerRebuildsWhenUniqueConsumerGroupChanges() throws Exception {
        TestKafkaConsumerHelper helper = new TestKafkaConsumerHelper();
        KafkaConsumer<String, String> initialConsumer = helper.consumer();

        helper.uniqueConsumerGroup = true;

        KafkaConsumer<String, String> rebuiltConsumer = helper.consumer();

        assertNotSame(initialConsumer, rebuiltConsumer);
        assertFalse(createdConsumers(helper).contains(initialConsumer));
        assertTrue(createdConsumers(helper).contains(rebuiltConsumer));
    }

    @SuppressWarnings("unchecked")
    private Set<KafkaConsumer<String, String>> createdConsumers(KafkaConsumerHelper helper) throws Exception {
        Field createdConsumersField = KafkaConsumerHelper.class.getDeclaredField("createdConsumers");
        createdConsumersField.setAccessible(true);
        return (Set<KafkaConsumer<String, String>>) createdConsumersField.get(helper);
    }

    private static class TestKafkaConsumerHelper extends KafkaConsumerHelper {
        TestKafkaConsumerHelper() {
            super(EXPECTED_HOST);
        }

        @Override
        Thread createShutdownHook() {
            return new Thread(() -> {
            }, "test-noop-kafka-consumer-shutdown");
        }

        Thread consumerShutdownHook() {
            return super.createShutdownHook();
        }
    }

    private static class CloseTrackingKafkaConsumer extends KafkaConsumer<String, String> {
        private final boolean failOnClose;
        private boolean closed;

        CloseTrackingKafkaConsumer(boolean failOnClose) {
            super(consumerProperties());
            this.failOnClose = failOnClose;
        }

        @Override
        public void close() {
            closed = true;
            if (failOnClose) {
                throw new IllegalStateException("close failed");
            }
        }

        private static Properties consumerProperties() {
            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, EXPECTED_HOST);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            return props;
        }
    }
}
