package net.bugreaper.modules.kafka;

import org.junit.jupiter.api.Test;
import testcontainers.KafkaSetup;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class KafkaConfigureValidationTests {

    private final KafkaSetup setup = KafkaSetup.getInstance();
    private final Kafka kafka = setup.getKafka();

    @Test
    void configMaxMessagesReadTest() {

        Throwable exception = assertThrows(IllegalArgumentException.class, () ->
                kafka.setMaxConsumeMessages(0));

        assertEquals(
                "maxMessages too small (can`t bee less 1)",
                exception.getMessage());
    }

    @Test
    void configAwaitMsTest() {

        Throwable exception = assertThrows(IllegalArgumentException.class, () ->
                kafka.setAwaitMs(199));

        assertEquals(
                "awaitMs too small (can`t bee less 200ms)",
                exception.getMessage());
    }

    @Test
    void configConsumerTimeoutMsTest() {

        Throwable exception = assertThrows(IllegalArgumentException.class, () ->
                kafka.setConsumerTimeoutMs(0));

        assertEquals(
                "consumerTimeoutMs too small (can`t bee less 1ms)",
                exception.getMessage());
    }

}
