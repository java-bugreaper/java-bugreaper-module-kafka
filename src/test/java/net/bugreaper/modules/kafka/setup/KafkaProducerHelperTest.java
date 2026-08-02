package net.bugreaper.modules.kafka.setup;

import net.bugreaper.modules.kafka.exceptions.KafkaHelperException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class KafkaProducerHelperTest {

    private KafkaProducerHelper createHelperWithProducer(
            KafkaProducer<String, String> producer
    ) throws Exception {

        KafkaProducerHelper helper = new KafkaProducerHelper("localhost:9092");

        Field field = KafkaProducerHelper.class.getDeclaredField("kafkaProducer");
        field.setAccessible(true);
        field.set(helper, producer);

        return helper;
    }

    @Test
    void sendingMessageInterruptedTest() throws Exception {
        KafkaProducer<String, String> producer = mock(KafkaProducer.class);
        Future<RecordMetadata> future = mock(Future.class);

        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>("test-topic", "key", "value");

        when(producer.send(producerRecord))
                .thenReturn(future);

        when(future.get())
                .thenThrow(new InterruptedException());

        KafkaProducerHelper helper = createHelperWithProducer(producer);

        KafkaHelperException exception = assertThrows(
                KafkaHelperException.class,
                () -> helper.sending("test-topic", producerRecord)
        );

        assertEquals(
                "INTERRUPTED! Failed to send message to topic: 'test-topic'",
                exception.getMessage()
        );

        assertInstanceOf(InterruptedException.class, exception.getCause());

        assertTrue(Thread.currentThread().isInterrupted());

        // cleanup interrupt flag for other tests
        Thread.interrupted();
    }

    @Test
    void sendingMessageExecutionFailedTest() throws Exception {
        KafkaProducer<String, String> producer = mock(KafkaProducer.class);
        Future<RecordMetadata> future = mock(Future.class);

        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>("test-topic", "key", "value");

        when(producer.send(producerRecord))
                .thenReturn(future);

        when(future.get())
                .thenThrow(new ExecutionException(
                        new RuntimeException("Kafka error")
                ));

        KafkaProducerHelper helper = createHelperWithProducer(producer);

        KafkaHelperException exception = assertThrows(
                KafkaHelperException.class,
                () -> helper.sending("test-topic", producerRecord)
        );

        assertEquals(
                "Failed to send message to topic: 'test-topic'",
                exception.getMessage()
        );

        assertInstanceOf(ExecutionException.class, exception.getCause());
    }

}
