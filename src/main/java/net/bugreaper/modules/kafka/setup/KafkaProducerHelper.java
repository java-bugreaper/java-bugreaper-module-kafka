package net.bugreaper.modules.kafka.setup;

import net.bugreaper.modules.kafka.exceptions.KafkaHelperException;
import net.bugreaper.modules.kafka.logger.Log;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static java.lang.Thread.currentThread;


/**
 * Kafka producer helper responsible for sending messages to Kafka topics.
 */
public class KafkaProducerHelper {

    private KafkaProducer<String, String> kafkaProducer;
    private final String bootStrapServer;

    public KafkaProducerHelper(String bootStrapServer) {
        this.bootStrapServer = bootStrapServer;
        createProducer();

        // Shutdown hook to close connect automatically when JVM exits
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                kafkaProducer.close();
                Log.LOGGER.debug("Kafka producer closed");
            } catch (Exception e) {
                Log.LOGGER.warn("Failed to close kafka producer", e);
            }
        }
                , "kafka-producer-shutdown"
        ));
    }

    private void createProducer() {
        Properties properties = createProducerProperties();
        kafkaProducer = new KafkaProducer<>(properties);
    }

    private Properties createProducerProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootStrapServer);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "bugreaper-producer-" + currentThread().getId());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return props;
    }

    public void send(String topic, String message) {
        sending(topic, new ProducerRecord<>(topic, message));
    }

    public void sendWithKey(String topic, String key, String message) {
        sending(topic, new ProducerRecord<>(topic, key, message));
    }


    public void sending(String topic, ProducerRecord<String, String> produceRecord) {
        try {
            kafkaProducer.send(produceRecord).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaHelperException("INTERRUPTED! Failed to send message to topic: '%s'".formatted(topic), e);
        } catch (ExecutionException e) {
            throw new KafkaHelperException("Failed to send message to topic: '%s'".formatted(topic), e);
        }
    }
}
