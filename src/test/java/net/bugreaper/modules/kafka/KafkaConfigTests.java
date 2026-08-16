package net.bugreaper.modules.kafka;

import net.bugreaper.core.config.YamlUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;
import testcontainers.KafkaContainerSetup;

import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;


@SuppressWarnings("squid:S2699")
@Isolated
class KafkaConfigTests extends KafkaContainerSetup {


    private static final String CI = System.getenv("CI");
    private static final String PROPERTY = "bugreaperEnv";
    private String expectedHost;


    @BeforeEach
    void getCi() {
        YamlUtils.clearCache();
        if (Objects.equals(CI, "true")) {
            this.expectedHost = "docker:9096";
        } else {
            this.expectedHost = "localhost:9096";
        }
    }

    @Test
    void defaultConfigTest() {

        Kafka kafkaDef = getKafka();

        if (Objects.equals(CI, "true")) {
            System.setProperty(PROPERTY, "docker");
        } else {
            System.clearProperty(PROPERTY);
        }

        assertEquals(String.format("""
                        Kafka:
                            kafkaServer=%s
                            awaitMs=2000
                            awaitPollInterval=100
                            maxConsumedMessages=10
                            consumerTimeoutMs=5000
                            generate-unique-consumer=false
                            reverseMessages=true
                        """, expectedHost),
                kafkaDef.getConfigSummary());
    }

    @Test
    void configTest() {

        Kafka kafkaConf = Kafka.getInstance();

        if (Objects.equals(CI, "true")) {
            System.setProperty(PROPERTY, "docker");
        } else {
            System.clearProperty(PROPERTY);
        }

        assertEquals(String.format("""
                        Kafka:
                            kafkaServer=%s
                            awaitMs=1000
                            awaitPollInterval=200
                            maxConsumedMessages=5
                            consumerTimeoutMs=700
                            generate-unique-consumer=true
                            reverseMessages=false
                        """, expectedHost),
                kafkaConf.getConfigSummary());
    }

    @Test
    void configLogicTest() {

        Kafka kafkaConf = new Kafka().setAwaitMs(600); //if await is small - can be issue with re-count and re-grab

        String topic = "grabConf";
        String message = "test1";

        kafkaConf.createTopic(topic);
        kafkaConf.purgeTopic(topic);
        kafkaConf.sendToTopic(topic, message);

        kafkaConf.seeMessagesCountInTopicExactly(topic, 1);
        kafkaConf.seeMessagesCountInTopicExactly(topic, 1);

        kafkaConf.grabMessagesFromTopic(topic).seeListAnyEquals(message);
        kafkaConf.grabMessagesFromTopic(topic).seeListAnyEquals(message);

        kafkaConf.seeMessagesCountInTopicExactly(topic, 1);

        kafkaConf.sendToTopic(topic, message);
        kafkaConf.seeMessagesCountInTopicExactly(topic, 2);

    }

}
