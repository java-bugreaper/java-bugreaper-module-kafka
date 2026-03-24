package net.bugreaper.modules.kafka;

import net.bugreaper.core.config.YamlUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import testcontainers.KafkaSetup;

import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;


@SuppressWarnings("squid:S2699")
class KafkaConfigTests {


    private final KafkaSetup setup = KafkaSetup.getInstance();

    private static final String CI = System.getenv("CI");
    private static final String PROPERTY = "bugreaperEnv";
    private String expectedHost;


    @BeforeEach
    void getCi(){
        YamlUtils.clearCache();
        if(Objects.equals(CI, "true")){
            this.expectedHost = "docker:9096";
        }else {
            this.expectedHost = "localhost:9096";
        }
    }

    @Test
    void defaultConfigTest() {

        Kafka kafkaDef = setup.getKafka();

        if(Objects.equals(CI, "true")){
            System.setProperty(PROPERTY, "docker");
        }else {
            System.clearProperty(PROPERTY);
        }

        assertEquals(String.format("""
                        Kafka:
                            kafkaServer=%s
                            awaitMs=2000
                            maxConsumedMessages=10
                            consumerTimeoutMs=5000
                            generate-unique-consumer=false
                        """, expectedHost),
                kafkaDef.getConfigSummary());
    }

    @Test
    void configTest() {

        Kafka kafkaConf = Kafka.getInstance();

        if(Objects.equals(CI, "true")){
            System.setProperty(PROPERTY, "docker");
        }else {
            System.clearProperty(PROPERTY);
        }

        assertEquals(String.format("""
                        Kafka:
                            kafkaServer=%s
                            awaitMs=300
                            maxConsumedMessages=5
                            consumerTimeoutMs=700
                            generate-unique-consumer=true
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

        kafkaConf.seeCountMessagesInTopicExactly(topic, 1);
        kafkaConf.seeCountMessagesInTopicExactly(topic, 1);

        kafkaConf.grabMessagesFromTopic(topic).seeListAnyEquals(message);
        kafkaConf.grabMessagesFromTopic(topic).seeListAnyEquals(message);

        kafkaConf.seeCountMessagesInTopicExactly(topic, 1);

        kafkaConf.sendToTopic(topic, message);
        kafkaConf.seeCountMessagesInTopicExactly(topic, 2);

    }

}
