package net.bugreaper.modules.kafka;

import io.qameta.allure.Allure;
import io.qameta.allure.Step;
import net.bugreaper.core.assertable.AssertableStringList;
import net.bugreaper.core.config.YamlUtils;
import net.bugreaper.modules.kafka.interfaces.KafkaConfig;
import net.bugreaper.modules.kafka.interfaces.KafkaInt;
import net.bugreaper.modules.kafka.interfaces.KafkaAsserts;
import net.bugreaper.modules.kafka.logger.Log;
import net.bugreaper.modules.kafka.setup.KafkaConsumerHelper;
import net.bugreaper.modules.kafka.setup.KafkaProducerHelper;

/**
 * Kafka helper that provides a common API for operating with Kafka.
 *
 * <p>It is recommended to use a single instance:
 * {@code Kafka kafka = Kafka.getInstance();}
 * </p>
 *
 * <p>Uses a static consumer membership to reduce consumer rebalancing delays after reconnects.</p>
 *
 * <p><b>Important:</b> Consumed messages remain available after the test execution.
 * It is recommended to purge messages between tests or use unique test data.</p>
 *
 * <p>Consumed messages are limited by default to {@link #maxConsumedMessages}
 * (only the latest messages are grabbed). The limit can be changed using
 * {@link #setMaxConsumeMessages(int)} or configuration:
 * {@code modules.kafka.max-consumed-messages}.</p>
 *
 * <p>Consumed messages are returned in reverse order (newest messages first) by default
 * to improve performance for fast tests.</p>
 *
 * <p>Assertions use the default await timeout
 * {@link #awaitMs} and wait for the first message.
 * The timeout can be changed using {@link #setAwaitMs(int)} or configuration:
 * {@code modules.kafka.await}.</p>
 *
 * <p>For testing, it is recommended to use one partition per topic,
 * but multiple partitions are supported.</p>
 *
 * <p>By default, the consumer uses a static group ID.
 * A unique consumer can be enabled using {@link #setUniqueConsumer(boolean)}
 * or configuration:
 * {@code modules.kafka.generate-unique-consumer}.</p>
 *
 * @author Oleksii Betin "ambu550"
 * @since 1.0.0
 */
public class Kafka extends KafkaConsumerHelper implements KafkaInt, KafkaAsserts, KafkaConfig {

    private final KafkaProducerHelper messageProducer;
    private static Kafka instance;

    /**
     * Returns the instance of {@link Kafka} with config builder {@link #Kafka()}.
     * <p>
     * This implementation is thread-safe using method-level synchronization.
     *
     * @return the shared instance of {@link Kafka}
     * @see #Kafka() config setup
     *
     * @throws IllegalArgumentException if the configuration contains invalid values
     */
    public static synchronized Kafka getInstance() {
        if (instance == null) {
            instance = new Kafka();
        }

        return instance;
    }

    /**
     * Creates a Kafka helper with the specified bootstrap server.
     *
     * @param bootStrapServer kafka server (example: {@code "my-kafka:9092"})
     */
    public Kafka(String bootStrapServer) {
        super(bootStrapServer);
        this.messageProducer = new KafkaProducerHelper(bootStrapServer);
    }

    /**
     * Constructs a MongoDb client using YAML configuration.
     *
     * <p>Loads configuration values from a YAML file.</p>
     *
     * <p><b>Default file:</b> {@code bugreaper.yml}</p>
     * <p><b>Custom file:</b> using {@code -DbugreaperEnv=test} loads {@code bugreaper-test.yml}</p>
     *
     * <pre>
     * modules:
     *   kafka:
     *     url: localhost:9096
     *     await: 300 # (optional)
     *     max-consumed-messages: 5 # (optional)
     *     max-consumer-timeout: 700 # (optional)
     *     generate-unique-consumer: true # (optional)
     *     reverse-messages: true # (optional)
     * </pre>
     *
     * <p>Missing required keys will result in configuration errors.
     * Missing optional keys will fall back to predefined defaults.</p>
     *
     * @throws IllegalArgumentException if the configuration contains invalid values
     */
    public Kafka() {
        super(YamlUtils.getStringValueByPath("modules.kafka.url"));
        this.messageProducer = new KafkaProducerHelper(bootStrapServer);

        //optional config fields
        Object awaitVal = YamlUtils.getValueByPath("modules.kafka.await", true);
        if (awaitVal instanceof Number number) {
            setAwaitMs(number.intValue());
        }
        Object awaitPollIntervalVal = YamlUtils.getValueByPath("modules.kafka.await-poll-interval", true);
        if (awaitPollIntervalVal instanceof Number number) {
            this.awaitPollInterval = number.intValue();
        }
        Object maxConsumedMessagesVal = YamlUtils.getValueByPath("modules.kafka.max-consumed-messages", true);
        if (maxConsumedMessagesVal instanceof Number number) {
            setMaxConsumeMessages(number.intValue());
        }
        Object consumerTimeoutMsVal = YamlUtils.getValueByPath("modules.kafka.max-consumer-timeout", true);
        if (consumerTimeoutMsVal instanceof Number number) {
            setConsumerTimeoutMs(number.intValue());
        }
        Object uniqueConsumer = YamlUtils.getValueByPath("modules.kafka.generate-unique-consumer", true);
        if (uniqueConsumer instanceof Boolean bool) {
            setUniqueConsumer(bool);
        }
        Object reverseVal = YamlUtils.getValueByPath("modules.kafka.reverse-messages", true);
        if (reverseVal instanceof Boolean reverse) {
            setReverseMessages(reverse);
        }
    }


    //setters

    @Override
    public Kafka setAwaitMs(int awaitMs) {
        if (awaitMs < 200) {
            throw new IllegalArgumentException("awaitMs too small (can`t bee less 200ms)");
        }
        this.awaitMs = awaitMs;
        return this;
    }

    @Override
    public Kafka setMaxConsumeMessages(int maxMessages) {
        if (maxMessages < 1) {
            throw new IllegalArgumentException("maxMessages too small (can`t bee less 1)");
        }
        this.maxConsumedMessages = maxMessages;
        return this;
    }

    @Override
    public Kafka setConsumerTimeoutMs(int consumerTimeoutMs) {
        if (consumerTimeoutMs < 1) {
            throw new IllegalArgumentException("consumerTimeoutMs too small (can`t bee less 1ms)");
        }
        this.consumerTimeoutMs = consumerTimeoutMs;
        return this;
    }

    @Override
    public Kafka setUniqueConsumer(boolean unique) {
        this.uniqueConsumerGroup = unique;
        return this;
    }

    @Override
    public Kafka setReverseMessages(boolean reverseMessages) {
        this.reverseMessages = reverseMessages;
        return this;
    }

    @Override
    public String getConfigSummary() {
        String info = String.format("""
                        %s:
                            kafkaServer=%s
                            awaitMs=%d
                            awaitPollInterval=%d
                            maxConsumedMessages=%d
                            consumerTimeoutMs=%d
                            generate-unique-consumer=%b
                            reverseMessages=%b%n""",
                this.getClass().getSimpleName(), bootStrapServer,
                awaitMs, awaitPollInterval, maxConsumedMessages, consumerTimeoutMs,
                uniqueConsumerGroup, reverseMessages);

        Log.LOGGER.info(info);
        return info;
    }

    //interaction

    @Override
    @Step("(Kafka) Create topic: {topic}")
    public void createTopic(String topic) {
        adminClient.createTopicMethod(topic, 1);
    }

    @Override
    @Step("(Kafka) Create topic: '{topic}' with <{partitionsCount}> partitions")
    public void createTopic(String topic, int partitionsCount) {
        adminClient.createTopicMethod(topic, partitionsCount);
    }

    @Override
    @Step("(Kafka) Delete topic: '{topic}'")
    public void deleteTopic(String topic) {
        adminClient.deleteTopicMethod(topic);
    }

    @Override
    @Step("(Kafka) Purge topic: '{topic}'")
    public void purgeTopic(String topic) {
        adminClient.purgeTopicMethod(topic);
    }

    @Override
    @Step("(Kafka) Send message to topic: '{topic}'")
    public void sendToTopic(String topic, String message) {
        Allure.addAttachment("message:", "application/json", message);
        messageProducer.send(topic, message);
    }

    @Override
    @Step("(Kafka) Send message to topic: '{topic}' with key '{key}'")
    public void sendToTopicWithKey(String topic, String key, String message) {
        Allure.addAttachment("message:", "application/json", message);
        messageProducer.sendWithKey(topic, key, message);
    }

    //get data

    @Override
    @Step("(Kafka) Grab messages from topic: '{topic}'")
    public AssertableStringList grabMessagesFromTopic(String topic) {
        return grabMessagesFromTopicMethod(topic, null);
    }

    @Override
    @Step("(Kafka) Grab messages from topic: '{topic}' with key: '{key}'")
    public AssertableStringList grabMessagesFromTopic(String topic, String key) {
        return grabMessagesFromTopicMethod(topic, key);
    }

    @Override
    @Step("(Kafka) Get all topics names")
    public AssertableStringList getAllTopicsNames() {
        return getAllTopicsNamesMethod();
    }

    @Override
    public int getMessagesCountInTopic(String topic) {
        return getTopicMessageCountMethod(topic);
    }

    @Override
    public int getPartitionsCount(String topic) {
        return adminClient.getPartitionsCountMethod(topic);
    }

    //asserts

    @Override
    @Step("(Kafka)[ASSERT] Topic: '{topic}' has a message EQUALS expected text")
    public void seeMessagesHaveEqualText(String topic, String expectedText) {
        grabMessagesFromTopic(topic)
                .seeListAnyEquals(expectedText);
    }

    @Override
    @Step("(Kafka)[ASSERT] Topic: '{topic}' has a message CONTAINS expected text")
    public void seeMessagesContainText(String topic, String expectedPart) {
        grabMessagesFromTopic(topic)
                .seeListAnyContains(expectedPart);
    }


    @Override
    @Step("(Kafka)[ASSERT] Topic: '{topic}' has a message EQUALS expected JSON")
    public void seeMessagesHaveEqualJson(String topic, String expectedJson) {
        grabMessagesFromTopic(topic)
                .seeListAnyEqualsJson(expectedJson);
    }

    @Override
    @Step("(Kafka)[ASSERT] Topic: '{topic}' has a message CONTAINS expected JSON")
    public void seeMessagesContainJson(String topic, String expectedJsonPart) {
        grabMessagesFromTopic(topic)
                .seeListAnyContainsJson(expectedJsonPart);
    }

    @Override
    @Step("(Kafka)[ASSERT] Topic: '{topic}' contains EXACTLY <{expectedCount}> messages")
    public void seeMessagesCountInTopicExactly(String topic, int expectedCount) {
        assertCountInTopicMethod(topic, expectedCount, awaitMs);
    }

    @Override
    @Step("(Kafka)[ASSERT] Topic: '{topic}' is not empty")
    public void seeTopicIsNotEmpty(String topic) {
        seeTopicIsNotEmptyMethod(topic, awaitMs);
    }

    @Override
    @Step("(Kafka)[ASSERT] Topic: '{topic}' is empty")
    public void seeTopicIsEmpty(String topic) {
        seeTopicIsEmptyMethod(topic, awaitMs);
    }


    @Override
    @Step("(Kafka)[ASSERT] Topic: '{topic}' exists")
    public void seeTopicExists(String topic) {
        adminClient.seeTopicExistsMethod(topic, awaitMs);
    }
}
