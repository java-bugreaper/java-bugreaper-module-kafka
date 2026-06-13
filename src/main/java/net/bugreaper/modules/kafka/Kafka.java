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
 * This class consists methods that operate with Kafka
 *
 * <p>For one instance run recommended: {@code Kafka kafka = Kafka.getInstance();}</p>
 *
 * <p><b>Interaction:</b>
 * {@link #createTopic(String topic)}
 * {@link #createTopic(String topic, int partitionsCount)}
 * {@link #deleteTopic(String topic)}
 * {@link #purgeTopic(String topic)}
 * {@link #sendToTopic(String topic, String message)}
 * {@link #sendToTopicWithKey(String topic, String key, String message)}
 *
 * <p><b>Get data:</b>
 * {@link #grabMessagesFromTopic(String topic)}
 * {@link #getTopicMessageCount(String topic)}
 * {@link #getAllTopicsNames()}
 *
 * <p><b>Asserts:</b>
 * {@link #seeTopicIsEmpty(String topic)}
 * {@link #seeTopicIsNotEmpty(String topic)}
 * {@link #seeCountMessagesInTopicExactly(String topic, int expectedCount)}
 * {@link #seeMessagesHaveEqualJson(String topic, String expectedJson)}
 * {@link #seeMessagesContainJson(String topic, String expectedJsonPart)}
 * {@link #seeMessagesHaveEqualText(String topic, String expectedText)}
 * {@link #seeMessagesContainText(String topic, String expectedPart)}
 *
 * <p> Consumer with static membership for remove re-balancing lag after reconnect consumer
 * <p><b> Asserts and grab left messages after test!!! - recommended to use purge between tests(preferable) or test unique data</b>
 * <p> Grab messages limited, by default {@link #maxConsumedMessages} (grab last messages) (can be changed by {@link #setMaxConsumeMessages(int)} or config {@code modules.kafka.max-consumed-messages})
 * <p> Consumed messages will be reversed (newest messages first - for fast tests)
 * <p> Await for first message, by default {@link #awaitMs} (can be changed by {@link #setAwaitMs(int)} or config {@code modules.kafka.await})
 * <p> For testing recommended to use ONE partition per topic (but supports multiple partitions)
 *
 * <p> Consumer has static groupId by default (not recommended parallel tests on one topic), can be changed by {@link #setUniqueConsumer(boolean)} or config {@code modules.kafka.generate-unique-consumer}
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
     * @return the singleton instance of {@link Kafka}
     */
    public static synchronized Kafka getInstance() {
        if (instance == null) {
            instance = new Kafka();
        }

        return instance;
    }

    /**
     * @param bootStrapServer kafka server (example: {@code "my-kafka:9092"})
     */
    public Kafka(String bootStrapServer) {
        super(bootStrapServer);
        this.messageProducer = new KafkaProducerHelper(bootStrapServer);
    }

    /**
     * Constructs a kafka client configuration.
     *
     * <p>Loads configuration values from a YAML file.</p>
     *
     * <p><b>Default file:</b> {@code bugreaper.yml}</p>
     * <p><b>Custom file:</b> using {@code -DbugreaperEnv=test} loads {@code bugreaper-test.yml}</p>
     *
     * <p><b>Required configuration keys:</b></p>
     * <ul>
     *     <li>{@code modules.kafka.url}</li>
     * </ul>
     *
     * <p><b>Optional configuration keys:</b></p>
     * <ul>
     *     <li>{@code modules.kafka.await}</li>
     *     <li>{@code modules.kafka.max-consumed-messages}</li>
     *     <li>{@code modules.kafka.max-consumer-timeout}</li>
     *     <li>{@code modules.kafka.generate-unique-consumer}</li>
     * </ul>
     *
     * <p>Missing required keys will result in configuration errors.
     * Missing optional keys will fall back to predefined defaults.</p>
     */
    public Kafka() {
        super(YamlUtils.getStringValueByPath("modules.kafka.url"));
        this.messageProducer = new KafkaProducerHelper(bootStrapServer);

        //optional config fields
        Object awaitVal = YamlUtils.getValueByPath("modules.kafka.await", true);
        if (awaitVal instanceof Number number) {
            setAwaitMs(number.intValue());
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
    public String getConfigSummary() {
        String info = String.format("""
                        %s:
                            kafkaServer=%s
                            awaitMs=%d
                            maxConsumedMessages=%d
                            consumerTimeoutMs=%d
                            generate-unique-consumer=%b%n""",
                this.getClass().getSimpleName(), bootStrapServer,
                awaitMs, maxConsumedMessages, consumerTimeoutMs, uniqueConsumerGroup);

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
    @Step("(Kafka) Create topic: {topic} with partitions: {partitionsCount}")
    public void createTopic(String topic, int partitionsCount) {
        adminClient.createTopicMethod(topic, partitionsCount);
    }

    @Override
    @Step("(Kafka) Delete topic: {topic}")
    public void deleteTopic(String topic) {
        adminClient.deleteTopicMethod(topic);
    }

    @Override
    @Step("(Kafka) Purge topic: {topic}")
    public void purgeTopic(String topic) {
        adminClient.purgeTopicMethod(topic);
    }

    @Override
    @Step("(Kafka) Send message to topic: {topic}")
    public void sendToTopic(String topic, String message) {
        Allure.addAttachment("message:", "application/json", message);
        messageProducer.send(topic, message);
    }

    @Override
    @Step("(Kafka) Send message to topic: {topic} with key: {key}")
    public void sendToTopicWithKey(String topic, String key, String message) {
        Allure.addAttachment("message:", "application/json", message);
        messageProducer.sendWithKey(topic, key, message);
    }

    //get data

    @Override
    @Step("(Kafka) Grab messages from topic: {topic}")
    public AssertableStringList grabMessagesFromTopic(String topic) {
        return grabMessagesFromTopicMethod(topic);
    }

    @Override
    @Step("(Kafka) Get all topics names")
    public AssertableStringList getAllTopicsNames() {
        return getAllTopicsNamesMethod();
    }

    @Override
    public int getTopicMessageCount(String topic) {
        return getTopicMessageCountMethod(topic);
    }

    @Override
    public int getPartitionsCount(String topic) {
        return adminClient.getPartitionsCountMethod(topic);
    }

    //asserts

    @Override
    @Step("(Kafka)[ASSERT] Message in topic: <{topic}> EQUALS expected text")
    public void seeMessagesHaveEqualText(String topic, String expectedText) {
        grabMessagesFromTopic(topic)
                .seeListAnyEquals(expectedText);
    }

    @Override
    @Step("(Kafka)[ASSERT] Message in topic: <{topic}> CONTAINS expected text")
    public void seeMessagesContainText(String topic, String expectedPart) {
        grabMessagesFromTopic(topic)
                .seeListAnyContains(expectedPart);
    }


    @Override
    @Step("(Kafka)[ASSERT] Message in topic: <{topic}> EQUALS expected JSON")
    public void seeMessagesHaveEqualJson(String topic, String expectedJson) {
        grabMessagesFromTopic(topic)
                .seeListAnyEqualsJson(expectedJson);
    }

    @Override
    @Step("(Kafka)[ASSERT] Message in topic: <{topic}> CONTAINS expected JSON")
    public void seeMessagesContainJson(String topic, String expectedJsonPart) {
        grabMessagesFromTopic(topic)
                .seeListAnyContainsJson(expectedJsonPart);
    }

    @Override
    @Step("(Kafka)[ASSERT] Topic: <{topic}> contains {expectedCount} messages")
    public void seeCountMessagesInTopicExactly(String topic, int expectedCount) {
        assertCountInTopicMethod(topic, expectedCount, awaitMs);
    }

    @Override
    @Step("(Kafka)[ASSERT] Topic: <{topic}> is not empty")
    public void seeTopicIsNotEmpty(String topic) {
        seeTopicIsNotEmptyMethod(topic, awaitMs);
    }

    @Override
    @Step("(Kafka)[ASSERT] Topic: <{topic}> is empty")
    public void seeTopicIsEmpty(String topic) {
        seeTopicIsEmptyMethod(topic, awaitMs);
    }

}
