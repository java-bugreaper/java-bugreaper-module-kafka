package net.bugreaper.modules.kafka.setup;

import net.bugreaper.core.assertable.AssertableStringList;
import net.bugreaper.core.mappers.StringMappers;
import net.bugreaper.modules.kafka.exceptions.KafkaHelperException;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.awaitility.core.ConditionTimeoutException;

import java.text.MessageFormat;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import static net.bugreaper.core.allurereporter.AllureReporter.attachFromList;
import static net.bugreaper.core.mappers.StringMappers.formatMilliseconds;
import static net.bugreaper.core.utils.AwaitUtils.awaitCustom;
import static net.bugreaper.modules.kafka.logger.Log.LOGGER;
import static org.junit.jupiter.api.Assertions.*;


/**
 * Kafka consumer helper responsible for consuming messages from Kafka topics.
 */
@SuppressWarnings("squid:S5960")
public class KafkaConsumerHelper {

    // KafkaConsumer is not thread-safe, so each test thread must own its own instance.
    private final ThreadLocal<ConsumerHolder> consumerHolder;

    // ThreadLocal values from other threads are not visible to the shutdown hook thread.
    // Keeping a concurrent registry lets us close all consumers created by this helper safely.
    private final Set<KafkaConsumer<String, String>> createdConsumers = ConcurrentHashMap.newKeySet();

    protected final String bootStrapServer;
    protected final KafkaAdminHelper adminClient;

    /**
     * default consumer group
     */
    private static final String DEFAULT_GROUP = "bugreaper-consumer-group";

    /**
     * default ms await in tests
     */
    protected volatile int awaitMs = 2000;

    /**
     * default max ms for consumer
     */
    protected volatile int consumerTimeoutMs = 5000;

    /**
     * default max messages that will be consumed by grab
     */
    protected volatile int maxConsumedMessages = 10;

    /**
     * switch for unique groupId for consumer
     */
    protected volatile boolean uniqueConsumerGroup = false;

    /**
     * default consumed messages will be reversed in list (newer messages will be checked first)
     */
    protected volatile boolean reverseMessages = true;

    protected KafkaConsumerHelper(String bootStrapServer) {

        this.bootStrapServer = bootStrapServer;
        this.adminClient = new KafkaAdminHelper(bootStrapServer);
        this.consumerHolder = ThreadLocal.withInitial(this::createConsumerHolder);

        Runtime.getRuntime().addShutdownHook(createShutdownHook());
    }


    private ConsumerHolder createConsumerHolder() {
        boolean currentUniqueConsumerGroup = uniqueConsumerGroup;
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(createConsumerProperties(bootStrapServer, currentUniqueConsumerGroup));
        createdConsumers.add(kafkaConsumer);
        return new ConsumerHolder(kafkaConsumer, currentUniqueConsumerGroup);
    }

    protected KafkaConsumer<String, String> consumer() {
        ConsumerHolder holder = consumerHolder.get();

        // setUniqueConsumer can be called after a thread has already created a consumer.
        // Rebuilding here keeps the consumer group config consistent with the latest helper state.
        if (holder.uniqueConsumerGroup != uniqueConsumerGroup) {
            closeConsumer(holder.consumer);
            consumerHolder.remove();
            holder = consumerHolder.get();
            LOGGER.debug("Kafka consumer rebuild");
        }

        return holder.consumer;
    }

    private record ConsumerHolder(KafkaConsumer<String, String> consumer, boolean uniqueConsumerGroup) {
    }

    private void closeConsumer(KafkaConsumer<String, String> kafkaConsumer) {
        try {
            kafkaConsumer.close();
            createdConsumers.remove(kafkaConsumer);
        } catch (Exception e) {
            LOGGER.warn("Failed to close kafka consumer", e);
        }
    }

    private void closeCreatedConsumers() {
        createdConsumers.forEach(this::closeConsumer);
        consumerHolder.remove();
    }


    // Shutdown hook to close connect automatically when JVM exits
    Thread createShutdownHook() {
        return new Thread(() -> {
            try {
                closeCreatedConsumers();
                LOGGER.debug("Kafka consumer closed");
            } catch (Exception e) {
                LOGGER.warn("Failed to close kafka consumer", e);
            }
        }, "kafka-consumer-shutdown");
    }

    protected AssertableStringList grabMessagesFromTopicMethod(String topic, String key) {

        //Check is topic exists no timeout
        adminClient.getPartitionsCountMethod(topic);

        //create a consumer with static membership to remove latency on rebalancing lag
        List<TopicPartition> partitions = consumer().partitionsFor(topic).stream().map(p -> new TopicPartition(topic, p.partition()))
                .toList();

        consumer().assign(partitions);

        ConsumerRecords<String, String> records = consumer().poll(Duration.ofMillis(awaitMs));

        // add messages to list
        Deque<String> resultList = new ArrayDeque<>();

        long startTime = System.currentTimeMillis();

        for (ConsumerRecord<String, String> message : records) {

            if (System.currentTimeMillis() - startTime > consumerTimeoutMs) {
                throw new KafkaHelperException(MessageFormat.format("Consuming interrupted after reaching the time limit: {0}. Increase the timeout using setConsumerTimeoutMs() or the 'max-consumer-timeout' configuration if needed.", formatMilliseconds(consumerTimeoutMs)));
            }

            if (key != null && !Objects.equals(key, message.key())) {
                continue;
            }

            LOGGER.debug("message: {}", message.value());
            resultList.add(message.value());

        }

        unsubscribe();

        if (resultList.isEmpty()) {
            throw new ConditionTimeoutException("No messages were received from topic '%s' within %s".formatted(topic, formatMilliseconds(awaitMs)));
        }

        if (resultList.size() > maxConsumedMessages) {
            LOGGER.warn("""
                            Topic '{}' contains <{}> messages, which exceeds the configured maxMessages({}).
                            Only the latest messages will be consumed. Change the limit using .setMaxConsumeMessages(int) or the 'max-consumed-messages' configuration.""",
                    topic, resultList.size(), maxConsumedMessages);
        }

        // cut oldest messages out of resultList and reverse list (newest first)
        List<String> actualList = new ArrayList<>(maxConsumedMessages);
        Iterator<String> it = resultList.descendingIterator();

        int count = 0;
        while (it.hasNext() && count < maxConsumedMessages) {
            actualList.add(it.next());
            count++;
        }

        // reverse back if false
        if (!reverseMessages) {
            Collections.reverse(actualList);
        }

        LOGGER.info("Messages consumed from topic <{}>: {}", topic, actualList.size());
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("List of messages: {}", StringMappers.listToString(actualList));
        }

        attachFromList(String.format("Messages(%d) list:", actualList.size()), actualList);

        return new AssertableStringList(actualList);
    }

    protected AssertableStringList getAllTopicsNamesMethod() {
        Map<String, List<PartitionInfo>> topicsMap = consumer().listTopics();
        ArrayList<String> topics = new ArrayList<>(topicsMap.keySet());

        attachFromList("Topics list:", topics);

        return new AssertableStringList(topics);

    }

    protected int getTopicMessageCountMethod(String topic) {

        //Check is topic exists no timeout
        adminClient.getPartitionsCountMethod(topic);

        List<TopicPartition> partitions = consumer().partitionsFor(topic).stream().map(p -> new TopicPartition(topic, p.partition()))
                .toList();

        consumer().assign(partitions);

        consumer().seekToEnd(Collections.emptySet());
        Map<TopicPartition, Long> endPartitions = partitions.stream().collect(Collectors.toMap(Function.identity(), consumer()::position));
        long all = partitions.stream().mapToLong(endPartitions::get).sum();

        consumer().seekToBeginning(Collections.emptySet());
        Map<TopicPartition, Long> startPartitions = partitions.stream().collect(Collectors.toMap(Function.identity(), consumer()::position));
        long start = partitions.stream().mapToLong(startPartitions::get).sum();
        long count = all - start;
        consumer().unsubscribe();
        LOGGER.debug("In Topic: <{}> -> (all {} message/s - {} deleted) = {}", topic, all, start, count);
        return (int) count;
    }

    private void unsubscribe() {
        consumer().unsubscribe();
    }

    private Properties createConsumerProperties(String bootStrapServer, boolean uniqueConsumerGroup) {
        Properties props = new Properties();


        if (uniqueConsumerGroup) {

            props.put(ConsumerConfig.GROUP_ID_CONFIG, DEFAULT_GROUP + "-" + UUID.randomUUID());
        } else {
            props.put(ConsumerConfig.GROUP_ID_CONFIG, DEFAULT_GROUP);
        }
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return props;
    }

    protected void assertCountInTopicMethod(String topic, int expectedCount, int awaitMs) {
        try {
            awaitCustom(awaitMs).untilAsserted(() ->
                    assertEquals(
                            expectedCount,
                            getTopicMessageCountMethod(topic)));
        } catch (ConditionTimeoutException e) {
            throw new AssertionError(
                    "Expected EXACTLY <%d> messages in topic '%s', but got <%d> within %s"
                            .formatted(expectedCount, topic, getTopicMessageCountMethod(topic), formatMilliseconds(awaitMs)));
        }
    }

    protected void seeTopicIsNotEmptyMethod(String topic, int awaitMs) {
        try {
            awaitCustom(awaitMs).untilAsserted(() ->
                    assertNotEquals(
                            0,
                            getTopicMessageCountMethod(topic)));
        } catch (ConditionTimeoutException e) {
            //for throw error
            getTopicMessageCountMethod(topic);

            throw new AssertionError(
                    "Expected topic '%s' to be empty, but got no messages within %s"
                            .formatted(topic, formatMilliseconds(awaitMs)));
        }
    }


    protected void seeTopicIsEmptyMethod(String topic, int awaitMs) {
        try {
            awaitCustom(awaitMs).untilAsserted(() ->
                    assertEquals(
                            0,
                            getTopicMessageCountMethod(topic)));
        } catch (ConditionTimeoutException e) {
            throw new AssertionError(
                    "Expected topic '%s' to be empty, but got <%d> messages within %s"
                            .formatted(topic, getTopicMessageCountMethod(topic), formatMilliseconds(awaitMs)));
        }
    }

}
