package net.bugreaper.modules.kafka.setup;

import net.bugreaper.modules.kafka.exceptions.KafkaHelperException;
import net.bugreaper.modules.kafka.logger.Log;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.awaitility.core.ConditionTimeoutException;

import java.util.*;
import java.util.concurrent.ExecutionException;

import static net.bugreaper.core.mappers.StringMappers.formatMilliseconds;
import static net.bugreaper.core.utils.AwaitUtils.awaitCustom;
import static org.junit.jupiter.api.Assertions.assertTrue;


/**
 * Kafka admin helper that provides a common API for Kafka administration operations.
 */
@SuppressWarnings("squid:S5960")
public class KafkaAdminHelper {

    private final AdminClient adminClient;


    public KafkaAdminHelper(String bootStrapServer) {
        this.adminClient = AdminClient.create(createAdminClient(bootStrapServer));

        // Shutdown hook to close connect automatically when JVM exits
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                adminClient.close();
                Log.LOGGER.debug("Kafka admin client closed");
            } catch (Exception e) {
                Log.LOGGER.warn("Failed to close kafka admin client", e);
            }
        }
                , "kafka-admin-client-shutdown"
        ));
    }

    KafkaAdminHelper(AdminClient adminClient) {
        this.adminClient = adminClient;
    }

    private Properties createAdminClient(String bootStrapServer) {
        final Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);

        properties.put("default.api.timeout.ms", 10000);

        properties.put("socket.connection.setup.timeout.ms", 3000);
        properties.put("socket.connection.setup.timeout.max.ms", 3000);

        properties.put("request.timeout.ms", 10000);

        properties.put("reconnect.backoff.ms", 500);
        properties.put("reconnect.backoff.max.ms", 2000);

        return properties;
    }


    public void createTopicMethod(String topic, int partitions) {

        try {
            short replicationFactor = 1;
            NewTopic newTopic = new NewTopic(topic, partitions, replicationFactor);

            CreateTopicsResult result = adminClient.createTopics(
                    Collections.singleton(newTopic)
            );

            KafkaFuture<Void> future = result.values().get(topic);
            future.get();

        } catch (ExecutionException e) {
            if (e.getCause() instanceof TopicExistsException) {
                Log.LOGGER.warn(e.getMessage());
            } else {
                throw new KafkaHelperException("Failed to create topic: '%s'".formatted(topic), e);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaHelperException("INTERRUPTED! Failed to create topic: '%s'".formatted(topic), e);
        }

        Log.LOGGER.debug("Topic '{}' created", topic);
    }

    public void deleteTopicMethod(String topic) {
        try {
            adminClient.deleteTopics(Collections.singleton(topic)).all().get();
        } catch (ExecutionException e) {
            throw new KafkaHelperException("Failed to delete topic: '%s'".formatted(topic), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaHelperException("INTERRUPTED! Failed to delete topic: '%s'".formatted(topic), e);
        }

        Log.LOGGER.debug("Topic '{}' deleted", topic);
    }

    public void purgeTopicMethod(String topic) {

        // count all partitions in topic
        int partitionsCount = getPartitionsCountMethod(topic);
        Log.LOGGER.debug("Partitions in topic '{}': {}", topic, partitionsCount);

        Map<TopicPartition, RecordsToDelete> topicPartitionRecordToDelete = new HashMap<>();

        // add all partitions to list
        for (int i = 0; i < partitionsCount; i++) {
            topicPartitionRecordToDelete.put(
                    new TopicPartition(topic, i),
                    RecordsToDelete.beforeOffset(-1L)
            );
        }

        try {
            adminClient.deleteRecords(topicPartitionRecordToDelete).all().get();
        } catch (ExecutionException e) {
            throw new KafkaHelperException("Failed to purge topic: '%s'".formatted(topic), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaHelperException("INTERRUPTED! Failed to purge topic: '%s'".formatted(topic), e);
        }

        Log.LOGGER.debug("Topic '{}' purged", topic);
    }


    public int getPartitionsCountMethod(String topic) {

        DescribeTopicsResult result = adminClient.describeTopics(Collections.singletonList(topic));

        Map<String, KafkaFuture<TopicDescription>> values = result.topicNameValues();
        KafkaFuture<TopicDescription> topicDescription = values.get(topic);

        try {
            return topicDescription.get().partitions().size();

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaHelperException("INTERRUPTED! Failed to find topic: '%s'".formatted(topic), e);
        } catch (ExecutionException e) {
            Log.LOGGER.error("Failed to find topic: {}", topic);
            if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                throw new KafkaHelperException("Topic '%s' does not exist".formatted(topic), e);
            } else {
                throw new KafkaHelperException("Failed to find topic: '%s'".formatted(topic), e);
            }

        }
    }

    public boolean getTopicExistenceMethod(String topic) {

        try {
            adminClient.describeTopics(Collections.singleton(topic))
                    .allTopicNames()
                    .get();
            return true;
        } catch (ExecutionException e) {
            if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                return false;
            }
            throw new KafkaHelperException("Failed to check topic existence: '%s'".formatted(topic), e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaHelperException("INTERRUPTED! Failed to check topic existence: '%s'".formatted(topic), e);
        }
    }

    public void seeTopicExistsMethod(String topic, int awaitMs) {
        try {
            awaitCustom(awaitMs).untilAsserted(() ->
                    assertTrue(getTopicExistenceMethod(topic)));
        } catch (ConditionTimeoutException e) {
            throw new AssertionError(
                    "Topic '%s' does not exist within %s"
                            .formatted(topic, formatMilliseconds(awaitMs)));
        }
    }

}
