package net.bugreaper.modules.kafka;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.parallel.*;
import testcontainers.KafkaContainerSetup;

import java.util.concurrent.CompletableFuture;

import static java.lang.Thread.sleep;


@SuppressWarnings("squid:S2699")
@Execution(ExecutionMode.CONCURRENT) // All methods inside this class run in parallel
class KafkaParallelTests extends KafkaContainerSetup {


    private final Kafka kafkaConfig = Kafka.getInstance();

    private final Kafka kafka = getKafka();

    @Test
    void parallel1Test() {

        String topic = "paralleltopic1";

        kafkaConfig.createTopic(topic);
        kafkaConfig.purgeTopic(topic);

        CompletableFuture<Void> step1 = CompletableFuture.runAsync(() -> kafkaConfig.seeCountMessagesInTopicExactly(topic, 1));
        CompletableFuture<Void> step2 = CompletableFuture.runAsync(() -> pushWithSleep(kafkaConfig, topic, 500));
        CompletableFuture<Void> step3 = CompletableFuture.runAsync(() -> kafkaConfig.grabMessagesFromTopic(topic));

        CompletableFuture.allOf(step1, step2, step3).join();
    }

    @Test
    void parallel2Test() {

        String topic = "paralleltopic2";

        kafkaConfig.createTopic(topic);
        kafkaConfig.purgeTopic(topic);

        CompletableFuture<Void> step1 = CompletableFuture.runAsync(() -> kafkaConfig.grabMessagesFromTopic(topic).seeListAnyContains("some"));
        CompletableFuture<Void> step2 = CompletableFuture.runAsync(() -> pushWithSleep(kafkaConfig, topic, 500));
        CompletableFuture<Void> step3 = CompletableFuture.runAsync(() -> kafkaConfig.seeTopicIsNotEmpty(topic));

        CompletableFuture.allOf(step1, step2, step3).join();

        CompletableFuture.allOf(step1, step2).join();
    }

    @Test
    void parallelGrabDefaultTest() {

        String topic = "paralleltopic_obj";

        kafka.createTopic(topic);
        kafka.purgeTopic(topic);

        CompletableFuture<Void> step1 = CompletableFuture.runAsync(() -> kafka.grabMessagesFromTopic(topic).seeListAnyContains("some"));
        CompletableFuture<Void> step2 = CompletableFuture.runAsync(() -> pushWithSleep(kafkaConfig, topic, 500));
        CompletableFuture<Void> step3 = CompletableFuture.runAsync(() -> kafka.seeTopicIsNotEmpty(topic));

        CompletableFuture.allOf(step1, step2, step3).join();

        CompletableFuture.allOf(step1, step2).join();
    }

    @SuppressWarnings("squid:S2925")
    private void pushWithSleep(Kafka object, String topic, int sleep) {
        try {
            sleep(sleep);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        object.sendToTopic(topic, "some message");
    }

}
