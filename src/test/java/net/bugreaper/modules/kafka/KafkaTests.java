package net.bugreaper.modules.kafka;

import ch.qos.logback.classic.Level;
import com.fasterxml.jackson.databind.JsonNode;
import net.bugreaper.core.utils.AllureAssert;
import net.bugreaper.core.utils.AllureResultLoader;
import net.bugreaper.core.utils.LogWatcher;
import org.hamcrest.MatcherAssert;
import org.hamcrest.core.StringContains;
import org.junit.jupiter.api.*;
import testcontainers.KafkaSetup;

import java.util.concurrent.CompletableFuture;

import static java.lang.Thread.sleep;
import static org.hamcrest.Matchers.startsWithIgnoringCase;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;


@SuppressWarnings("squid:S2699")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class KafkaTests {


    private final KafkaSetup setup = KafkaSetup.getInstance();
    private final Kafka kafka = setup.getKafka();


    @Test
    void createTopicMultipleTimesTest() {
        String topic = "new_topic";

        kafka.createTopic(topic);
        kafka.getAllTopicsNames()
                .seeListAnyEquals(topic);

        //second time no error
        kafka.createTopic(topic);
        kafka.getAllTopicsNames()
                .seeListAnyEquals(topic);
    }

    @Test
    void deleteTopicTest() {
        String topic = "delete_topic";

        kafka.createTopic(topic);

        kafka.getAllTopicsNames()
                .seeListAnyEquals(topic);

        kafka.deleteTopic(topic);

        var topics = kafka.getAllTopicsNames();
        Throwable exception = assertThrows(AssertionError.class, () ->
                topics.seeListAnyEquals(topic));

        MatcherAssert.assertThat(
                exception.getMessage(),
                StringContains.containsString("There is no elements in the list equals to string:\n" +
                        "delete_topic"));

    }

    @Test
    void pushMessagesWithKeysAndCheckStringEqualTest() {
        String topic = "equalStringTopic";
        String message = "Test_massage";

        kafka.sendToTopicWithKey(topic, "key-1", "dummy");
        kafka.sendToTopicWithKey(topic, "key-1", message);

        kafka.seeMessagesHaveEqualText(topic, message);
    }

    @Test
    void pushMessageMultipleTimesAndCheckContainsTest() {
        String topic = "severalStringTests";

        kafka.sendToTopic(topic, "message_1");
        kafka.seeMessagesContainText(topic, "mess");
        kafka.sendToTopic(topic, "message_1");
        kafka.seeMessagesContainText(topic, "message_1");

        kafka.purgeTopic(topic);

        kafka.sendToTopic(topic, "test_22");

        kafka.grabMessagesFromTopic(topic)
                .seeListAnyMatcher(startsWithIgnoringCase("TEST"));
    }

    @Test
    void checkCountFeatureTest() {
        String topic = "countTopic";

        kafka.sendToTopic(topic, "debug");
        kafka.purgeTopic(topic);

        kafka.sendToTopic(topic, "message_1");
        kafka.seeTopicIsNotEmpty(topic);
        kafka.seeCountMessagesInTopicExactly(topic, 1);

        kafka.sendToTopic(topic, "message_2");
        kafka.seeCountMessagesInTopicExactly(topic, 2);
        assertEquals(2,
                kafka.getTopicMessageCount(topic),
                "Expected count");

        kafka.seeMessagesContainText(topic, "message_1");

        kafka.purgeTopic(topic);

        kafka.seeTopicIsEmpty(topic);

        kafka.sendToTopic(topic, "test_22");
        kafka.seeCountMessagesInTopicExactly(topic, 1);
        kafka.seeMessagesContainText(topic, "test_22");

    }

    @Test
    void jsonContainsTest() {
        String topic = "json_1";

        kafka.sendToTopic(topic, "message_1");
        kafka.sendToTopic(topic, """
                {"id": 15, "text": "some15"}""");
        kafka.sendToTopic(topic, """
                {"id": 16, "text": "some16"}""");

        kafka.seeMessagesContainJson(topic, """
                {"id": 16}""");
    }

    @Test
    void grabAndJsonContainsExtendedTest() {
        String topic = "json_1";

        kafka.sendToTopic(topic, "message_1");
        kafka.sendToTopic(topic, """
                {"id": 15, "text": "some15"}""");


        kafka.grabMessagesFromTopic(topic)
                .seeListAnyContainsExtendedJson("""
                        {
                        "id:>": 10,
                        "text:regex": "some.*"
                        }""");
    }

    @Test
    @Order(1)
    void jsonEqualsTest() {
        String topic = "json_2";

        kafka.sendToTopic(topic, """
                {"id": 20, "text": "some20"}""");
        kafka.sendToTopic(topic, "message_1");
        kafka.sendToTopic(topic, """
                {"id": 22, "text": "some22"}""");

        kafka.sendToTopic(topic, "message_2");

        kafka.seeMessagesHaveEqualJson(topic, """
                {"id": 22, "text": "some22"}""");

        String topic2 = "json_22";
        kafka.sendToTopic(topic2, "message_1");
        kafka.seeMessagesHaveEqualText(topic2, "message_1");
    }

    @Test
    @Order(2)
    void allureForListCheck() {
        JsonNode result = AllureResultLoader.loadByTestName("jsonEqualsTest");

        AllureAssert.assertThat(result)
                .hasStep("(Kafka) Send message to topic: json_2")
                .hasAttachment("message:", """
                        {"id": 20, "text": "some20"}""")

                .hasStep("(Kafka)[ASSERT] Message in topic: <json_2> EQUALS expected JSON")
                .hasSubStep("(Kafka) Grab messages from topic: json_2")
                //list reversed!
                .hasAttachment("Messages(4) list:", """
                        [
                        message_2
                        
                        -----------
                        
                        {"id": 22, "text": "some22"}

                        -----------

                        message_1

                        -----------

                        {"id": 20, "text": "some20"}
                        ]""")

                .hasStep("(Kafka)[ASSERT] Message in topic: <json_2> EQUALS expected JSON")
                .hasSubStep("(Assert) List should have JSON EQUAL to:")
                .hasAttachment("expected json", """
                        {"id": 22, "text": "some22"}""")

                .hasStep("(Kafka)[ASSERT] Message in topic: <json_22> EQUALS expected text")
                .hasSubStep("(Kafka) Grab messages from topic: json_22")

                .hasStep("(Kafka)[ASSERT] Message in topic: <json_22> EQUALS expected text")
                .hasSubStep("(Assert) List should have STRING EQUAL to: <message_1>");
    }

    @Test
    void purgeTopicTest() {

        String topic = "topicForPurge";
        String message = "test1";

        kafka.sendToTopic(topic, message);
        kafka.seeCountMessagesInTopicExactly(topic, 1);
        kafka.purgeTopic(topic);
        kafka.seeCountMessagesInTopicExactly(topic, 0);
    }

    @Test
    void purgeCommitedTopicTest() {

        String topic = "topicForPurge2";
        String message = "test1";

        kafka.sendToTopic(topic, message);
        kafka.seeCountMessagesInTopicExactly(topic, 1);
        kafka.grabMessagesFromTopic(topic);
        kafka.seeCountMessagesInTopicExactly(topic, 1);

        kafka.purgeTopic(topic);
        kafka.seeCountMessagesInTopicExactly(topic, 0);

        kafka.sendToTopic(topic, message);
        kafka.purgeTopic(topic);
        kafka.seeCountMessagesInTopicExactly(topic, 0);
    }

    @Test
    void grabNotCleanedTopicTest() {

        String topic = "grab";
        String message = "test1";

        kafka.createTopic(topic);
        kafka.purgeTopic(topic);
        kafka.sendToTopic(topic, message);

        kafka.seeCountMessagesInTopicExactly(topic, 1);

        kafka.grabMessagesFromTopic(topic).seeListAnyEquals(message);
        kafka.seeCountMessagesInTopicExactly(topic, 1);
        kafka.grabMessagesFromTopic(topic).seeListAnyEquals(message);

        kafka.seeCountMessagesInTopicExactly(topic, 1);
    }


    @Test
    void topicWithMultiplePartitionsTest() {
        String topic = "multiplePartitions";

        kafka.createTopic(topic, 3);

        kafka.sendToTopic(topic, "message_1");
        kafka.sendToTopic(topic, "message_2");
        kafka.sendToTopic(topic, "message_3");

        kafka.seeCountMessagesInTopicExactly(topic, 3);

        kafka.seeMessagesContainText(topic, "message_2");

        assertEquals(
                "[[INFO] Messages consumed from topic <multiplePartitions>: 3]",
                logWatcher.getLoggedEvents(Level.INFO).toString());

        kafka.purgeTopic(topic);

        kafka.seeCountMessagesInTopicExactly(topic, 0);

        kafka.sendToTopic(topic, "test_22");
        kafka.seeCountMessagesInTopicExactly(topic, 1);
    }

    private LogWatcher logWatcher;
    @BeforeEach
    void setup() {
        logWatcher = new LogWatcher("bugreaper-module-kafka", Level.DEBUG);
    }

    @AfterEach
    void teardown() {
        logWatcher.detach();
    }

    @Test
    void maxGrabbedMessagesTest() {
        String topic = "maxMessages";

        Kafka kafkaCustom = setup.getKafka().setMaxConsumeMessages(2);

        kafkaCustom.createTopic(topic);

        kafkaCustom.sendToTopic(topic, "test_1");
        kafkaCustom.sendToTopic(topic, "test_2");
        kafkaCustom.sendToTopic(topic, "test_3");

        kafkaCustom.seeCountMessagesInTopicExactly(topic, 3);

        kafkaCustom.grabMessagesFromTopic(topic)
                .seeListHasExactlyCount(2)
                .seeListAnyContains("test_2")
                .seeListAnyContains("test_3");

        assertEquals(
                """
                [[WARN] Count of messages in topic <maxMessages> is <3>: more than maxMessages(2) in config
                only last messages will be consumed to list (can be changed by .setMaxConsumeMessages(int) or config 'max-consumed-messages')]""",
                logWatcher.getLoggedEvents(Level.WARN).toString());

        assertEquals(
                "[[INFO] Messages consumed from topic <maxMessages>: 2]",
                logWatcher.getLoggedEvents(Level.INFO).toString());

        MatcherAssert.assertThat(
                logWatcher.getLoggedEvents(Level.DEBUG).toString(),
                StringContains.containsString("""
                        List of messages: [
                        test_3
                        
                        -----------
                        
                        test_2
                        ]"""));
    }

    @Test
    void parallelNotEmptyTest() {

        String topic = "paralleltopic1";

        kafka.createTopic(topic);
        kafka.purgeTopic(topic);

        CompletableFuture<Void> step1 = CompletableFuture.runAsync(() -> kafka.seeTopicIsNotEmpty(topic));
        CompletableFuture<Void> step2 = CompletableFuture.runAsync(() -> pushWithSleep(topic));

        CompletableFuture.allOf(step1, step2).join();
    }

    @Test
    void parallelCountTest() {

        String topic = "paralleltopic2";

        kafka.createTopic(topic);
        kafka.purgeTopic(topic);

        CompletableFuture<Void> step1 = CompletableFuture.runAsync(() -> kafka.seeCountMessagesInTopicExactly(topic, 1));
        CompletableFuture<Void> step2 = CompletableFuture.runAsync(() -> pushWithSleep(topic));

        CompletableFuture.allOf(step1, step2).join();
    }

    @Test
    void parallelGrabTest() {

        String topic = "paralleltopic3";

        kafka.createTopic(topic);
        kafka.purgeTopic(topic);

        CompletableFuture<Void> step1 = CompletableFuture.runAsync(() -> kafka.grabMessagesFromTopic(topic).seeListAnyContains("some"));
        CompletableFuture<Void> step2 = CompletableFuture.runAsync(() -> pushWithSleep(topic));

        CompletableFuture.allOf(step1, step2).join();
    }

    @SuppressWarnings("squid:S2925")
    private void pushWithSleep(String topic) {
        try {
            sleep(500);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        kafka.sendToTopic(topic, "some message");
    }

}
