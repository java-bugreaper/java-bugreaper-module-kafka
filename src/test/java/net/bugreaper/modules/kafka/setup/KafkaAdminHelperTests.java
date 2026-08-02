package net.bugreaper.modules.kafka.setup;

import net.bugreaper.modules.kafka.exceptions.KafkaHelperException;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.jupiter.api.Test;
import testcontainers.KafkaContainerSetup;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class KafkaAdminHelperTests extends KafkaContainerSetup {


    @Test
    void getTopicExistenceMethodInterruptedTest() throws Exception {
        AdminClient adminClient = mock(AdminClient.class);
        DescribeTopicsResult result = mock(DescribeTopicsResult.class);
        KafkaFuture<Map<String, TopicDescription>> future = mock(KafkaFuture.class);

        when(adminClient.describeTopics(anyCollection())).thenReturn(result);
        when(result.allTopicNames()).thenReturn(future);
        when(future.get()).thenThrow(new InterruptedException("Interrupted"));

        KafkaAdminHelper helper = new KafkaAdminHelper(adminClient);

        Throwable exception = assertThrows(KafkaHelperException.class,
                () -> helper.getTopicExistenceMethod("test-topic"));

        assertEquals("INTERRUPTED! Failed to check topic existence: 'test-topic'", exception.getMessage());

        assertInstanceOf(InterruptedException.class, exception.getCause());
        assertTrue(Thread.currentThread().isInterrupted());

        // clear interrupt flag for other tests
        Thread.interrupted();
    }

    @Test
    void getTopicExistenceMethodTimeoutTest() throws Exception {
        AdminClient adminClient = mock(AdminClient.class);
        DescribeTopicsResult describeTopicsResult = mock(DescribeTopicsResult.class);
        KafkaFuture<Map<String, TopicDescription>> future = mock(KafkaFuture.class);

        when(adminClient.describeTopics(anyCollection())).thenReturn(describeTopicsResult);
        when(describeTopicsResult.allTopicNames()).thenReturn(future);
        when(future.get()).thenThrow(new ExecutionException(new TimeoutException("Timeout")));

        KafkaAdminHelper helper = new KafkaAdminHelper(adminClient);

        Throwable exception = assertThrows(KafkaHelperException.class,
                () -> helper.getTopicExistenceMethod("test-topic"));

        assertEquals("Failed to check topic existence: 'test-topic'", exception.getMessage());
        assertInstanceOf(TimeoutException.class, exception.getCause());
    }

    @Test
    void getPartitionsCountMethodInterruptedTest() throws Exception {
        AdminClient adminClient = mock(AdminClient.class);
        DescribeTopicsResult result = mock(DescribeTopicsResult.class);
        KafkaFuture<TopicDescription> future = mock(KafkaFuture.class);

        when(adminClient.describeTopics(anyCollection()))
                .thenReturn(result);

        when(result.topicNameValues())
                .thenReturn(Map.of("test-topic", future));

        when(future.get())
                .thenThrow(new InterruptedException());

        KafkaAdminHelper helper = new KafkaAdminHelper(adminClient);

        KafkaHelperException exception = assertThrows(
                KafkaHelperException.class,
                () -> helper.getPartitionsCountMethod("test-topic")
        );

        assertEquals(
                "INTERRUPTED! Failed to find topic: 'test-topic'",
                exception.getMessage()
        );

        assertInstanceOf(InterruptedException.class, exception.getCause());
        assertTrue(Thread.currentThread().isInterrupted());

        // clear interrupt flag for other tests
        Thread.interrupted();
    }

    @Test
    void getPartitionsCountMethodTimeoutTest() throws Exception {
        AdminClient adminClient = mock(AdminClient.class);
        DescribeTopicsResult result = mock(DescribeTopicsResult.class);
        KafkaFuture<TopicDescription> future = mock(KafkaFuture.class);

        when(adminClient.describeTopics(anyCollection()))
                .thenReturn(result);

        when(result.topicNameValues())
                .thenReturn(Map.of("test-topic", future));

        when(future.get())
                .thenThrow(new ExecutionException(
                        new TimeoutException("timeout")
                ));

        KafkaAdminHelper helper = new KafkaAdminHelper(adminClient);

        KafkaHelperException exception = assertThrows(
                KafkaHelperException.class,
                () -> helper.getPartitionsCountMethod("test-topic")
        );

        assertEquals(
                "Failed to find topic: 'test-topic'",
                exception.getMessage()
        );

        assertInstanceOf(ExecutionException.class, exception.getCause());
    }


    @Test
    void createTopicMethodInterruptedTest() throws Exception {
        AdminClient adminClient = mock(AdminClient.class);
        CreateTopicsResult result = mock(CreateTopicsResult.class);
        KafkaFuture<Void> future = mock(KafkaFuture.class);

        when(adminClient.createTopics(anyCollection()))
                .thenReturn(result);

        when(result.values())
                .thenReturn(Map.of("test-topic", future));

        when(future.get())
                .thenThrow(new InterruptedException());

        KafkaAdminHelper helper = new KafkaAdminHelper(adminClient);

        KafkaHelperException exception = assertThrows(
                KafkaHelperException.class,
                () -> helper.createTopicMethod("test-topic", 1)
        );

        assertEquals(
                "INTERRUPTED! Failed to create topic: 'test-topic'",
                exception.getMessage()
        );

        assertInstanceOf(InterruptedException.class, exception.getCause());

        assertTrue(Thread.currentThread().isInterrupted());

        // Important: clear interrupt flag for following tests
        Thread.interrupted();
    }

    @Test
    void createTopicMethodTimeoutTest() throws Exception {
        AdminClient adminClient = mock(AdminClient.class);
        CreateTopicsResult result = mock(CreateTopicsResult.class);
        KafkaFuture<Void> future = mock(KafkaFuture.class);

        when(adminClient.createTopics(anyCollection()))
                .thenReturn(result);

        when(result.values())
                .thenReturn(Map.of("test-topic", future));

        when(future.get())
                .thenThrow(new ExecutionException(
                        new TimeoutException("timeout")
                ));

        KafkaAdminHelper helper = new KafkaAdminHelper(adminClient);

        KafkaHelperException exception = assertThrows(
                KafkaHelperException.class,
                () -> helper.createTopicMethod("test-topic", 1)
        );

        assertEquals(
                "Failed to create topic: 'test-topic'",
                exception.getMessage()
        );

        assertInstanceOf(ExecutionException.class, exception.getCause());
    }

    @Test
    void purgeTopicMethodInterruptedTest() throws Exception {
        AdminClient adminClient = mock(AdminClient.class);

        // mock getPartitionsCountMethod()
        DescribeTopicsResult describeResult = mock(DescribeTopicsResult.class);
        KafkaFuture<TopicDescription> topicFuture = mock(KafkaFuture.class);

        TopicDescription description = new TopicDescription(
                "test-topic",
                false,
                List.of(
                        new TopicPartitionInfo(
                                0,
                                Node.noNode(),
                                List.of(),
                                List.of()
                        )
                )
        );

        when(adminClient.describeTopics(anyCollection()))
                .thenReturn(describeResult);

        when(describeResult.topicNameValues())
                .thenReturn(Map.of("test-topic", topicFuture));

        when(topicFuture.get())
                .thenReturn(description);


        // mock deleteRecords interruption
        DeleteRecordsResult deleteResult = mock(DeleteRecordsResult.class);
        KafkaFuture<Void> deleteFuture = mock(KafkaFuture.class);

        when(adminClient.deleteRecords(anyMap()))
                .thenReturn(deleteResult);

        when(deleteResult.all())
                .thenReturn(deleteFuture);

        when(deleteFuture.get())
                .thenThrow(new InterruptedException());


        KafkaAdminHelper helper = new KafkaAdminHelper(adminClient);

        KafkaHelperException exception = assertThrows(
                KafkaHelperException.class,
                () -> helper.purgeTopicMethod("test-topic")
        );

        assertEquals(
                "INTERRUPTED! Failed to purge topic: 'test-topic'",
                exception.getMessage()
        );

        assertInstanceOf(InterruptedException.class, exception.getCause());

        assertTrue(Thread.currentThread().isInterrupted());

        // clear interrupt flag for next tests
        Thread.interrupted();
    }


    @Test
    void purgeTopicMethodExecutionFailTest() throws Exception {
        AdminClient adminClient = mock(AdminClient.class);

        // mock getPartitionsCountMethod() dependency
        DescribeTopicsResult describeResult = mock(DescribeTopicsResult.class);
        KafkaFuture<TopicDescription> topicFuture = mock(KafkaFuture.class);

        TopicDescription description = new TopicDescription(
                "test-topic",
                false,
                List.of(
                        new TopicPartitionInfo(
                                0,
                                Node.noNode(),
                                List.of(),
                                List.of()
                        )
                )
        );

        when(adminClient.describeTopics(anyCollection()))
                .thenReturn(describeResult);

        when(describeResult.topicNameValues())
                .thenReturn(Map.of("test-topic", topicFuture));

        when(topicFuture.get())
                .thenReturn(description);


        // mock deleteRecords failure
        DeleteRecordsResult deleteResult = mock(DeleteRecordsResult.class);
        KafkaFuture<Void> deleteFuture = mock(KafkaFuture.class);

        when(adminClient.deleteRecords(anyMap()))
                .thenReturn(deleteResult);

        when(deleteResult.all())
                .thenReturn(deleteFuture);

        when(deleteFuture.get())
                .thenThrow(new ExecutionException(
                        new TimeoutException("timeout")
                ));


        KafkaAdminHelper helper = new KafkaAdminHelper(adminClient);

        KafkaHelperException exception = assertThrows(
                KafkaHelperException.class,
                () -> helper.purgeTopicMethod("test-topic")
        );

        assertEquals(
                "Failed to purge topic: 'test-topic'",
                exception.getMessage()
        );

        assertInstanceOf(ExecutionException.class, exception.getCause());
    }

    @Test
    void deleteTopicMethodInterruptedTest() throws Exception {
        AdminClient adminClient = mock(AdminClient.class);
        DeleteTopicsResult result = mock(DeleteTopicsResult.class);
        KafkaFuture<Void> future = mock(KafkaFuture.class);

        when(adminClient.deleteTopics(anyCollection()))
                .thenReturn(result);

        when(result.all())
                .thenReturn(future);

        when(future.get())
                .thenThrow(new InterruptedException());

        KafkaAdminHelper helper = new KafkaAdminHelper(adminClient);

        KafkaHelperException exception = assertThrows(
                KafkaHelperException.class,
                () -> helper.deleteTopicMethod("test-topic")
        );

        assertEquals(
                "INTERRUPTED! Failed to delete topic: 'test-topic'",
                exception.getMessage()
        );

        assertInstanceOf(InterruptedException.class, exception.getCause());

        // verify interrupt flag was restored
        assertTrue(Thread.currentThread().isInterrupted());

        // cleanup for other tests
        Thread.interrupted();
    }
}
