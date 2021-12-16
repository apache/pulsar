/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.client.impl;

import com.google.common.collect.Sets;
import io.netty.channel.EventLoopGroup;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.Cleanup;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Messages;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.util.netty.EventLoopUtil;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static org.apache.pulsar.client.impl.ClientTestFixtures.createDelayedCompletedFuture;
import static org.apache.pulsar.client.impl.ClientTestFixtures.createExceptionFuture;
import static org.apache.pulsar.client.impl.ClientTestFixtures.createPulsarClientMockWithMockedClientCnx;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

/**
 * Unit Tests of {@link MultiTopicsConsumerImpl}.
 */
public class MultiTopicsConsumerImplTest {

    private ExecutorProvider executorProvider;
    private ExecutorService internalExecutor;

    @BeforeMethod(alwaysRun = true)
    public void setUp() {
        executorProvider = new ExecutorProvider(1, "MultiTopicsConsumerImplTest");
        internalExecutor = Executors.newSingleThreadScheduledExecutor();
    }

    @AfterMethod(alwaysRun = true)
    public void cleanUp() {
        if (executorProvider != null) {
            executorProvider.shutdownNow();
            executorProvider = null;
        }
        if (internalExecutor != null) {
            internalExecutor.shutdownNow();
            internalExecutor = null;
        }
    }

    @Test
    public void testGetStats() throws Exception {
        String topicName = "test-stats";
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setServiceUrl("pulsar://localhost:6650");
        conf.setStatsIntervalSeconds(100);

        ThreadFactory threadFactory = new DefaultThreadFactory("client-test-stats", Thread.currentThread().isDaemon());

        @Cleanup("shutdown")
        EventLoopGroup eventLoopGroup = EventLoopUtil.newEventLoopGroup(conf.getNumIoThreads(), false, threadFactory);

        @Cleanup("shutdownNow")
        ExecutorProvider executorProvider = new ExecutorProvider(1, "client-test-stats");

        @Cleanup
        PulsarClientImpl clientImpl = new PulsarClientImpl(conf, eventLoopGroup);

        ConsumerConfigurationData consumerConfData = new ConsumerConfigurationData();
        consumerConfData.setTopicNames(Sets.newHashSet(topicName));

        assertEquals(Long.parseLong("100"), clientImpl.getConfiguration().getStatsIntervalSeconds());

        MultiTopicsConsumerImpl impl = new MultiTopicsConsumerImpl(
            clientImpl, consumerConfData,
            executorProvider, null, null, null, true);

        impl.getStats();
    }

    // Test uses a mocked PulsarClientImpl which will complete the getPartitionedTopicMetadata() internal async call
    // after a delay longer than the interval between the two subscribeAsync() calls in the test method body.
    //
    // Code under tests is using CompletableFutures. Theses may hang indefinitely if code is broken.
    // That's why a test timeout is defined.
    @Test(timeOut = 5000)
    public void testParallelSubscribeAsync() throws Exception {
        String topicName = "parallel-subscribe-async-topic";
        MultiTopicsConsumerImpl<byte[]> impl = createMultiTopicsConsumer();

        CompletableFuture<Void> firstInvocation = impl.subscribeAsync(topicName, true);
        Thread.sleep(5); // less than completionDelayMillis
        CompletableFuture<Void> secondInvocation = impl.subscribeAsync(topicName, true);

        firstInvocation.get(); // does not throw
        Throwable t = expectThrows(ExecutionException.class, secondInvocation::get);
        Throwable cause = t.getCause();
        assertEquals(cause.getClass(), PulsarClientException.class);
        assertTrue(cause.getMessage().endsWith("Topic is already being subscribed for in other thread."));
    }

    private MultiTopicsConsumerImpl<byte[]> createMultiTopicsConsumer() {
        ConsumerConfigurationData<byte[]> consumerConfData = new ConsumerConfigurationData<>();
        consumerConfData.setSubscriptionName("subscriptionName");
        return createMultiTopicsConsumer(consumerConfData);
    }

    private MultiTopicsConsumerImpl<byte[]> createMultiTopicsConsumer(
            ConsumerConfigurationData<byte[]> consumerConfData) {
        int completionDelayMillis = 100;
        Schema<byte[]> schema = Schema.BYTES;
        PulsarClientImpl clientMock = createPulsarClientMockWithMockedClientCnx(executorProvider, internalExecutor);
        when(clientMock.getPartitionedTopicMetadata(any())).thenAnswer(invocation -> createDelayedCompletedFuture(
                new PartitionedTopicMetadata(), completionDelayMillis));
        when(clientMock.<byte[]>preProcessSchemaBeforeSubscribe(any(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(schema));
        MultiTopicsConsumerImpl<byte[]> impl = new MultiTopicsConsumerImpl<byte[]>(
                clientMock, consumerConfData, executorProvider,
                new CompletableFuture<>(), schema, null, true);
        return impl;
    }

    @Test
    public void testReceiveAsyncCanBeCancelled() {
        // given
        MultiTopicsConsumerImpl<byte[]> consumer = createMultiTopicsConsumer();
        CompletableFuture<Message<byte[]>> future = consumer.receiveAsync();
        Awaitility.await().untilAsserted(() -> assertTrue(consumer.hasNextPendingReceive()));
        // when
        future.cancel(true);
        // then
        assertTrue(consumer.pendingReceives.isEmpty());
    }

    @Test
    public void testBatchReceiveAsyncCanBeCancelled() {
        // given
        MultiTopicsConsumerImpl<byte[]> consumer = createMultiTopicsConsumer();
        CompletableFuture<Messages<byte[]>> future = consumer.batchReceiveAsync();
        assertTrue(consumer.hasPendingBatchReceive());
        // when
        future.cancel(true);
        // then
        assertFalse(consumer.hasPendingBatchReceive());
    }

    @Test
    public void testConsumerCleanupOnSubscribeFailure() {
        ConsumerConfigurationData<byte[]> consumerConfData = new ConsumerConfigurationData<>();
        consumerConfData.setSubscriptionName("subscriptionName");
        consumerConfData.setTopicNames(new HashSet<>(Arrays.asList("a", "b", "c")));
        int completionDelayMillis = 10;
        Schema<byte[]> schema = Schema.BYTES;
        PulsarClientImpl clientMock = createPulsarClientMockWithMockedClientCnx(executorProvider, internalExecutor);
        when(clientMock.getPartitionedTopicMetadata(any())).thenAnswer(invocation -> createExceptionFuture(
                new PulsarClientException.InvalidConfigurationException("a mock exception"), completionDelayMillis));
        CompletableFuture<Consumer<byte[]>> completeFuture = new CompletableFuture<>();
        MultiTopicsConsumerImpl<byte[]> impl = new MultiTopicsConsumerImpl<byte[]>(clientMock, consumerConfData,
                executorProvider, completeFuture, schema, null, true);
        // assert that we don't start in closed, then we move to closed and get an exception
        // indicating that closeAsync was called
        assertEquals(impl.getState(), HandlerState.State.Uninitialized);
        try {
            completeFuture.get(2, TimeUnit.SECONDS);
        } catch (Throwable ignore) {
            // just ignore the exception
        }
        assertTrue(completeFuture.isCompletedExceptionally());
        assertEquals(impl.getConsumers().size(), 0);
        assertEquals(impl.getState(), HandlerState.State.Closed);
        verify(clientMock, times(1)).cleanupConsumer(any());
    }

    @Test
    public void testDontCheckForPartitionsUpdatesOnNonPartitionedTopics() throws Exception {
        ExecutorProvider executorProvider = mock(ExecutorProvider.class);
        ConsumerConfigurationData<byte[]> consumerConfData = new ConsumerConfigurationData<>();
        consumerConfData.setSubscriptionName("subscriptionName");
        consumerConfData.setTopicNames(new HashSet<>(Arrays.asList("a", "b", "c")));
        consumerConfData.setAutoUpdatePartitionsIntervalSeconds(1);
        consumerConfData.setAutoUpdatePartitions(true);

        @Cleanup("stop")
        Timer timer = new HashedWheelTimer();

        PulsarClientImpl clientMock = createPulsarClientMockWithMockedClientCnx(executorProvider, internalExecutor);
        when(clientMock.timer()).thenReturn(timer);
        when(clientMock.preProcessSchemaBeforeSubscribe(any(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(null));

        // Simulate non partitioned topics
        PartitionedTopicMetadata metadata = new PartitionedTopicMetadata(0);
        when(clientMock.getPartitionedTopicMetadata(any())).thenReturn(CompletableFuture.completedFuture(metadata));
        CompletableFuture<Consumer<byte[]>> completeFuture = new CompletableFuture<>();

        MultiTopicsConsumerImpl<byte[]> impl = new MultiTopicsConsumerImpl<>(
                clientMock, consumerConfData, executorProvider,
                completeFuture, Schema.BYTES, null, true);
        impl.setState(HandlerState.State.Ready);
        Thread.sleep(5000);

        // getPartitionedTopicMetadata should have been called only the first time, for each of the 3 topics,
        // but not anymore since the topics are not partitioned.
        verify(clientMock, times(3)).getPartitionedTopicMetadata(any());
    }

}
