/*
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

import static org.apache.pulsar.client.impl.ClientTestFixtures.createDelayedCompletedFuture;
import static org.apache.pulsar.client.impl.ClientTestFixtures.createExceptionFuture;
import static org.apache.pulsar.client.impl.ClientTestFixtures.createPulsarClientMockWithMockedClientCnx;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import com.google.common.collect.Sets;
import io.netty.channel.EventLoopGroup;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.DeadLetterPolicy;
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
import org.mockito.ArgumentCaptor;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

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

    @SuppressWarnings({"deprecation", "unchecked"})
    @Test
    public void testGetStats() throws Exception {
        String topicName = "test-stats";
        ClientConfigurationData conf = new ClientConfigurationData();
        // ip and port is arbitrary since test will attempt to make a connection
        // and the connection is not needed for making the test to pass. This test should be improved.
        conf.setServiceUrl("pulsar://127.0.0.99:23456");
        conf.setStatsIntervalSeconds(100);

        ThreadFactory threadFactory = new DefaultThreadFactory("client-test-stats", Thread.currentThread().isDaemon());

        @Cleanup("shutdown")
        EventLoopGroup eventLoopGroup = EventLoopUtil.newEventLoopGroup(conf.getNumIoThreads(), false, threadFactory);

        @Cleanup("shutdownNow")
        ExecutorProvider executorProvider = new ExecutorProvider(1, "client-test-stats");

        @Cleanup
        PulsarClientImpl clientImpl = new PulsarClientImpl(conf, eventLoopGroup);

        @SuppressWarnings("rawtypes")
        ConsumerConfigurationData consumerConfData = new ConsumerConfigurationData();
        consumerConfData.setTopicNames(Sets.newHashSet(topicName));

        assertEquals(Long.parseLong("100"), clientImpl.getConfiguration().getStatsIntervalSeconds());

        @SuppressWarnings("rawtypes")
        MultiTopicsConsumerImpl impl = new MultiTopicsConsumerImpl(
            clientImpl, consumerConfData,
            executorProvider, null, Schema.BYTES, null, true);

        impl.getStats();

        clientImpl.close();
        executorProvider.shutdownNow();
        eventLoopGroup.shutdownGracefully().get();
    }

    // Test uses a mocked PulsarClientImpl which will complete the getPartitionedTopicMetadata() internal async call
    // after a delay longer than the interval between the two subscribeAsync() calls in the test method body.
    //
    // Code under tests is using CompletableFutures. Theses may hang indefinitely if code is broken.
    // That's why a test timeout is defined.
    @Test(timeOut = 10000)
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
        when(clientMock.getPartitionedTopicMetadata(any(), anyBoolean(), anyBoolean()))
                .thenAnswer(invocation -> createDelayedCompletedFuture(
                new PartitionedTopicMetadata(), completionDelayMillis));
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
        Awaitility.await().untilAsserted(() -> assertTrue(consumer.hasPendingBatchReceive()));
        // when
        future.cancel(true);
        // then
        assertFalse(consumer.hasPendingBatchReceive());
    }

    @Test(expectedExceptions = {IllegalArgumentException.class})
    public void testValidTopicNames() {
        ConsumerConfigurationData<byte[]> consumerConfData = new ConsumerConfigurationData<>();
        consumerConfData.setSubscriptionName("subscriptionName");
        consumerConfData.setTopicNames(Sets.newHashSet("persistent://public/invalid-topic"));
        createMultiTopicsConsumer(consumerConfData);
    }

    @Test
    public void testConsumerCleanupOnSubscribeFailure() {
        ConsumerConfigurationData<byte[]> consumerConfData = new ConsumerConfigurationData<>();
        consumerConfData.setSubscriptionName("subscriptionName");
        consumerConfData.setTopicNames(new HashSet<>(Arrays.asList("a", "b", "c")));
        int completionDelayMillis = 10;
        Schema<byte[]> schema = Schema.BYTES;
        PulsarClientImpl clientMock = createPulsarClientMockWithMockedClientCnx(executorProvider, internalExecutor);
        when(clientMock.getPartitionedTopicMetadata(any(), anyBoolean(), anyBoolean()))
                .thenAnswer(invocation -> createExceptionFuture(
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
    @SuppressWarnings("unchecked")
    public void testCloseDiscardsQueuedMessages() throws Exception {
        MultiTopicsConsumerImpl<byte[]> consumer = createMultiTopicsConsumer();
        TopicMessageImpl<byte[]> message = mock(TopicMessageImpl.class);
        when(message.size()).thenReturn(100);
        consumer.enqueueMessageAndCheckBatchReceive(message);
        assertEquals(consumer.getIncomingMessageSize(), 100);

        consumer.closeAsync().get(10, TimeUnit.SECONDS);

        verify(message).release();
        assertEquals(consumer.getTotalIncomingMessages(), 0);
        assertEquals(consumer.getIncomingMessageSize(), 0);
    }

    @Test
    public void testUnsubscribeAsyncInvalidTopicNameReturnsFailedFuture() {
        MultiTopicsConsumerImpl<byte[]> consumer = createMultiTopicsConsumer();
        CompletableFuture<Void> future = consumer.unsubscribeAsync("persistent://public/invalid-topic");

        assertTrue(future.isCompletedExceptionally());
        CompletionException ex = expectThrows(CompletionException.class, future::join);
        assertTrue(ex.getCause() instanceof IllegalArgumentException);
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

        // Simulate non partitioned topics
        PartitionedTopicMetadata metadata = new PartitionedTopicMetadata(0);
        when(clientMock.getPartitionedTopicMetadata(any(), anyBoolean(), anyBoolean()))
                .thenReturn(CompletableFuture.completedFuture(metadata));
        CompletableFuture<Consumer<byte[]>> completeFuture = new CompletableFuture<>();

        MultiTopicsConsumerImpl<byte[]> impl = new MultiTopicsConsumerImpl<>(
                clientMock, consumerConfData, executorProvider,
                completeFuture, Schema.BYTES, null, true);
        impl.setState(HandlerState.State.Ready);
        Thread.sleep(5000);

        // getPartitionedTopicMetadata should have been called only the first time, for each of the 3 topics,
        // but not anymore since the topics are not partitioned.
        verify(clientMock, times(3)).getPartitionedTopicMetadata(any(), anyBoolean(), anyBoolean());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testOnTopicsExtendedRemovedTopicCleansUnackedMessages() {
        String topicName = "persistent://public/default/deleted-topic";
        String topicPartition0 = topicName + "-partition-0";
        String topicPartition1 = topicName + "-partition-1";
        String otherTopicPartition = "persistent://public/default/other-topic-partition-0";

        ConsumerConfigurationData<byte[]> consumerConfData = new ConsumerConfigurationData<>();
        consumerConfData.setSubscriptionName("subscriptionName");
        consumerConfData.setAutoUpdatePartitions(true);
        consumerConfData.setAutoUpdatePartitionsIntervalSeconds(60);
        consumerConfData.setAckTimeoutMillis(1000);

        MultiTopicsConsumerImpl<byte[]> impl = createMultiTopicsConsumer(consumerConfData);

        impl.partitionedTopics.put(topicName, 2);
        impl.allTopicPartitionsNumber.set(2);

        ConsumerImpl<byte[]> partitionConsumer0 = (ConsumerImpl<byte[]>) mock(ConsumerImpl.class);
        ConsumerImpl<byte[]> partitionConsumer1 = (ConsumerImpl<byte[]>) mock(ConsumerImpl.class);
        when(partitionConsumer0.getTopic()).thenReturn(topicPartition0);
        when(partitionConsumer1.getTopic()).thenReturn(topicPartition1);
        when(partitionConsumer0.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));
        when(partitionConsumer1.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));
        impl.consumers.put(topicPartition0, partitionConsumer0);
        impl.consumers.put(topicPartition1, partitionConsumer1);

        TopicMessageIdImpl removedTopicMessageId = new TopicMessageIdImpl(topicPartition0, new MessageIdImpl(1, 1, 0));
        TopicMessageIdImpl otherTopicMessageId =
                new TopicMessageIdImpl(otherTopicPartition, new MessageIdImpl(2, 2, 0));
        impl.getUnAckedMessageTracker().add(removedTopicMessageId);
        impl.getUnAckedMessageTracker().add(otherTopicMessageId);
        assertEquals(impl.getUnAckedMessageTracker().size(), 2);

        when(impl.client.getPartitionsForTopic(topicName, false)).thenReturn(CompletableFuture.completedFuture(
                Collections.emptyList()));

        PartitionsChangedListener listener = impl.topicsPartitionChangedListener;
        listener.onTopicsExtended(Collections.singleton(topicName)).join();

        assertTrue(impl.getConsumers().isEmpty());
        assertEquals(impl.partitionedTopics.get(topicName), Integer.valueOf(0));
        assertEquals(impl.allTopicPartitionsNumber.get(), 0);
        assertEquals(impl.getUnAckedMessageTracker().size(), 1);
        assertTrue(impl.getUnAckedMessageTracker().remove(otherTopicMessageId));
        verify(partitionConsumer0).closeAsync();
        verify(partitionConsumer1).closeAsync();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testBatchAcknowledgeRemovesOnlyAckedMessageIdsFromTracker() {
        String topic0 = "persistent://public/default/topic-a-partition-0";
        String topic1 = "persistent://public/default/topic-b-partition-0";

        ConsumerConfigurationData<byte[]> conf = new ConsumerConfigurationData<>();
        conf.setSubscriptionName("subscriptionName");
        conf.setAckTimeoutMillis(1000);
        MultiTopicsConsumerImpl<byte[]> impl = createMultiTopicsConsumer(conf);
        impl.setState(HandlerState.State.Ready);

        ConsumerImpl<byte[]> consumer0 = mock(ConsumerImpl.class);
        ConsumerImpl<byte[]> consumer1 = mock(ConsumerImpl.class);
        CompletableFuture<Void> pendingAck = new CompletableFuture<>();
        when(consumer0.getTopic()).thenReturn(topic0);
        when(consumer1.getTopic()).thenReturn(topic1);
        when(consumer0.doAcknowledgeWithTxn(anyList(), any(), any(), any())).thenReturn(pendingAck);
        when(consumer1.doAcknowledgeWithTxn(anyList(), any(), any(), any())).thenReturn(new CompletableFuture<>());

        impl.consumers.put(topic0, consumer0);
        impl.consumers.put(topic1, consumer1);

        TopicMessageIdImpl messageId0 = new TopicMessageIdImpl(topic0, new MessageIdImpl(1, 1, 0));
        TopicMessageIdImpl messageId1 = new TopicMessageIdImpl(topic1, new MessageIdImpl(2, 2, 0));
        impl.getUnAckedMessageTracker().add(messageId0);
        impl.getUnAckedMessageTracker().add(messageId1);

        impl.acknowledgeAsync(Arrays.asList(messageId0, messageId1));
        pendingAck.complete(null);

        assertEquals(impl.getUnAckedMessageTracker().size(), 1);
    }

    // -------------------------------------------------------------------------
    // Tests for shared retry / dead-letter producer provider wiring.
    // -------------------------------------------------------------------------

    private static DeadLetterPolicy sampleDeadLetterPolicy() {
        return DeadLetterPolicy.builder()
                .maxRedeliverCount(3)
                .retryLetterTopic("persistent://public/default/my-topic-RETRY")
                .deadLetterTopic("persistent://public/default/my-topic-DLQ")
                .build();
    }

    @SuppressWarnings("unchecked")
    private ConsumerImpl<byte[]> mockPartitionConsumer(String partitionName) {
        ConsumerImpl<byte[]> c = (ConsumerImpl<byte[]>) mock(ConsumerImpl.class);
        when(c.getTopic()).thenReturn(partitionName);
        return c;
    }

    @Test
    public void testSharedRetryProviderNotInstalledWhenNoDeadLetterPolicy() {
        ConsumerConfigurationData<byte[]> conf = new ConsumerConfigurationData<>();
        conf.setSubscriptionName("subscriptionName");
        // No DeadLetterPolicy set (default: null).
        MultiTopicsConsumerImpl<byte[]> impl = createMultiTopicsConsumer(conf);

        ConsumerImpl<byte[]> partition0 =
                mockPartitionConsumer("persistent://public/default/my-topic-partition-0");

        impl.installSharedRetryOrDlqProducerProviderIfNeeded(partition0, conf);

        assertTrue(impl.getSharedRetryOrDlqProducerProvidersForTesting().isEmpty(),
                "No shared provider should be created when deadLetterPolicy is null");
        verify(partition0, never()).setRetryOrDlqProducerProvider(any());
    }

    @Test
    public void testSharedRetryProviderNotInstalledWhenOptedOut() {
        ConsumerConfigurationData<byte[]> conf = new ConsumerConfigurationData<>();
        conf.setSubscriptionName("subscriptionName");
        conf.setDeadLetterPolicy(sampleDeadLetterPolicy());
        // Opt out of the sharing optimisation.
        conf.setShareRetryAndDeadLetterProducers(false);

        MultiTopicsConsumerImpl<byte[]> impl = createMultiTopicsConsumer(conf);

        ConsumerImpl<byte[]> partition0 =
                mockPartitionConsumer("persistent://public/default/my-topic-partition-0");

        impl.installSharedRetryOrDlqProducerProviderIfNeeded(partition0, conf);

        assertTrue(impl.getSharedRetryOrDlqProducerProvidersForTesting().isEmpty(),
                "Opting out of sharing should prevent any provider from being installed");
        verify(partition0, never()).setRetryOrDlqProducerProvider(any());
    }

    @Test
    public void testSharedRetryProviderInstalledForPartitionedTopicAndReusedAcrossPartitions() {
        ConsumerConfigurationData<byte[]> conf = new ConsumerConfigurationData<>();
        conf.setSubscriptionName("subscriptionName");
        conf.setDeadLetterPolicy(sampleDeadLetterPolicy());
        // Default: shareRetryAndDeadLetterProducers = true

        MultiTopicsConsumerImpl<byte[]> impl = createMultiTopicsConsumer(conf);

        ConsumerImpl<byte[]> partition0 =
                mockPartitionConsumer("persistent://public/default/my-topic-partition-0");
        ConsumerImpl<byte[]> partition1 =
                mockPartitionConsumer("persistent://public/default/my-topic-partition-1");

        impl.installSharedRetryOrDlqProducerProviderIfNeeded(partition0, conf);
        impl.installSharedRetryOrDlqProducerProviderIfNeeded(partition1, conf);

        // Exactly one shared provider for the single subscription-level topic.
        assertEquals(impl.getSharedRetryOrDlqProducerProvidersForTesting().size(), 1);
        SharedRetryOrDlqProducerProvider provider = impl.getSharedRetryOrDlqProducerProvidersForTesting()
                .get("persistent://public/default/my-topic");
        assertNotNull(provider, "Provider should be keyed by the subscription-level (non-partitioned) topic name");

        // Both partition sub-consumers must receive the exact same provider instance.
        ArgumentCaptor<RetryOrDlqProducerProvider> captor0 = ArgumentCaptor.forClass(RetryOrDlqProducerProvider.class);
        ArgumentCaptor<RetryOrDlqProducerProvider> captor1 = ArgumentCaptor.forClass(RetryOrDlqProducerProvider.class);
        verify(partition0, times(1)).setRetryOrDlqProducerProvider(captor0.capture());
        verify(partition1, times(1)).setRetryOrDlqProducerProvider(captor1.capture());
        assertSame(captor0.getValue(), provider);
        assertSame(captor1.getValue(), provider,
                "All partition sub-consumers of the same subscription must share the same provider instance");
    }

    @Test
    public void testSharedRetryProviderIsolatedPerSubscriptionTopic() {
        ConsumerConfigurationData<byte[]> conf = new ConsumerConfigurationData<>();
        conf.setSubscriptionName("subscriptionName");
        conf.setDeadLetterPolicy(sampleDeadLetterPolicy());

        MultiTopicsConsumerImpl<byte[]> impl = createMultiTopicsConsumer(conf);

        ConsumerImpl<byte[]> topicAPart0 =
                mockPartitionConsumer("persistent://public/default/topic-a-partition-0");
        ConsumerImpl<byte[]> topicAPart1 =
                mockPartitionConsumer("persistent://public/default/topic-a-partition-1");
        ConsumerImpl<byte[]> topicBPart0 =
                mockPartitionConsumer("persistent://public/default/topic-b-partition-0");
        ConsumerImpl<byte[]> topicBPart1 =
                mockPartitionConsumer("persistent://public/default/topic-b-partition-1");

        impl.installSharedRetryOrDlqProducerProviderIfNeeded(topicAPart0, conf);
        impl.installSharedRetryOrDlqProducerProviderIfNeeded(topicAPart1, conf);
        impl.installSharedRetryOrDlqProducerProviderIfNeeded(topicBPart0, conf);
        impl.installSharedRetryOrDlqProducerProviderIfNeeded(topicBPart1, conf);

        // One provider per subscription-level topic, independent of partition count.
        assertEquals(impl.getSharedRetryOrDlqProducerProvidersForTesting().size(), 2);
        SharedRetryOrDlqProducerProvider providerA =
                impl.getSharedRetryOrDlqProducerProvidersForTesting()
                        .get("persistent://public/default/topic-a");
        SharedRetryOrDlqProducerProvider providerB =
                impl.getSharedRetryOrDlqProducerProvidersForTesting()
                        .get("persistent://public/default/topic-b");
        assertNotNull(providerA);
        assertNotNull(providerB);
        assertFalse(providerA == providerB,
                "Different subscription-level topics must get isolated provider instances");

        // Capture every setRetryOrDlqProducerProvider call per consumer and verify grouping.
        ArgumentCaptor<RetryOrDlqProducerProvider> aPart0Captor =
                ArgumentCaptor.forClass(RetryOrDlqProducerProvider.class);
        ArgumentCaptor<RetryOrDlqProducerProvider> aPart1Captor =
                ArgumentCaptor.forClass(RetryOrDlqProducerProvider.class);
        ArgumentCaptor<RetryOrDlqProducerProvider> bPart0Captor =
                ArgumentCaptor.forClass(RetryOrDlqProducerProvider.class);
        ArgumentCaptor<RetryOrDlqProducerProvider> bPart1Captor =
                ArgumentCaptor.forClass(RetryOrDlqProducerProvider.class);
        verify(topicAPart0).setRetryOrDlqProducerProvider(aPart0Captor.capture());
        verify(topicAPart1).setRetryOrDlqProducerProvider(aPart1Captor.capture());
        verify(topicBPart0).setRetryOrDlqProducerProvider(bPart0Captor.capture());
        verify(topicBPart1).setRetryOrDlqProducerProvider(bPart1Captor.capture());
        assertSame(aPart0Captor.getValue(), providerA);
        assertSame(aPart1Captor.getValue(), providerA);
        assertSame(bPart0Captor.getValue(), providerB);
        assertSame(bPart1Captor.getValue(), providerB);
    }

    @Test
    public void testSharedRetryProviderUsesSubscriptionLevelTopicNameForDefaultDlqTopics() {
        // Build a DeadLetterPolicy without explicit retry/DLQ topic names, so the provider
        // must synthesise defaults based on the subscription-level topic name.
        ConsumerConfigurationData<byte[]> conf = new ConsumerConfigurationData<>();
        conf.setSubscriptionName("mySub");
        conf.setDeadLetterPolicy(DeadLetterPolicy.builder().maxRedeliverCount(5).build());

        MultiTopicsConsumerImpl<byte[]> impl = createMultiTopicsConsumer(conf);

        ConsumerImpl<byte[]> partition0 =
                mockPartitionConsumer("persistent://public/default/orders-partition-0");
        impl.installSharedRetryOrDlqProducerProviderIfNeeded(partition0, conf);

        SharedRetryOrDlqProducerProvider provider = impl.getSharedRetryOrDlqProducerProvidersForTesting()
                .get("persistent://public/default/orders");
        assertNotNull(provider);
        assertEquals(provider.getSubscriptionTopicNameForTesting(),
                "persistent://public/default/orders");

        DeadLetterPolicy effective = provider.getDeadLetterPolicyForTesting();
        assertNotNull(effective);
        // Defaults must be based on the subscription-level topic name, NOT the partition name.
        assertEquals(effective.getRetryLetterTopic(),
                "persistent://public/default/orders-mySub-RETRY");
        assertEquals(effective.getDeadLetterTopic(),
                "persistent://public/default/orders-mySub-DLQ");
        assertEquals(effective.getMaxRedeliverCount(), 5);
    }

    @Test
    public void testSharedRetryProviderCloseIsNoopWhenNoProducerCreated() throws Exception {
        // Even if a provider was installed, if no retry/DLQ producer has actually been
        // created yet (no reconsumeLater triggered), close() must complete immediately
        // and must not blow up.
        ConsumerConfigurationData<byte[]> conf = new ConsumerConfigurationData<>();
        conf.setSubscriptionName("subscriptionName");
        conf.setDeadLetterPolicy(sampleDeadLetterPolicy());

        MultiTopicsConsumerImpl<byte[]> impl = createMultiTopicsConsumer(conf);
        ConsumerImpl<byte[]> partition0 =
                mockPartitionConsumer("persistent://public/default/my-topic-partition-0");
        impl.installSharedRetryOrDlqProducerProviderIfNeeded(partition0, conf);

        SharedRetryOrDlqProducerProvider provider = impl.getSharedRetryOrDlqProducerProvidersForTesting()
                .get("persistent://public/default/my-topic");
        assertNotNull(provider);

        CompletableFuture<Void> closeFuture = provider.close();
        closeFuture.get(5, TimeUnit.SECONDS);
        assertTrue(closeFuture.isDone());
        assertFalse(closeFuture.isCompletedExceptionally());

        // Idempotent: calling close() a second time must also be fine.
        provider.close().get(5, TimeUnit.SECONDS);
    }

    // -------------------------------------------------------------------------
    // Tests covering regressions guarded by the Oct-2026 hardening pass:
    //   * close()-vs-getOrCreate* race (problem B)
    //   * dynamic partition expansion updating the partition hint (problem C)
    //   * cleanup of shared providers on topic unsubscribe (problem E)
    //   * close() idempotency via CAS
    // -------------------------------------------------------------------------

    @Test
    public void testSharedRetryProviderGetOrCreateAfterCloseFailsFast() throws Exception {
        // Problem B: once close() has been called, any subsequent getOrCreate* call must
        // return an AlreadyClosedException instead of handing out a brand-new producer
        // that would never be closed (resource leak) or a snapshotted stale producer
        // that was about to be closed (use-after-close).
        ConsumerConfigurationData<byte[]> conf = new ConsumerConfigurationData<>();
        conf.setSubscriptionName("subscriptionName");
        conf.setDeadLetterPolicy(sampleDeadLetterPolicy());

        MultiTopicsConsumerImpl<byte[]> impl = createMultiTopicsConsumer(conf);
        ConsumerImpl<byte[]> partition0 =
                mockPartitionConsumer("persistent://public/default/my-topic-partition-0");
        impl.installSharedRetryOrDlqProducerProviderIfNeeded(partition0, conf);

        SharedRetryOrDlqProducerProvider provider = impl.getSharedRetryOrDlqProducerProvidersForTesting()
                .get("persistent://public/default/my-topic");
        assertNotNull(provider);
        assertFalse(provider.isClosed());

        provider.close().get(5, TimeUnit.SECONDS);
        assertTrue(provider.isClosed());

        // Retry producer: must fail with AlreadyClosedException without creating a producer.
        CompletableFuture<org.apache.pulsar.client.api.Producer<byte[]>> retryFuture =
                provider.getOrCreateRetryLetterProducer(Schema.BYTES);
        assertTrue(retryFuture.isCompletedExceptionally());
        Throwable retryCause = expectThrows(CompletionException.class, retryFuture::join).getCause();
        assertTrue(retryCause instanceof PulsarClientException.AlreadyClosedException,
                "Expected AlreadyClosedException but got " + retryCause);

        // Dead-letter producer: same contract.
        CompletableFuture<org.apache.pulsar.client.api.Producer<byte[]>> dlqFuture =
                provider.getOrCreateDeadLetterProducer(Schema.BYTES);
        assertTrue(dlqFuture.isCompletedExceptionally());
        Throwable dlqCause = expectThrows(CompletionException.class, dlqFuture::join).getCause();
        assertTrue(dlqCause instanceof PulsarClientException.AlreadyClosedException,
                "Expected AlreadyClosedException but got " + dlqCause);
    }

    @Test
    public void testSharedRetryProviderCloseIsIdempotent() throws Exception {
        // The close() path uses a CAS-based `closed` flag so that only the first invocation
        // actually drives the underlying producer teardown. Subsequent calls must return
        // immediately-completed futures and must not throw or NPE.
        ConsumerConfigurationData<byte[]> conf = new ConsumerConfigurationData<>();
        conf.setSubscriptionName("subscriptionName");
        conf.setDeadLetterPolicy(sampleDeadLetterPolicy());

        MultiTopicsConsumerImpl<byte[]> impl = createMultiTopicsConsumer(conf);
        ConsumerImpl<byte[]> partition0 =
                mockPartitionConsumer("persistent://public/default/my-topic-partition-0");
        impl.installSharedRetryOrDlqProducerProviderIfNeeded(partition0, conf);

        SharedRetryOrDlqProducerProvider provider = impl.getSharedRetryOrDlqProducerProvidersForTesting()
                .get("persistent://public/default/my-topic");
        assertNotNull(provider);

        // Call close() concurrently from multiple threads -- all must observe success and
        // the `closed` state must flip exactly once.
        int threadCount = 8;
        java.util.concurrent.CountDownLatch startGate = new java.util.concurrent.CountDownLatch(1);
        java.util.List<CompletableFuture<Void>> results = new java.util.ArrayList<>();
        java.util.List<Thread> workers = new java.util.ArrayList<>();
        for (int i = 0; i < threadCount; i++) {
            CompletableFuture<Void> slot = new CompletableFuture<>();
            results.add(slot);
            Thread t = new Thread(() -> {
                try {
                    startGate.await();
                    provider.close().get(5, TimeUnit.SECONDS);
                    slot.complete(null);
                } catch (Throwable ex) {
                    slot.completeExceptionally(ex);
                }
            }, "close-racer-" + i);
            t.setDaemon(true);
            workers.add(t);
            t.start();
        }
        startGate.countDown();
        for (Thread t : workers) {
            t.join(10_000);
        }
        for (CompletableFuture<Void> r : results) {
            r.get(5, TimeUnit.SECONDS); // throws if any invocation failed
        }
        assertTrue(provider.isClosed());
    }

    @Test
    public void testSharedRetryProviderUpdatePartitionHint() {
        // Problem C: partition expansion must propagate the new partition count to the shared
        // provider so that future producer creations size their maxPendingMessages against the
        // new partition count. Already-created producers keep their existing size.
        ConsumerConfigurationData<byte[]> conf = new ConsumerConfigurationData<>();
        conf.setSubscriptionName("subscriptionName");
        conf.setDeadLetterPolicy(sampleDeadLetterPolicy());

        MultiTopicsConsumerImpl<byte[]> impl = createMultiTopicsConsumer(conf);
        // Seed the subscription-level topic with a known initial partition count.
        String subscriptionTopic = "persistent://public/default/my-topic";
        impl.partitionedTopics.put(subscriptionTopic, 2);

        ConsumerImpl<byte[]> partition0 =
                mockPartitionConsumer(subscriptionTopic + "-partition-0");
        impl.installSharedRetryOrDlqProducerProviderIfNeeded(partition0, conf);

        SharedRetryOrDlqProducerProvider provider = impl.getSharedRetryOrDlqProducerProvidersForTesting()
                .get(subscriptionTopic);
        assertNotNull(provider);
        assertEquals(provider.getPartitionHintForTesting(), 2);

        // Simulate a partition expansion.
        provider.updatePartitionHint(8);
        assertEquals(provider.getPartitionHintForTesting(), 8);

        // Non-positive values must be ignored (no-op) to protect against buggy callers.
        provider.updatePartitionHint(0);
        provider.updatePartitionHint(-1);
        assertEquals(provider.getPartitionHintForTesting(), 8);

        // Updating with the same value must be a no-op but must not throw.
        provider.updatePartitionHint(8);
        assertEquals(provider.getPartitionHintForTesting(), 8);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSharedRetryProviderClearedWhenTopicDeletedViaPartitionChangedListener() {
        // Problem E (one of its paths): when the subscription-level topic is deleted on the
        // broker and the partitions-changed listener tears down all per-partition consumers,
        // the shared provider for that topic MUST be released -- otherwise long-lived Pattern
        // subscriptions would leak providers (and the underlying retry/DLQ producers) over time.
        String topicName = "persistent://public/default/deleted-shared-topic";
        String topicPartition0 = topicName + "-partition-0";
        String topicPartition1 = topicName + "-partition-1";

        ConsumerConfigurationData<byte[]> conf = new ConsumerConfigurationData<>();
        conf.setSubscriptionName("subscriptionName");
        conf.setDeadLetterPolicy(sampleDeadLetterPolicy());
        conf.setAutoUpdatePartitions(true);
        conf.setAutoUpdatePartitionsIntervalSeconds(60);
        conf.setAckTimeoutMillis(1000);

        MultiTopicsConsumerImpl<byte[]> impl = createMultiTopicsConsumer(conf);
        impl.partitionedTopics.put(topicName, 2);
        impl.allTopicPartitionsNumber.set(2);

        ConsumerImpl<byte[]> partition0 = (ConsumerImpl<byte[]>) mock(ConsumerImpl.class);
        ConsumerImpl<byte[]> partition1 = (ConsumerImpl<byte[]>) mock(ConsumerImpl.class);
        when(partition0.getTopic()).thenReturn(topicPartition0);
        when(partition1.getTopic()).thenReturn(topicPartition1);
        when(partition0.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));
        when(partition1.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));
        impl.consumers.put(topicPartition0, partition0);
        impl.consumers.put(topicPartition1, partition1);

        // Install the shared provider the same way createInternalConsumer would.
        impl.installSharedRetryOrDlqProducerProviderIfNeeded(partition0, conf);
        impl.installSharedRetryOrDlqProducerProviderIfNeeded(partition1, conf);
        SharedRetryOrDlqProducerProvider providerBeforeDeletion =
                impl.getSharedRetryOrDlqProducerProvidersForTesting().get(topicName);
        assertNotNull(providerBeforeDeletion, "Provider must have been installed for the partitioned topic");

        // Simulate the broker reporting that the topic has been deleted (no partitions returned).
        when(impl.client.getPartitionsForTopic(topicName, false))
                .thenReturn(CompletableFuture.completedFuture(Collections.emptyList()));

        PartitionsChangedListener listener = impl.topicsPartitionChangedListener;
        listener.onTopicsExtended(Collections.singleton(topicName)).join();

        // Sub-consumers should be gone, and -- critically -- the shared provider must be gone too.
        assertTrue(impl.getConsumers().isEmpty());
        assertFalse(impl.getSharedRetryOrDlqProducerProvidersForTesting().containsKey(topicName),
                "Shared retry/DLQ provider must be released after the topic has been fully removed");
        assertTrue(providerBeforeDeletion.isClosed(),
                "The removed provider instance must have been closed to release underlying producers");
    }

    @Test
    public void testSharedRetryProviderClearedOnUnsubscribeAsync() throws Exception {
        // Problem E (second path): an explicit unsubscribeAsync(topicName) call must also drop
        // the shared provider cached under that subscription-level topic. We exercise this
        // path indirectly by invoking the private helper via its package-private surface so the
        // test can run without a real broker.
        ConsumerConfigurationData<byte[]> conf = new ConsumerConfigurationData<>();
        conf.setSubscriptionName("subscriptionName");
        conf.setDeadLetterPolicy(sampleDeadLetterPolicy());

        MultiTopicsConsumerImpl<byte[]> impl = createMultiTopicsConsumer(conf);
        String subscriptionTopic = "persistent://public/default/to-be-unsubscribed";
        ConsumerImpl<byte[]> partition0 =
                mockPartitionConsumer(subscriptionTopic + "-partition-0");
        impl.installSharedRetryOrDlqProducerProviderIfNeeded(partition0, conf);

        SharedRetryOrDlqProducerProvider provider = impl.getSharedRetryOrDlqProducerProvidersForTesting()
                .get(subscriptionTopic);
        assertNotNull(provider);
        assertFalse(provider.isClosed());

        // Reflectively invoke the package-private helper that unsubscribeAsync() uses on the
        // success branch to release the shared provider. Doing it this way keeps the test
        // self-contained (no broker, no partition metadata plumbing).
        java.lang.reflect.Method remove = MultiTopicsConsumerImpl.class
                .getDeclaredMethod("removeSharedRetryOrDlqProducerProvider", String.class);
        remove.setAccessible(true);
        remove.invoke(impl, subscriptionTopic);

        assertFalse(impl.getSharedRetryOrDlqProducerProvidersForTesting().containsKey(subscriptionTopic),
                "Shared provider must be removed from the map on unsubscribe");
        // Give the close future a short window to resolve.
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(provider::isClosed);

        // Calling the helper again for a missing key must be a no-op (defensive contract).
        remove.invoke(impl, subscriptionTopic);
        remove.invoke(impl, "persistent://public/default/does-not-exist");
    }

}
