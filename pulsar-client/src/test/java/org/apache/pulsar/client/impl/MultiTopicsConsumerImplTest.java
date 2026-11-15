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
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import com.google.common.collect.Sets;
import io.netty.buffer.Unpooled;
import io.netty.channel.EventLoopGroup;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import lombok.Cleanup;
import org.apache.pulsar.client.api.BatchReceivePolicy;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Messages;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.util.netty.EventLoopUtil;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
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

    @Test(timeOut = 10000)
    @SuppressWarnings("unchecked")
    public void testRedeliverGivesBackPermitsOfMessagesQueuedForListener() throws Exception {
        String topic = "persistent://public/default/topic-partition-0";
        int numMessages = 3;
        AtomicInteger listenedMessages = new AtomicInteger();
        ConsumerConfigurationData<byte[]> conf = new ConsumerConfigurationData<>();
        conf.setSubscriptionName("subscriptionName");
        conf.setSubscriptionType(SubscriptionType.Failover);
        conf.setMessageListenerExecutor((message, runnable) -> runnable.run());
        conf.setMessageListener((consumer, message) -> listenedMessages.incrementAndGet());
        MultiTopicsConsumerImpl<byte[]> impl = createMultiTopicsConsumer(conf);

        ConsumerImpl<byte[]> partitionConsumer = mock(ConsumerImpl.class);
        when(partitionConsumer.getTopic()).thenReturn(topic);
        when(partitionConsumer.isValidConsumerEpoch(any())).thenReturn(true);

        impl.internalPinnedExecutor.submit(() -> {
            // Queue messages for the listener, as receiving them from a partition consumer does...
            for (int i = 0; i < numMessages; i++) {
                MessageImpl<byte[]> message = MessageImpl.create(topic, new MessageIdImpl(1, i, 0),
                        new MessageMetadata(), Unpooled.wrappedBuffer(new byte[1]), Optional.empty(), null,
                        Schema.BYTES, 0, false, 0);
                impl.enqueueMessageAndCheckBatchReceive(new TopicMessageImpl<>(topic, message, partitionConsumer));
            }
            // ...while another thread requests a redelivery, whose task is queued before the listener drain
            impl.redeliverUnacknowledgedMessages();
            impl.tryTriggerListener();
        }).get();
        // The redelivery and the listener drain run on the pinned executor before this task
        impl.internalPinnedExecutor.submit(() -> { }).get();

        // The redelivery discarded the messages, so each must give its permit back to the consumer that received it
        assertEquals(listenedMessages.get(), 0);
        verify(partitionConsumer, times(numMessages)).increaseAvailablePermits(any(MessageImpl.class));
    }

    @Test(timeOut = 10000)
    @SuppressWarnings("unchecked")
    public void testSelectiveSeekGivesBackPermitsOfDroppedMessageOnce() throws Exception {
        String soughtTopic = "persistent://public/default/topic-partition-0";
        String skippedTopic = "persistent://public/default/topic-partition-1";
        int receiverQueueSize = 1;
        AtomicInteger listenedMessages = new AtomicInteger();
        ConsumerConfigurationData<byte[]> conf = new ConsumerConfigurationData<>();
        conf.setSubscriptionName("subscriptionName");
        conf.setMessageListenerExecutor((message, runnable) -> runnable.run());
        conf.setMessageListener((consumer, message) -> listenedMessages.incrementAndGet());
        MultiTopicsConsumerImpl<byte[]> impl = createMultiTopicsConsumer(conf);
        ClientCnx cnx = mock(ClientCnx.class);
        ConsumerImpl<byte[]> soughtConsumer = addPartitionConsumer(impl, soughtTopic, cnx, receiverQueueSize);
        ConsumerImpl<byte[]> skippedConsumer = addPartitionConsumer(impl, skippedTopic, cnx, receiverQueueSize);
        // Seeking a message of one partition skips the other, whose consumer neither seeks nor reconnects
        TopicMessageIdImpl seekMessageId = new TopicMessageIdImpl(soughtTopic, new MessageIdImpl(1, 0, 0));
        when(soughtConsumer.seekAsync(seekMessageId)).thenReturn(CompletableFuture.completedFuture(null));

        impl.internalPinnedExecutor.submit(() -> {
            // Queue a message of each partition for the listener, as receiving them from their consumers does
            impl.enqueueMessageAndCheckBatchReceive(
                    new TopicMessageImpl<>(soughtTopic, newMessage(soughtTopic, 1, 1, 0), soughtConsumer));
            impl.enqueueMessageAndCheckBatchReceive(
                    new TopicMessageImpl<>(skippedTopic, newMessage(skippedTopic, 2, 0, 1), skippedConsumer));
        }).get();
        impl.seekAsync(seekMessageId).get();
        // The receive tasks that the seek started run on the pinned executor before this task
        impl.internalPinnedExecutor.submit(() -> { }).get();

        // The seek dropped the message of the partition that sought, and refilled the receiver queue of its consumer:
        // the refill covers the message's permit, so giving it back too would credit it twice
        assertEquals(listenedMessages.get(), 0);
        verify(soughtConsumer, never()).increaseAvailablePermits(any(MessageImpl.class));
        verify(soughtConsumer).increaseAvailablePermits(cnx, receiverQueueSize);
        // The message of the skipped partition stays queued, still holding its permit
        assertEquals(impl.getTotalIncomingMessages(), 1);
        verify(skippedConsumer, never()).increaseAvailablePermits(any(MessageImpl.class));
        verify(skippedConsumer, never()).increaseAvailablePermits(any(ClientCnx.class), anyInt());
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

    @DataProvider
    public static Object[][] seekByFunction() {
        return new Object[][] {{false}, {true}};
    }

    @Test(timeOut = 10000, dataProvider = "seekByFunction")
    @SuppressWarnings("unchecked")
    public void testSelectiveSeekKeepsMessagesOfSkippedPartition(boolean seekByFunction) throws Exception {
        String soughtTopic = "persistent://public/default/topic-partition-0";
        String skippedTopic = "persistent://public/default/topic-partition-1";
        ConsumerConfigurationData<byte[]> conf = new ConsumerConfigurationData<>();
        conf.setSubscriptionName("subscriptionName");
        conf.setAckTimeoutMillis(1000);
        MultiTopicsConsumerImpl<byte[]> impl = createMultiTopicsConsumer(conf);
        ConsumerImpl<byte[]> soughtConsumer = addPartitionConsumer(impl, soughtTopic, null, 10);
        ConsumerImpl<byte[]> skippedConsumer = addPartitionConsumer(impl, skippedTopic, null, 10);
        CompletableFuture<Void> soughtSeek = new CompletableFuture<>();
        when(soughtConsumer.seekAsync(any(MessageId.class))).thenReturn(soughtSeek);
        when(soughtConsumer.seekAsync(any(Function.class))).thenReturn(soughtSeek);
        CompletableFuture<Messages<byte[]>> handedOver = new CompletableFuture<>();
        when(skippedConsumer.batchReceiveAsync()).thenReturn(handedOver, new CompletableFuture<>());

        // Each partition has a message queued, and a delivered message that is not acknowledged yet
        impl.enqueueMessageAndCheckBatchReceive(
                new TopicMessageImpl<>(soughtTopic, newMessage(soughtTopic, 1, 1, 0), soughtConsumer));
        impl.enqueueMessageAndCheckBatchReceive(
                new TopicMessageImpl<>(skippedTopic, newMessage(skippedTopic, 2, 1, 1), skippedConsumer));
        TopicMessageIdImpl soughtUnacked = new TopicMessageIdImpl(soughtTopic, new MessageIdImpl(1, 0, 0));
        TopicMessageIdImpl skippedUnacked = new TopicMessageIdImpl(skippedTopic, new MessageIdImpl(2, 0, 1));
        impl.getUnAckedMessageTracker().add(soughtUnacked);
        impl.getUnAckedMessageTracker().add(skippedUnacked);

        // While the seek of one partition is in progress, the consumer of the other partition hands over a message
        impl.receiveMessageFromConsumer(skippedConsumer, true);
        CompletableFuture<Void> seek = seekPartition(impl, seekByFunction, soughtTopic, new MessageIdImpl(1, 0, 0));
        MessagesImpl<byte[]> messages = new MessagesImpl<>(-1, -1);
        messages.add(newMessage(skippedTopic, 2, 2, 1));
        handedOver.complete(messages);
        impl.internalPinnedExecutor.submit(() -> { }).get();
        soughtSeek.complete(null);
        seek.get();
        impl.internalPinnedExecutor.submit(() -> { }).get();

        // The seek dropped the queued and the unacknowledged messages of the partition that sought only
        assertEquals(impl.getTotalIncomingMessages(), 2);
        assertEquals(impl.getIncomingMessageSize(), 2);
        assertEquals(impl.incomingMessages.poll().getMessageId(), new MessageIdImpl(2, 1, 1));
        assertEquals(impl.incomingMessages.poll().getMessageId(), new MessageIdImpl(2, 2, 1));
        assertEquals(impl.getUnAckedMessageTracker().size(), 1);
        assertTrue(impl.getUnAckedMessageTracker().remove(skippedUnacked));
    }

    @Test(timeOut = 10000, dataProvider = "seekByFunction")
    @SuppressWarnings("unchecked")
    public void testSelectiveSeekRefillsOnlySoughtPartition(boolean seekByFunction) throws Exception {
        String soughtTopic = "persistent://public/default/topic-partition-0";
        String skippedTopic = "persistent://public/default/topic-partition-1";
        int receiverQueueSize = 10;
        ConsumerConfigurationData<byte[]> conf = new ConsumerConfigurationData<>();
        conf.setSubscriptionName("subscriptionName");
        MultiTopicsConsumerImpl<byte[]> impl = createMultiTopicsConsumer(conf);
        ClientCnx cnx = mock(ClientCnx.class);
        ConsumerImpl<byte[]> soughtConsumer = addPartitionConsumer(impl, soughtTopic, cnx, receiverQueueSize);
        ConsumerImpl<byte[]> skippedConsumer = addPartitionConsumer(impl, skippedTopic, cnx, receiverQueueSize);
        when(soughtConsumer.seekAsync(any(MessageId.class))).thenReturn(CompletableFuture.completedFuture(null));
        when(soughtConsumer.seekAsync(any(Function.class))).thenReturn(CompletableFuture.completedFuture(null));

        seekPartition(impl, seekByFunction, soughtTopic, new MessageIdImpl(1, 0, 0)).get();
        // The receive tasks that the seek started run on the pinned executor before this task
        impl.internalPinnedExecutor.submit(() -> { }).get();

        // The consumer that sought gets a full receiver queue of permits. The consumer that the seek skipped kept its
        // permits and its connection, and kept receiving: the seek gives it nothing.
        verify(soughtConsumer).increaseAvailablePermits(cnx, receiverQueueSize);
        verify(skippedConsumer, never()).increaseAvailablePermits(any(ClientCnx.class), anyInt());
        verify(skippedConsumer, never()).batchReceiveAsync();
    }

    @Test(timeOut = 10000)
    public void testSeekKeepsOneReceiveLoopPerConsumer() throws Exception {
        String stoppedTopic = "persistent://public/default/topic-partition-0";
        String waitingTopic = "persistent://public/default/topic-partition-1";
        MultiTopicsConsumerImpl<byte[]> impl = createMultiTopicsConsumer();
        ConsumerImpl<byte[]> stoppedConsumer = addPartitionConsumer(impl, stoppedTopic, null, 10);
        ConsumerImpl<byte[]> waitingConsumer = addPartitionConsumer(impl, waitingTopic, null, 10);
        List<CompletableFuture<?>> stoppedReceives = recordReceives(stoppedConsumer);
        List<CompletableFuture<?>> waitingReceives = recordReceives(waitingConsumer);
        CompletableFuture<Void> partitionSeeks = new CompletableFuture<>();
        when(stoppedConsumer.seekAsync(any(MessageId.class))).thenReturn(partitionSeeks);
        when(waitingConsumer.seekAsync(any(MessageId.class))).thenReturn(partitionSeeks);

        // Both receive loops wait for the next message of their consumer
        impl.internalPinnedExecutor.submit(() -> {
            impl.receiveMessageFromConsumer(stoppedConsumer, false);
            impl.receiveMessageFromConsumer(waitingConsumer, false);
        }).get();
        CompletableFuture<Void> seek = impl.seekAsync(MessageId.earliest);
        // A message from before the seek reaches one of the loops during the seek: the loop drops it, and stops until
        // the seek completes. The other loop still waits for its receive.
        completeReceive(stoppedReceives.get(0), newMessage(stoppedTopic, 1, 2, 0));
        impl.internalPinnedExecutor.submit(() -> { }).get();
        assertEquals(impl.getTotalIncomingMessages(), 0);
        assertEquals(stoppedReceives.size(), 1);

        partitionSeeks.complete(null);
        seek.get();
        // The restart of the stopped loop runs on the pinned executor before this task
        impl.internalPinnedExecutor.submit(() -> { }).get();

        // The seek restarted the stopped loop only. A second loop on the consumer whose loop still waits would race
        // the first one for its messages, and could hand a later message over first.
        assertEquals(stoppedReceives.size(), 2);
        assertEquals(waitingReceives.size(), 1);

        // The waiting loop goes on when its receive completes
        completeReceive(waitingReceives.get(0), newMessage(waitingTopic, 2, 0, 1));
        impl.internalPinnedExecutor.submit(() -> { }).get();
        assertEquals(impl.getTotalIncomingMessages(), 1);
        assertEquals(waitingReceives.size(), 2);
    }

    @Test(timeOut = 10000)
    public void testSeekLeavesConsumerReplacedDuringSeekToItsReplacement() throws Exception {
        String replacedTopic = "persistent://public/default/topic-partition-0";
        String slowTopic = "persistent://public/default/topic-partition-1";
        int receiverQueueSize = 10;
        MultiTopicsConsumerImpl<byte[]> impl = createMultiTopicsConsumer();
        ClientCnx cnx = mock(ClientCnx.class);
        ConsumerImpl<byte[]> replacedConsumer = addPartitionConsumer(impl, replacedTopic, cnx, receiverQueueSize);
        ConsumerImpl<byte[]> slowConsumer = addPartitionConsumer(impl, slowTopic, cnx, receiverQueueSize);
        List<CompletableFuture<?>> replacedReceives = recordReceives(replacedConsumer);
        when(replacedConsumer.seekAsync(any(MessageId.class))).thenReturn(CompletableFuture.completedFuture(null));
        CompletableFuture<Void> slowSeek = new CompletableFuture<>();
        when(slowConsumer.seekAsync(any(MessageId.class))).thenReturn(slowSeek);

        impl.internalPinnedExecutor.submit(() -> impl.receiveMessageFromConsumer(replacedConsumer, false)).get();
        CompletableFuture<Void> seek = impl.seekAsync(MessageId.earliest);
        // The seek of one partition completed. Its loop dropped a message from before the seek, and stopped until the
        // seek of the other partition completes too.
        completeReceive(replacedReceives.get(0), newMessage(replacedTopic, 1, 0, 0));
        impl.internalPinnedExecutor.submit(() -> { }).get();

        // Meanwhile, unsubscribing and subscribing the topic of the first partition again replaces its consumer. The
        // new consumer does not seek: its loop receives, and its messages reach the queue.
        ConsumerImpl<byte[]> replacement = addPartitionConsumer(impl, replacedTopic, cnx, receiverQueueSize);
        List<CompletableFuture<?>> replacementReceives = recordReceives(replacement);
        impl.internalPinnedExecutor.submit(() -> impl.receiveMessageFromConsumer(replacement, false)).get();
        assertEquals(replacementReceives.size(), 1);
        completeReceive(replacementReceives.get(0), newMessage(replacedTopic, 2, 0, 0));
        impl.internalPinnedExecutor.submit(() -> { }).get();
        assertEquals(impl.getTotalIncomingMessages(), 1);
        assertEquals(replacementReceives.size(), 2);

        slowSeek.complete(null);
        seek.get();
        impl.internalPinnedExecutor.submit(() -> { }).get();

        // The seek neither restarts nor refills the replaced consumer, nor the replacement, which got its permits when
        // it subscribed. It refills the other consumer that sought.
        assertEquals(replacedReceives.size(), 1);
        verify(replacedConsumer, never()).increaseAvailablePermits(any(ClientCnx.class), anyInt());
        assertEquals(replacementReceives.size(), 2);
        verify(replacement, never()).increaseAvailablePermits(any(ClientCnx.class), anyInt());
        verify(slowConsumer).increaseAvailablePermits(cnx, receiverQueueSize);
    }

    // Seek one partition only: by a message id of the partition, or by a function that gives no position for the other
    // partitions
    private static CompletableFuture<Void> seekPartition(MultiTopicsConsumerImpl<byte[]> impl, boolean byFunction,
                                                         String topic, MessageIdImpl messageId) {
        if (byFunction) {
            return impl.seekAsync(t -> t.equals(topic) ? messageId : null);
        }
        return impl.seekAsync(new TopicMessageIdImpl(topic, messageId));
    }

    @SuppressWarnings("unchecked")
    private static ConsumerImpl<byte[]> addPartitionConsumer(MultiTopicsConsumerImpl<byte[]> impl, String topic,
                                                             ClientCnx cnx, int receiverQueueSize) {
        ConnectionHandler connectionHandler = mock(ConnectionHandler.class);
        when(connectionHandler.cnx()).thenReturn(cnx);
        ConsumerImpl<byte[]> consumer = mock(ConsumerImpl.class);
        when(consumer.getTopic()).thenReturn(topic);
        when(consumer.getConnectionHandler()).thenReturn(connectionHandler);
        when(consumer.getCurrentReceiverQueueSize()).thenReturn(receiverQueueSize);
        when(consumer.batchReceiveAsync()).thenReturn(new CompletableFuture<>());
        when(consumer.receiveAsync()).thenReturn(new CompletableFuture<>());
        // What ConsumerImpl#seekAsync(Function) does for a topic that the function gives no position for
        when(consumer.seekAsync(any(Function.class))).thenReturn(CompletableFuture.completedFuture(null));
        impl.consumers.put(topic, consumer);
        return consumer;
    }

    private static MessageImpl<byte[]> newMessage(String topic, long ledgerId, long entryId, int partitionIndex) {
        return MessageImpl.create(topic, new MessageIdImpl(ledgerId, entryId, partitionIndex), new MessageMetadata(),
                Unpooled.wrappedBuffer(new byte[1]), Optional.empty(), null, Schema.BYTES, 0, false, 0);
    }

    // Make each receive request of a partition consumer wait until the test completes it, and record it
    private static List<CompletableFuture<?>> recordReceives(ConsumerImpl<byte[]> consumer) {
        List<CompletableFuture<?>> receives = new CopyOnWriteArrayList<>();
        when(consumer.receiveAsync()).thenAnswer(invocation -> {
            CompletableFuture<Message<byte[]>> receive = new CompletableFuture<>();
            receives.add(receive);
            return receive;
        });
        when(consumer.batchReceiveAsync()).thenAnswer(invocation -> {
            CompletableFuture<Messages<byte[]>> receive = new CompletableFuture<>();
            receives.add(receive);
            return receive;
        });
        return receives;
    }

    // Complete a receive request for a single message, see receiveMessageFromConsumer(consumer, false)
    @SuppressWarnings("unchecked")
    private static void completeReceive(CompletableFuture<?> receive, MessageImpl<byte[]> message) {
        assertTrue(((CompletableFuture<Message<byte[]>>) receive).complete(message));
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

    @Test(timeOut = 10000)
    @SuppressWarnings("unchecked")
    public void testListenerDrainGivesBackPermitsAfterRedeliver() throws Exception {
        String topic = "persistent://public/default/topic-partition-0";
        int numMessages = 3;
        AtomicInteger queuedAtRedeliver = new AtomicInteger(-1);
        ConsumerConfigurationData<byte[]> conf = new ConsumerConfigurationData<>();
        conf.setSubscriptionName("subscriptionName");
        conf.setSubscriptionType(SubscriptionType.Failover);
        // Run the listener inside the listener drain, so that it requests the redelivery while the drain still has
        // queued messages
        conf.setMessageListenerExecutor((message, runnable) -> runnable.run());
        conf.setMessageListener((consumer, message) -> {
            if (queuedAtRedeliver.get() < 0) {
                queuedAtRedeliver.set(((ConsumerBase<byte[]>) consumer).getTotalIncomingMessages());
                consumer.redeliverUnacknowledgedMessages();
            }
        });
        MultiTopicsConsumerImpl<byte[]> impl = createMultiTopicsConsumer(conf);

        ConsumerImpl<byte[]> partitionConsumer = mock(ConsumerImpl.class);
        when(partitionConsumer.isValidConsumerEpoch(any())).thenReturn(true);

        // Queue messages received before the redelivery, then start the listener drain
        impl.internalPinnedExecutor.submit(() -> {
            for (int i = 0; i < numMessages; i++) {
                MessageImpl<byte[]> message = MessageImpl.create(topic, new MessageIdImpl(1, i, 0),
                        new MessageMetadata(), Unpooled.wrappedBuffer(new byte[1]), Optional.empty(), null,
                        Schema.BYTES, 0, false, 0);
                impl.enqueueMessageAndCheckBatchReceive(new TopicMessageImpl<>(topic, message, partitionConsumer));
            }
            impl.tryTriggerListener();
        }).get();
        // The drain runs on the pinned executor before this task
        impl.internalPinnedExecutor.submit(() -> { }).get();

        assertEquals(queuedAtRedeliver.get(), numMessages - 1);
        // Each drained message gave its permit back to the consumer that received it
        verify(partitionConsumer, times(numMessages)).increaseAvailablePermits(any(MessageImpl.class));
    }

    @DataProvider
    public static Object[][] hasListener() {
        return new Object[][] {{false}, {true}};
    }

    @Test(timeOut = 10000, dataProvider = "hasListener")
    @SuppressWarnings("unchecked")
    public void testOldEpochMessageFromSubConsumerGivesBackPermitOnce(boolean hasListener) throws Exception {
        String topic = "persistent://public/default/topic-partition-0";
        ConsumerConfigurationData<byte[]> conf = new ConsumerConfigurationData<>();
        conf.setSubscriptionName("subscriptionName");
        conf.setSubscriptionType(SubscriptionType.Failover);
        if (hasListener) {
            conf.setMessageListener((consumer, message) -> { });
        }
        MultiTopicsConsumerImpl<byte[]> impl = createMultiTopicsConsumer(conf);

        ConsumerImpl<byte[]> partitionConsumer = mock(ConsumerImpl.class);
        when(partitionConsumer.getTopic()).thenReturn(topic);
        CompletableFuture<Messages<byte[]>> handedOver = new CompletableFuture<>();
        when(partitionConsumer.batchReceiveAsync()).thenReturn(handedOver, new CompletableFuture<>());

        // Hold the pinned executor, so that the hand-over of a message received before the redelivery is queued
        // ahead of the redelivery task and runs after the epoch bump
        CountDownLatch release = new CountDownLatch(1);
        impl.internalPinnedExecutor.execute(() -> {
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        impl.receiveMessageFromConsumer(partitionConsumer, true);
        MessagesImpl<byte[]> messages = new MessagesImpl<>(-1, -1);
        messages.add(MessageImpl.create(topic, new MessageIdImpl(1, 0, 0), new MessageMetadata(),
                Unpooled.wrappedBuffer(new byte[1]), Optional.empty(), null, Schema.BYTES, 0, false, 0));
        handedOver.complete(messages);
        impl.redeliverUnacknowledgedMessages();
        release.countDown();
        impl.internalPinnedExecutor.submit(() -> { }).get();

        // The hand-over dropped the message. Without a listener, the partition consumer already gave its permit back
        // when it handed the message over.
        verify(partitionConsumer, times(hasListener ? 1 : 0)).increaseAvailablePermits(nullable(ClientCnx.class));
    }

    @Test(timeOut = 10000)
    public void testBatchReceiveWaitingForMessagesResumesPausedConsumers() throws Exception {
        String topic0 = "persistent://public/default/topic-partition-0";
        String topic1 = "persistent://public/default/topic-partition-1";
        ConsumerConfigurationData<byte[]> conf = new ConsumerConfigurationData<>();
        conf.setSubscriptionName("subscriptionName");
        conf.setReceiverQueueSize(4);
        // A batch takes a full receiver queue, and has no timeout to return fewer messages
        conf.setBatchReceivePolicy(BatchReceivePolicy.builder().maxNumMessages(4).build());
        MultiTopicsConsumerImpl<byte[]> impl = createMultiTopicsConsumer(conf);
        ConsumerImpl<byte[]> consumer0 = addPartitionConsumer(impl, topic0, null, 4);
        ConsumerImpl<byte[]> consumer1 = addPartitionConsumer(impl, topic1, null, 4);
        List<CompletableFuture<?>> receives0 = recordReceives(consumer0);
        List<CompletableFuture<?>> receives1 = recordReceives(consumer1);

        // Before the application asks for a batch, partition 0 fills the queue, and partition 1 hands over 3 more
        // messages: both are paused, with 7 messages in the queue
        impl.receiveMessageFromConsumer(consumer0, true);
        impl.receiveMessageFromConsumer(consumer1, true);
        completeBatchReceive(receives0.get(0), topic0, 0, 4);
        impl.internalPinnedExecutor.submit(() -> { }).get();
        completeBatchReceive(receives1.get(0), topic1, 1, 3);
        impl.internalPinnedExecutor.submit(() -> { }).get();
        assertEquals(impl.incomingMessages.size(), 7);
        assertEquals(receives0.size(), 1);
        assertEquals(receives1.size(), 1);

        // The first batch leaves 3 messages, more than half of the queue
        assertEquals(impl.internalBatchReceiveAsync().get().size(), 4);
        assertEquals(impl.incomingMessages.size(), 3);

        // The second batch waits for a 4th message: the paused partitions must be asked for more
        CompletableFuture<Messages<byte[]>> secondBatch = impl.internalBatchReceiveAsync();
        impl.internalPinnedExecutor.submit(() -> { }).get();
        impl.internalPinnedExecutor.submit(() -> { }).get();
        assertFalse(secondBatch.isDone());
        assertEquals(receives0.size(), 2);
        completeBatchReceive(receives0.get(1), topic0, 0, 1);
        assertEquals(secondBatch.get().size(), 4);
    }

    @SuppressWarnings("unchecked")
    private static void completeBatchReceive(CompletableFuture<?> receive, String topic, int partitionIndex,
                                             int numMessages) {
        MessagesImpl<byte[]> messages = new MessagesImpl<>(-1, -1);
        for (int i = 0; i < numMessages; i++) {
            messages.add(newMessage(topic, partitionIndex, i, partitionIndex));
        }
        assertTrue(((CompletableFuture<Messages<byte[]>>) receive).complete(messages));
    }

}
