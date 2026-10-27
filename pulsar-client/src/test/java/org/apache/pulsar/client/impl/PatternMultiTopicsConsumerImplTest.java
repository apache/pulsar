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
import static org.apache.pulsar.client.impl.ClientTestFixtures.createPulsarClientMockWithMockedClientCnx;
import static org.apache.pulsar.client.impl.ClientTestFixtures.mockClientCnx;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.common.api.proto.CommandGetTopicsOfNamespace;
import org.apache.pulsar.common.api.proto.CommandWatchTopicListSuccess;
import org.apache.pulsar.common.api.proto.CommandWatchTopicUpdate;
import org.apache.pulsar.common.lookup.GetTopicsResult;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.topics.TopicList;
import org.apache.pulsar.common.topics.TopicsPattern;
import org.apache.pulsar.common.topics.TopicsPatternFactory;
import org.awaitility.Awaitility;
import org.jspecify.annotations.NonNull;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Unit Tests of {@link PatternMultiTopicsConsumerImpl}.
 */
public class PatternMultiTopicsConsumerImplTest {
    private PatternMultiTopicsConsumerImpl.TopicsChangedListener mockListener;
    private ExecutorProvider executorProvider;
    private ExecutorService internalExecutor;
    private PulsarClientImpl clientMock;
    private ClientCnx cnx;
    private Timer timer;

    @BeforeMethod(alwaysRun = true)
    public void setUp() {
        executorProvider = new ExecutorProvider(1, "PatternMultiTopicsConsumerImplTest");
        internalExecutor = Executors.newSingleThreadScheduledExecutor();
        cnx = mockClientCnx();
        clientMock = createPulsarClientMockWithMockedClientCnx(executorProvider, internalExecutor, cnx);
        timer = new HashedWheelTimer();
        when(clientMock.timer()).thenReturn(timer);
        mockListener = mock(PatternMultiTopicsConsumerImpl.TopicsChangedListener.class);
        when(mockListener.onTopicsAdded(any())).thenReturn(CompletableFuture.completedFuture(null));
        when(mockListener.onTopicsRemoved(any())).thenReturn(CompletableFuture.completedFuture(null));
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
        if (timer != null) {
            timer.stop();
        }
    }

    @Test
    public void testChangedUnfilteredResponse() {
        PatternMultiTopicsConsumerImpl.updateSubscriptions(
                TopicsPatternFactory.create(Pattern.compile("tenant/my-ns/name-.*")),
                new GetTopicsResult(Arrays.asList(
                        "persistent://tenant/my-ns/name-1",
                        "persistent://tenant/my-ns/name-2",
                        "persistent://tenant/my-ns/non-matching"),
                        null, false, true),
                mockListener,
                Collections.emptyList(), "");
        verify(mockListener).onTopicsAdded(Sets.newHashSet(
                "persistent://tenant/my-ns/name-1",
                "persistent://tenant/my-ns/name-2"));
        verify(mockListener).onTopicsRemoved(Collections.emptySet());
    }

    @Test
    public void testChangedFilteredResponse() {
        PatternMultiTopicsConsumerImpl.updateSubscriptions(
                TopicsPatternFactory.create(Pattern.compile("tenant/my-ns/name-.*")),
                new GetTopicsResult(Arrays.asList(
                        "persistent://tenant/my-ns/name-0",
                        "persistent://tenant/my-ns/name-1",
                        "persistent://tenant/my-ns/name-2"),
                        "TOPICS_HASH", true, true),
                mockListener,
                Arrays.asList("persistent://tenant/my-ns/name-0"), "");
        verify(mockListener).onTopicsAdded(Sets.newHashSet(
                "persistent://tenant/my-ns/name-1",
                "persistent://tenant/my-ns/name-2"));
        verify(mockListener).onTopicsRemoved(Collections.emptySet());
    }

    @Test
    public void testUnchangedResponse() {
        PatternMultiTopicsConsumerImpl.updateSubscriptions(
                TopicsPatternFactory.create(Pattern.compile("tenant/my-ns/name-.*")),
                new GetTopicsResult(Arrays.asList(
                        "persistent://tenant/my-ns/name-0",
                        "persistent://tenant/my-ns/name-1",
                        "persistent://tenant/my-ns/name-2"),
                        "TOPICS_HASH", true, false),
                mockListener,
                Arrays.asList("persistent://tenant/my-ns/name-0"), "");
        verify(mockListener, never()).onTopicsAdded(any());
        verify(mockListener, never()).onTopicsRemoved(any());
    }

    @Test
    public void testPatternSubscribeAndReconcileLoop() throws Exception {
        TopicsPattern topicsPattern =
                TopicsPatternFactory.create("persistent://tenant/namespace/.*", TopicsPattern.RegexImplementation.JDK);
        ConsumerConfigurationData<byte[]> consumerConfData = createConsumerConfigurationData();
        consumerConfData.setPatternAutoDiscoveryPeriod(1);

        CopyOnWriteArrayList<String> topics = new CopyOnWriteArrayList<>();
        topics.add("persistent://tenant/namespace/topic1");
        doAnswer(invocationOnMock -> {
            long requestId = invocationOnMock.getArgument(0);
            long watcherId = invocationOnMock.getArgument(1);
            String localHash = invocationOnMock.getArgument(4);
            CommandWatchTopicListSuccess success = new CommandWatchTopicListSuccess();
            success.setRequestId(requestId);
            success.setWatcherId(watcherId);
            List<String> topicsCopy = new ArrayList<>(topics);
            String calculatedHash = TopicList.calculateHash(topicsCopy);
            if (!localHash.equals(calculatedHash)) {
                success.addAllTopics(topicsCopy);
            }
            success.setTopicsHash(calculatedHash);
            return CompletableFuture.completedFuture(success);
        }).when(cnx).newWatchTopicList(anyLong(), anyLong(), any(), any(), any());
        doReturn(true).when(cnx).isSupportsTopicWatchers();
        doReturn(true).when(cnx).isSupportsTopicWatcherReconcile();

        PatternMultiTopicsConsumerImpl<byte[]> consumer =
                createPatternMultiTopicsConsumer(consumerConfData, topicsPattern);
        assertThat(consumer.subscribeFuture).succeedsWithin(Duration.ofSeconds(5));
        assertThat(consumer.getWatcherFuture()).succeedsWithin(Duration.ofSeconds(5));
        Awaitility.await().untilAsserted(() -> {
            assertThat(consumer.getPartitions()).containsExactlyInAnyOrder("persistent://tenant/namespace/topic1");
        });
        topics.add("persistent://tenant/namespace/topic2");
        Awaitility.await().untilAsserted(() -> {
            assertThat(consumer.getPartitions()).containsExactlyInAnyOrder("persistent://tenant/namespace/topic1",
                    "persistent://tenant/namespace/topic2");
        });
    }

    @Test
    public void testPatternSubscribeWithoutWatcher() throws Exception {
        TopicsPattern topicsPattern =
                TopicsPatternFactory.create("persistent://tenant/namespace/.*", TopicsPattern.RegexImplementation.JDK);
        ConsumerConfigurationData<byte[]> consumerConfData = createConsumerConfigurationData();
        consumerConfData.setPatternAutoDiscoveryPeriod(1);

        CopyOnWriteArrayList<String> topics = new CopyOnWriteArrayList<>();
        topics.add("persistent://tenant/namespace/topic1");
        LookupService mockLookup = clientMock.getLookup();
        doAnswer(invocationOnMock -> {
            String localHash = invocationOnMock.getArgument(3);
            List<String> topicsCopy = new ArrayList<>(topics);
            String calculatedHash = TopicList.calculateHash(topicsCopy);
            boolean changed = false;
            if (!localHash.equals(calculatedHash)) {
                changed = true;
            }
            GetTopicsResult result = new GetTopicsResult(topicsCopy, calculatedHash, false, changed);
            return CompletableFuture.completedFuture(result);
        }).when(mockLookup).getTopicsUnderNamespace(any(), any(), any(), any(), any());
        doReturn(false).when(cnx).isSupportsTopicWatchers();

        PatternMultiTopicsConsumerImpl<byte[]> consumer =
                createPatternMultiTopicsConsumer(consumerConfData, topicsPattern);
        assertThat(consumer.subscribeFuture).succeedsWithin(Duration.ofSeconds(5));
        Awaitility.await().untilAsserted(() -> {
            assertThat(consumer.getPartitions()).containsExactlyInAnyOrder("persistent://tenant/namespace/topic1");
        });
        topics.add("persistent://tenant/namespace/topic2");
        Awaitility.await().untilAsserted(() -> {
            assertThat(consumer.getPartitions()).containsExactlyInAnyOrder("persistent://tenant/namespace/topic1",
                    "persistent://tenant/namespace/topic2");
        });
    }

    @Test
    public void testPatternSubscribeAndHashHandlingWithChanges() throws Exception {
        TopicsPattern topicsPattern =
                TopicsPatternFactory.create("persistent://tenant/namespace/.*", TopicsPattern.RegexImplementation.JDK);
        ConsumerConfigurationData<byte[]> consumerConfData = createConsumerConfigurationData();
        consumerConfData.setPatternAutoDiscoveryPeriod(5);
        Timer timer = mock(Timer.class);
        when(clientMock.timer()).thenReturn(timer);
        Deque<TimerTask> tasks = new ConcurrentLinkedDeque<>();
        doAnswer(invocationOnMock -> {
            TimerTask task = invocationOnMock.getArgument(0);
            tasks.add(task);
            return mock(Timeout.class);
        }).when(timer).newTimeout(any(), anyLong(), any());
        CopyOnWriteArrayList<String> topics = new CopyOnWriteArrayList<>();
        topics.add("persistent://tenant/namespace/topic1");
        consumerConfData.setTopicNames(new HashSet<>(topics));
        AtomicInteger invocationCount = new AtomicInteger(0);
        doAnswer(invocationOnMock -> {
            invocationCount.incrementAndGet();
            long requestId = invocationOnMock.getArgument(0);
            long watcherId = invocationOnMock.getArgument(1);
            String localHash = invocationOnMock.getArgument(4);
            CommandWatchTopicListSuccess success = new CommandWatchTopicListSuccess();
            success.setRequestId(requestId);
            success.setWatcherId(watcherId);
            List<String> topicsCopy = new ArrayList<>(topics);
            String calculatedHash = TopicList.calculateHash(topicsCopy);
            if (!localHash.equals(calculatedHash)) {
                throw new RuntimeException("Assuming no changes");
            }
            success.setTopicsHash(calculatedHash);
            return CompletableFuture.completedFuture(success);
        }).when(cnx).newWatchTopicList(anyLong(), anyLong(), any(), any(), any());
        doReturn(true).when(cnx).isSupportsTopicWatchers();
        doReturn(true).when(cnx).isSupportsTopicWatcherReconcile();

        PatternMultiTopicsConsumerImpl<byte[]> consumer =
                createPatternMultiTopicsConsumer(consumerConfData, topicsPattern);
        assertThat(consumer.subscribeFuture).succeedsWithin(Duration.ofSeconds(5));
        assertThat(consumer.getWatcherFuture()).succeedsWithin(Duration.ofSeconds(5));
        Awaitility.await().untilAsserted(() -> {
            assertThat(consumer.getPartitions()).containsExactlyInAnyOrder("persistent://tenant/namespace/topic1");
        });
        runTimerTasks(tasks);
        topics.add("persistent://tenant/namespace/topic2");
        CommandWatchTopicUpdate update = new CommandWatchTopicUpdate();
        TopicListWatcher topicListWatcher = consumer.getTopicListWatcher();
        update.setWatcherId(topicListWatcher.getWatcherId());
        update.addNewTopic("persistent://tenant/namespace/topic2");
        update.setTopicsHash(TopicList.calculateHash(topics));
        topicListWatcher.handleCommandWatchTopicUpdate(update);
        Awaitility.await().untilAsserted(() -> {
            assertThat(consumer.getPartitions()).containsExactlyInAnyOrder("persistent://tenant/namespace/topic1",
                    "persistent://tenant/namespace/topic2");
        });
        runTimerTasks(tasks);
        runTimerTasks(tasks);
        assertThat(invocationCount.get()).isEqualTo(4);
        CommandWatchTopicUpdate update2 = new CommandWatchTopicUpdate();
        update2.setWatcherId(topicListWatcher.getWatcherId());
        topics.add("persistent://tenant/namespace/topic3");
        update2.addNewTopic("persistent://tenant/namespace/topic3");
        topics.add("persistent://tenant/namespace/topic4");
        update2.addNewTopic("persistent://tenant/namespace/topic4");
        topics.remove("persistent://tenant/namespace/topic1");
        update2.addDeletedTopic("persistent://tenant/namespace/topic1");
        update2.setTopicsHash(TopicList.calculateHash(topics));
        topicListWatcher.handleCommandWatchTopicUpdate(update2);
        Awaitility.await().untilAsserted(() -> {
            assertThat(consumer.getPartitions()).containsExactlyInAnyOrder("persistent://tenant/namespace/topic2",
                    "persistent://tenant/namespace/topic3", "persistent://tenant/namespace/topic4");
        });
        assertThat(invocationCount.get()).isEqualTo(4);
        runTimerTasks(tasks);
        assertThat(invocationCount.get()).isEqualTo(5);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testOnTopicsRemovedCleansUnackedMessagesForRemovedPartitionedTopic() {
        String partitionedTopic = "persistent://tenant/namespace/deleted-topic";
        String partition0 = partitionedTopic + "-partition-0";
        String partition1 = partitionedTopic + "-partition-1";
        String otherTopicPartition = "persistent://tenant/namespace/other-topic-partition-0";
        TopicsPattern topicsPattern =
                TopicsPatternFactory.create("persistent://tenant/namespace/.*", TopicsPattern.RegexImplementation.JDK);
        ConsumerConfigurationData<byte[]> consumerConfData = createConsumerConfigurationData();
        consumerConfData.setAckTimeoutMillis(1000);

        PatternMultiTopicsConsumerImpl<byte[]> consumer =
                createPatternMultiTopicsConsumer(consumerConfData, topicsPattern);

        consumer.partitionedTopics.put(partitionedTopic, 2);

        ConsumerImpl<byte[]> partitionConsumer0 = (ConsumerImpl<byte[]>) mock(ConsumerImpl.class);
        ConsumerImpl<byte[]> partitionConsumer1 = (ConsumerImpl<byte[]>) mock(ConsumerImpl.class);
        when(partitionConsumer0.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));
        when(partitionConsumer1.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));
        consumer.consumers.put(partition0, partitionConsumer0);
        consumer.consumers.put(partition1, partitionConsumer1);

        TopicMessageIdImpl removedTopicMessageId = new TopicMessageIdImpl(partition0, new MessageIdImpl(1, 1, 0));
        TopicMessageIdImpl otherTopicMessageId =
                new TopicMessageIdImpl(otherTopicPartition, new MessageIdImpl(2, 2, 0));
        consumer.getUnAckedMessageTracker().add(removedTopicMessageId);
        consumer.getUnAckedMessageTracker().add(otherTopicMessageId);
        assertThat(consumer.getUnAckedMessageTracker().size()).isEqualTo(2);

        consumer.topicsChangeListener.onTopicsRemoved(Arrays.asList(partition0, partition1)).join();

        assertThat(consumer.partitionedTopics.containsKey(partitionedTopic)).isFalse();
        assertThat(consumer.consumers).doesNotContainKeys(partition0, partition1);
        assertThat(consumer.getUnAckedMessageTracker().size()).isEqualTo(1);
        assertThat(consumer.getUnAckedMessageTracker().remove(otherTopicMessageId)).isTrue();
        verify(partitionConsumer0).closeAsync();
        verify(partitionConsumer1).closeAsync();
    }

    @Test
    public void testFailedChildIsRemovedAndFullRecheckCreatesReadyReplacement() throws Exception {
        String topic = "persistent://tenant/namespace/t190008";
        TopicsPattern pattern = TopicsPatternFactory.create("persistent://tenant/namespace/t.*",
                TopicsPattern.RegexImplementation.JDK);
        ConsumerConfigurationData<byte[]> conf = createConsumerConfigurationData();
        conf.setAutoUpdatePartitions(false);
        PatternMultiTopicsConsumerImpl<byte[]> consumer = createPatternMultiTopicsConsumer(conf, pattern);
        CompletableFuture<ProducerResponse> firstSubscribe = new CompletableFuture<>();
        AtomicBoolean firstRequest = new AtomicBoolean(true);
        doAnswer(invocation -> {
            invocation.<ByteBuf>getArgument(0).release();
            return firstRequest.compareAndSet(true, false)
                    ? firstSubscribe : CompletableFuture.completedFuture(mock(ProducerResponse.class));
        }).when(cnx).sendRequestWithId(any(), anyLong());
        clientMock.getConfiguration().setOperationTimeoutMs(0);
        try {
            CompletableFuture<Void> adding = consumer.topicsChangeListener.onTopicsAdded(Collections.singleton(topic));
            Awaitility.await().untilAsserted(() -> assertThat(consumer.getConsumers()).hasSize(1));
            ConsumerImpl<byte[]> failedChild = consumer.getConsumers().get(0);

            firstSubscribe.completeExceptionally(
                    new PulsarClientException.TimeoutException("Initial subscribe timed out"));

            assertThat(adding).failsWithin(Duration.ofSeconds(5));
            assertThat(failedChild.subscribeFuture()).isCompletedExceptionally();
            assertThat(consumer.getPartitions()).as("a failed child must not mask a missing subscription").isEmpty();
            assertThat(consumer.allTopicPartitionsNumber.get()).isZero();

            clientMock.getConfiguration().setOperationTimeoutMs(30000);
            List<String> fullSnapshot = Collections.singletonList(topic);
            LookupService lookup = clientMock.getLookup();
            doReturn(CompletableFuture.completedFuture(new GetTopicsResult(fullSnapshot,
                    TopicList.calculateHash(fullSnapshot), true, true)))
                    .when(lookup).getTopicsUnderNamespace(any(), any(), any(), any(), any());
            consumer.recheckTopicsChange().get(5, TimeUnit.SECONDS);

            assertThat(consumer.getPartitions()).containsExactly(topic);
            assertThat(consumer.getConsumers()).hasSize(1);
            ConsumerImpl<byte[]> replacement = consumer.getConsumers().get(0);
            assertThat(replacement).isNotSameAs(failedChild);
            assertThat(replacement.getState()).isEqualTo(HandlerState.State.Ready);
            assertThat(replacement.isConnected()).isTrue();
            assertThat(consumer.allTopicPartitionsNumber.get()).isEqualTo(1);
        } finally {
            firstSubscribe.complete(mock(ProducerResponse.class));
            consumer.closeAsync().get(5, TimeUnit.SECONDS);
        }
    }

    @SuppressWarnings("unchecked")
    private ConsumerImpl<byte[]> childForFailedAttemptTest(String topic, boolean initialFailure,
                                                          HandlerState.State state) {
        ConsumerImpl<byte[]> child = mock(ConsumerImpl.class, Mockito.RETURNS_DEEP_STUBS);
        doReturn(topic).when(child).getTopic();
        doReturn(state).when(child).getState();
        CompletableFuture<Consumer<byte[]>> initial = new CompletableFuture<>();
        if (initialFailure) {
            initial.completeExceptionally(new PulsarClientException.TimeoutException("Initial subscribe failed"));
        } else {
            initial.complete(child);
        }
        doReturn(initial).when(child).subscribeFuture();
        doReturn(CompletableFuture.completedFuture(null)).when(child).closeAsync();
        return child;
    }

    @Test
    public void testFailedPartitionExpansionPreservesHealthyAndReconnectingChildren() throws Exception {
        String base = "persistent://tenant/namespace/partitioned";
        String partition0 = base + "-partition-0";
        String partition1 = base + "-partition-1";
        String partition2 = base + "-partition-2";
        TopicsPattern pattern = TopicsPatternFactory.create("persistent://tenant/namespace/.*",
                TopicsPattern.RegexImplementation.JDK);
        ConsumerConfigurationData<byte[]> conf = createConsumerConfigurationData();
        conf.setAutoUpdatePartitions(false);
        PatternMultiTopicsConsumerImpl<byte[]> consumer = createPatternMultiTopicsConsumer(conf, pattern);
        ConsumerImpl<byte[]> healthy = childForFailedAttemptTest(partition0, false, HandlerState.State.Ready);
        ConsumerImpl<byte[]> reconnecting = childForFailedAttemptTest(partition1, false, HandlerState.State.Connecting);
        // The exceptional initial future, not a later state assignment, identifies the failed attempt.
        ConsumerImpl<byte[]> failed = childForFailedAttemptTest(partition2, true, HandlerState.State.Connecting);
        CompletableFuture<Void> closingFailed = new CompletableFuture<>();
        doReturn(closingFailed).when(failed).closeAsync();
        consumer.partitionedTopics.put(base, 3);
        consumer.consumers.put(partition0, healthy);
        consumer.consumers.put(partition1, reconnecting);
        consumer.consumers.put(partition2, failed);
        consumer.allTopicPartitionsNumber.set(3);
        CompletableFuture<Void> failedUpdate = new CompletableFuture<>();
        try {
            consumer.handleSubscribeOneTopicError(partition2, new RuntimeException("Expansion failed"), failedUpdate);
            Awaitility.await().untilAsserted(() -> verify(failed).closeAsync());
            assertThat(failedUpdate).isNotDone();
            closingFailed.complete(null);
            assertThat(failedUpdate).failsWithin(Duration.ofSeconds(5));

            assertThat(consumer.getPartitions()).containsExactlyInAnyOrder(partition0, partition1);
            assertThat(consumer.partitionedTopics).containsEntry(base, 3);
            assertThat(consumer.allTopicPartitionsNumber.get()).isEqualTo(2);
            verify(healthy, never()).closeAsync();
            verify(reconnecting, never()).closeAsync();

            consumer.topicsChangeListener.onTopicsAdded(Collections.singleton(partition2)).get(5, TimeUnit.SECONDS);
            assertThat(consumer.getPartitions()).containsExactlyInAnyOrder(partition0, partition1, partition2);
            assertThat(consumer.consumers.get(partition0)).isSameAs(healthy);
            assertThat(consumer.consumers.get(partition1)).isSameAs(reconnecting);
            assertThat(consumer.consumers.get(partition2)).isNotSameAs(failed);
            assertThat(consumer.consumers.get(partition2).getState()).isEqualTo(HandlerState.State.Ready);
            assertThat(consumer.allTopicPartitionsNumber.get()).isEqualTo(3);
        } finally {
            closingFailed.complete(null);
            consumer.closeAsync().get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testFailedChildCleanupDoesNotRemoveHealthyReplacement() throws Exception {
        String topic = "persistent://tenant/namespace/child-replacement";
        TopicsPattern pattern = TopicsPatternFactory.create("persistent://tenant/namespace/.*",
                TopicsPattern.RegexImplementation.JDK);
        PatternMultiTopicsConsumerImpl<byte[]> consumer = createPatternMultiTopicsConsumer(pattern);
        ConsumerImpl<byte[]> failed = childForFailedAttemptTest(topic, true, HandlerState.State.Connecting);
        ConsumerImpl<byte[]> replacement = childForFailedAttemptTest(topic, false, HandlerState.State.Ready);
        CompletableFuture<Void> closingFailed = new CompletableFuture<>();
        doReturn(closingFailed).when(failed).closeAsync();
        consumer.consumers.put(topic, failed);
        consumer.allTopicPartitionsNumber.set(1);
        CompletableFuture<Void> result = new CompletableFuture<>();
        try {
            consumer.handleSubscribeOneTopicError(topic, new RuntimeException("Subscribe failed"), result);
            Awaitility.await().untilAsserted(() -> verify(failed).closeAsync());
            consumer.consumers.put(topic, replacement);
            closingFailed.complete(null);
            assertThat(result).failsWithin(Duration.ofSeconds(5));
            assertThat(consumer.consumers.get(topic)).isSameAs(replacement);
            assertThat(consumer.allTopicPartitionsNumber.get()).isEqualTo(1);
            verify(replacement, never()).closeAsync();
        } finally {
            closingFailed.complete(null);
            consumer.closeAsync().get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testLateFailedChildCallbackAfterCloseDoesNotScheduleCleanup() throws Exception {
        TopicsPattern pattern = TopicsPatternFactory.create("persistent://tenant/namespace/.*",
                TopicsPattern.RegexImplementation.JDK);
        PatternMultiTopicsConsumerImpl<byte[]> consumer = createPatternMultiTopicsConsumer(pattern);
        consumer.closeAsync().get(5, TimeUnit.SECONDS);
        clearInvocations(clientMock);
        CompletableFuture<Void> result = new CompletableFuture<>();
        consumer.handleSubscribeOneTopicError("persistent://tenant/namespace/t190008",
                new RuntimeException("Late failed subscribe"), result);
        assertThat(result).isCompletedExceptionally();
        verify(clientMock, never()).externalExecutorProvider();
        assertThat(consumer.getConsumers()).isEmpty();
        assertThat(consumer.allTopicPartitionsNumber.get()).isZero();
    }

    @Test
    public void testAllFailedPartitionsRemoveEmptyBaseEntry() throws Exception {
        String base = "persistent://tenant/namespace/all-failed";
        TopicsPattern pattern = TopicsPatternFactory.create("persistent://tenant/namespace/.*",
                TopicsPattern.RegexImplementation.JDK);
        ConsumerConfigurationData<byte[]> conf = createConsumerConfigurationData();
        conf.setAutoUpdatePartitions(false);
        PatternMultiTopicsConsumerImpl<byte[]> consumer = createPatternMultiTopicsConsumer(conf, pattern);
        consumer.partitionedTopics.put(base, 2);
        consumer.allTopicPartitionsNumber.set(2);
        for (int index = 0; index < 2; index++) {
            String partition = base + "-partition-" + index;
            consumer.consumers.put(partition,
                    childForFailedAttemptTest(partition, true, HandlerState.State.Connecting));
        }
        try {
            CompletableFuture<Void> result = new CompletableFuture<>();
            consumer.handleSubscribeOneTopicError(base, new RuntimeException("Initial partitions failed"), result);
            assertThat(result).failsWithin(Duration.ofSeconds(5));
            assertThat(consumer.getConsumers()).isEmpty();
            assertThat(consumer.partitionedTopics).doesNotContainKey(base);
            assertThat(consumer.allTopicPartitionsNumber.get()).isZero();
        } finally {
            consumer.closeAsync().get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCloseCannotPublishRecheckTimeoutAfterCancellingIt() throws Exception {
        timer.stop();
        timer = mock(Timer.class);
        when(clientMock.timer()).thenReturn(timer);
        CountDownLatch scheduling = new CountDownLatch(1);
        CountDownLatch publishTimeout = new CountDownLatch(1);
        AtomicReference<PatternMultiTopicsConsumerImpl<byte[]>> parent = new AtomicReference<>();
        Timeout timeout = mock(Timeout.class);
        doAnswer(invocation -> {
            TimerTask task = invocation.getArgument(0);
            if (task instanceof PatternMultiTopicsConsumerImpl) {
                parent.set((PatternMultiTopicsConsumerImpl<byte[]>) task);
                scheduling.countDown();
                assertThat(publishTimeout.await(5, TimeUnit.SECONDS)).isTrue();
                return timeout;
            }
            return mock(Timeout.class);
        }).when(timer).newTimeout(any(), anyLong(), any());
        ConsumerConfigurationData<byte[]> conf = createConsumerConfigurationData();
        conf.setAutoUpdatePartitions(false);
        TopicsPattern pattern = TopicsPatternFactory.create("persistent://tenant/namespace/.*",
                TopicsPattern.RegexImplementation.JDK);
        ExecutorService creator = Executors.newSingleThreadExecutor(new DefaultThreadFactory("pattern-close-creation"));
        CompletableFuture<PatternMultiTopicsConsumerImpl<byte[]>> created = CompletableFuture.supplyAsync(
                () -> createPatternMultiTopicsConsumer(conf, pattern), creator);
        CompletableFuture<Void> closed = new CompletableFuture<>();
        Thread closer = new Thread(() -> {
            try {
                parent.get().closeAsync().get(5, TimeUnit.SECONDS);
                closed.complete(null);
            } catch (Throwable error) {
                closed.completeExceptionally(error);
            }
        }, "pattern-close-during-schedule");
        try {
            assertThat(scheduling.await(5, TimeUnit.SECONDS)).isTrue();
            closer.start();
            // Old code finishes close while newTimeout is blocked. Correct code waits for the scheduler monitor.
            Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> {
                ThreadInfo info = ManagementFactory.getThreadMXBean().getThreadInfo(closer.getId());
                return closed.isDone() || info != null && info.getThreadState() == Thread.State.BLOCKED
                        && info.getLockInfo() != null
                        && info.getLockInfo().getIdentityHashCode() == System.identityHashCode(parent.get());
            });
            publishTimeout.countDown();
            PatternMultiTopicsConsumerImpl<byte[]> consumer = created.get(5, TimeUnit.SECONDS);
            closed.get(5, TimeUnit.SECONDS);
            assertThat(consumer.getRecheckPatternTimeout()).isNull();
            verify(timeout).cancel();
        } finally {
            publishTimeout.countDown();
            closer.join(1000);
            creator.shutdownNow();
        }
    }

    @Test
    public void testFailedChildCleanupCompletesWhenExecutorRejectsTask() throws Exception {
        String topic = "persistent://tenant/namespace/rejected-cleanup";
        TopicsPattern pattern = TopicsPatternFactory.create("persistent://tenant/namespace/.*",
                TopicsPattern.RegexImplementation.JDK);
        PatternMultiTopicsConsumerImpl<byte[]> consumer = createPatternMultiTopicsConsumer(pattern);
        ConsumerImpl<byte[]> failed = childForFailedAttemptTest(topic, true, HandlerState.State.Connecting);
        consumer.consumers.put(topic, failed);
        consumer.allTopicPartitionsNumber.set(1);
        ExecutorService rejectingExecutor = mock(ExecutorService.class);
        ExecutorProvider rejectingProvider = mock(ExecutorProvider.class);
        doReturn(rejectingExecutor).when(rejectingProvider).getExecutor();
        doThrow(new RejectedExecutionException("Client is shutting down"))
                .when(rejectingExecutor).execute(any(Runnable.class));
        doReturn(rejectingProvider).when(clientMock).externalExecutorProvider();
        try {
            CompletableFuture<Void> result = new CompletableFuture<>();
            RuntimeException subscribeError = new RuntimeException("Initial subscribe failed");
            consumer.handleSubscribeOneTopicError(topic, subscribeError, result);
            assertThat(result).isCompletedExceptionally();
            verify(rejectingExecutor).execute(any(Runnable.class));
            verify(failed, never()).closeAsync();
        } finally {
            doReturn(executorProvider).when(clientMock).externalExecutorProvider();
            consumer.closeAsync().get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testQueuedFailedChildCleanupRunsWhenExecutorShutsDown() throws Exception {
        String topic = "persistent://tenant/namespace/queued-cleanup";
        TopicsPattern pattern = TopicsPatternFactory.create("persistent://tenant/namespace/.*",
                TopicsPattern.RegexImplementation.JDK);
        PatternMultiTopicsConsumerImpl<byte[]> consumer = createPatternMultiTopicsConsumer(pattern);
        ConsumerImpl<byte[]> failed = childForFailedAttemptTest(topic, true, HandlerState.State.Connecting);
        consumer.consumers.put(topic, failed);
        consumer.allTopicPartitionsNumber.set(1);
        CountDownLatch blocked = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        executorProvider.getExecutor().execute(() -> {
            blocked.countDown();
            try {
                release.await();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
        });
        try {
            assertThat(blocked.await(5, TimeUnit.SECONDS)).isTrue();
            CompletableFuture<Void> result = new CompletableFuture<>();
            consumer.handleSubscribeOneTopicError(topic, new RuntimeException("Initial subscribe failed"), result);
            assertThat(result).isNotDone();
            executorProvider.shutdownNow();
            assertThat(result).failsWithin(Duration.ofSeconds(5));
            assertThat(consumer.getConsumers()).isEmpty();
            assertThat(consumer.allTopicPartitionsNumber.get()).isZero();
            verify(failed).closeAsync();
        } finally {
            release.countDown();
            consumer.closeAsync().get(5, TimeUnit.SECONDS);
        }
    }

    private static void runTimerTasks(Deque<TimerTask> tasks) throws Exception {
        // first drain the queue to a list to avoid an infinite loop
        List<TimerTask> taskList = new ArrayList<>();
        while (!tasks.isEmpty()) {
            taskList.add(tasks.poll());
        }
        // now run the tasks
        for (TimerTask task : taskList) {
            task.run(mock(Timeout.class));
        }
    }

    private PatternMultiTopicsConsumerImpl<byte[]> createPatternMultiTopicsConsumer(TopicsPattern topicsPattern) {
        ConsumerConfigurationData<byte[]> consumerConfData = createConsumerConfigurationData();
        return createPatternMultiTopicsConsumer(consumerConfData, topicsPattern);
    }

    private static @NonNull ConsumerConfigurationData<byte[]> createConsumerConfigurationData() {
        ConsumerConfigurationData<byte[]> consumerConfData = new ConsumerConfigurationData<>();
        consumerConfData.setSubscriptionName("subscriptionName");
        consumerConfData.setAutoUpdatePartitionsIntervalSeconds(0);
        return consumerConfData;
    }

    private PatternMultiTopicsConsumerImpl<byte[]> createPatternMultiTopicsConsumer(
            ConsumerConfigurationData<byte[]> consumerConfData, TopicsPattern topicsPattern) {
        int completionDelayMillis = 100;
        Schema<byte[]> schema = Schema.BYTES;
        when(clientMock.getPartitionedTopicMetadata(any(), anyBoolean(), anyBoolean()))
                .thenAnswer(invocation -> createDelayedCompletedFuture(
                new PartitionedTopicMetadata(), completionDelayMillis));
        PatternMultiTopicsConsumerImpl<byte[]> consumer = new PatternMultiTopicsConsumerImpl<byte[]>(
                topicsPattern, clientMock, consumerConfData, executorProvider,
                new CompletableFuture<>(), schema, CommandGetTopicsOfNamespace.Mode.PERSISTENT, null);
        return consumer;
    }
}
