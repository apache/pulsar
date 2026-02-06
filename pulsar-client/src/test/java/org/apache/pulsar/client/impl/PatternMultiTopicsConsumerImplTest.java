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
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import com.google.common.collect.Sets;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
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
        }).when(mockLookup).getTopicsUnderNamespace(any(), any(), any(), any());
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
