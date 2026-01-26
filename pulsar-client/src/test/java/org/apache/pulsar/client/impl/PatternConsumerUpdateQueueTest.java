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

import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.netty.util.HashedWheelTimer;
import java.io.Closeable;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.AllArgsConstructor;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.common.util.FutureUtil;
import org.awaitility.Awaitility;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

@Test(groups = "utils")
public class PatternConsumerUpdateQueueTest {

    private QueueInstance createInstance(CompletableFuture<Void> customizedRecheckFuture,
                                         CompletableFuture<Void> customizedPartialUpdateFuture,
                                         CompletableFuture<Void> customizedConsumerInitFuture) {
        return createInstance(customizedRecheckFuture, customizedPartialUpdateFuture, customizedConsumerInitFuture,
                null, null);
    }

    private QueueInstance createInstance(CompletableFuture<Void> customizedRecheckFuture,
                                         CompletableFuture<Void> customizedPartialUpdateFuture,
                                         CompletableFuture<Void> customizedConsumerInitFuture,
                                         Collection<String> successTopics,
                                         Collection<String> errorTopics) {
        HashedWheelTimer timer = new HashedWheelTimer(new ExecutorProvider.ExtendedThreadFactory("timer-x",
                Thread.currentThread().isDaemon()), 1, TimeUnit.MILLISECONDS);
        PulsarClientImpl client = mock(PulsarClientImpl.class);
        when(client.timer()).thenReturn(timer);

        PatternMultiTopicsConsumerImpl patternConsumer = mock(PatternMultiTopicsConsumerImpl.class);
        when(patternConsumer.recheckTopicsChange()).thenReturn(customizedRecheckFuture);
        when(patternConsumer.getClient()).thenReturn(client);
        if (customizedConsumerInitFuture != null) {
            when(patternConsumer.getSubscribeFuture()).thenReturn(customizedConsumerInitFuture);
        } else {
            when(patternConsumer.getSubscribeFuture()).thenReturn(CompletableFuture.completedFuture(null));
        }

        PatternMultiTopicsConsumerImpl.TopicsChangedListener topicsChangeListener =
                mock(PatternMultiTopicsConsumerImpl.TopicsChangedListener.class);
        if (successTopics == null && errorTopics == null) {
            when(topicsChangeListener.onTopicsAdded(anyCollection())).thenReturn(customizedPartialUpdateFuture);
            when(topicsChangeListener.onTopicsRemoved(anyCollection())).thenReturn(customizedPartialUpdateFuture);
        } else {
            CompletableFuture<Void> ex = FutureUtil.failedFuture(new RuntimeException("Failed topics changed event"));
            Answer answer = invocationOnMock -> {
                Collection inputCollection = invocationOnMock.getArgument(0, Collection.class);
                if (successTopics.containsAll(inputCollection)) {
                    return customizedPartialUpdateFuture;
                } else if (errorTopics.containsAll(inputCollection)) {
                    return ex;
                } else {
                    throw new RuntimeException("Unexpected topics changed event");
                }
            };
            doAnswer(answer).when(topicsChangeListener).onTopicsAdded(anyCollection());
            doAnswer(answer).when(topicsChangeListener).onTopicsRemoved(anyCollection());
        }

        PatternConsumerUpdateQueue queue = new PatternConsumerUpdateQueue(patternConsumer, topicsChangeListener);
        return new QueueInstance(queue, patternConsumer, topicsChangeListener);
    }

    private QueueInstance createInstance() {
        CompletableFuture<Void> completedFuture = CompletableFuture.completedFuture(null);
        return createInstance(completedFuture, completedFuture, completedFuture);
    }

    @AllArgsConstructor
    private static class QueueInstance implements Closeable {
        private PatternConsumerUpdateQueue queue;
        private PatternMultiTopicsConsumerImpl mockedConsumer;
        private PatternMultiTopicsConsumerImpl.TopicsChangedListener mockedListener;

        @Override
        public void close() {
            mockedConsumer.getClient().timer().stop();
        }
    }

    @Test
    public void testTopicsChangedEvents() {
        QueueInstance instance = createInstance();

        Collection<String> addedTopics = Arrays.asList("a");
        Collection<String> removedTopics = Arrays.asList("b");
        for (int i = 0; i < 10; i++) {
            instance.queue.appendTopicsChangedOp(addedTopics, removedTopics, "");
        }
        Awaitility.await().untilAsserted(() -> {
            verify(instance.mockedListener, times(10)).onTopicsAdded(addedTopics);
            verify(instance.mockedListener, times(10)).onTopicsRemoved(removedTopics);
        });

        // cleanup.
        instance.close();
    }

    @Test
    public void testRecheckTask() {
        QueueInstance instance = createInstance();

        for (int i = 0; i < 10; i++) {
            instance.queue.appendRecheckOp();
        }

        Awaitility.await().untilAsserted(() -> {
            verify(instance.mockedConsumer, times(10)).recheckTopicsChange();
        });

        // cleanup.
        instance.close();
    }

    @Test
    public void testDelayedRecheckTask() {
        CompletableFuture<Void> recheckFuture = new CompletableFuture<>();
        CompletableFuture<Void> partialUpdateFuture = CompletableFuture.completedFuture(null);
        CompletableFuture<Void> consumerInitFuture = CompletableFuture.completedFuture(null);
        QueueInstance instance = createInstance(recheckFuture, partialUpdateFuture, consumerInitFuture);

        for (int i = 0; i < 10; i++) {
            instance.queue.appendRecheckOp();
        }

        recheckFuture.complete(null);
        Awaitility.await().untilAsserted(() -> {
            // The first task will be running, and never completed until all tasks have been added.
            // Since the first was started, the second one will not be skipped.
            // The others after the second task will be skipped.
            // So the times that called "recheckTopicsChange" will be 2.
            verify(instance.mockedConsumer, times(2)).recheckTopicsChange();
        });

        // cleanup.
        instance.close();
    }

    @Test
    public void testCompositeTasks() {
        CompletableFuture<Void> recheckFuture = new CompletableFuture<>();
        CompletableFuture<Void> partialUpdateFuture = CompletableFuture.completedFuture(null);
        CompletableFuture<Void> consumerInitFuture = CompletableFuture.completedFuture(null);
        QueueInstance instance = createInstance(recheckFuture, partialUpdateFuture, consumerInitFuture);

        Collection<String> addedTopics = Arrays.asList("a");
        Collection<String> removedTopics = Arrays.asList("b");
        for (int i = 0; i < 10; i++) {
            instance.queue.appendRecheckOp();
            instance.queue.appendTopicsChangedOp(addedTopics, removedTopics, "");
        }
        recheckFuture.complete(null);
        Awaitility.await().untilAsserted(() -> {
            // The first task will be running, and never completed until all tasks have been added.
            // Since the first was started, the second one will not be skipped.
            // The others after the second task will be skipped.
            // So the times that called "recheckTopicsChange" will be 2.
            verify(instance.mockedConsumer, times(2)).recheckTopicsChange();
            // The tasks after the second "recheckTopicsChange" will be skipped due to there is a previous
            //   "recheckTopicsChange" that has not been executed.
            // The tasks between the fist "recheckTopicsChange" and the second "recheckTopicsChange" will be skipped
            //   due to there is a following "recheckTopicsChange".
            verify(instance.mockedListener, times(0)).onTopicsAdded(addedTopics);
            verify(instance.mockedListener, times(0)).onTopicsRemoved(removedTopics);
        });

        // cleanup.
        instance.close();
    }

    @Test
    public void testErrorTask() {
        CompletableFuture<Void> immediatelyCompleteFuture = CompletableFuture.completedFuture(null);
        Collection<String> successTopics = Arrays.asList("a", "b");
        Collection<String> errorTopics = Arrays.asList(UUID.randomUUID().toString());
        QueueInstance instance = createInstance(immediatelyCompleteFuture, immediatelyCompleteFuture,
                immediatelyCompleteFuture, successTopics, errorTopics);

        Collection<String> addedTopics = Arrays.asList("a");
        Collection<String> removedTopics = Arrays.asList("b");

        instance.queue.appendTopicsChangedOp(addedTopics, removedTopics, "");
        instance.queue.appendTopicsChangedOp(errorTopics, List.of(), "");
        instance.queue.appendTopicsChangedOp(addedTopics, removedTopics, "");

        Awaitility.await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
            verify(instance.mockedListener, times(2)).onTopicsAdded(addedTopics);
            verify(instance.mockedListener, times(2)).onTopicsRemoved(removedTopics);
            verify(instance.mockedListener, times(1)).onTopicsAdded(errorTopics);
            // After an error task will push a recheck task to offset.
            verify(instance.mockedConsumer, times(1)).recheckTopicsChange();
        });

        // cleanup.
        instance.close();
    }

    @Test
    public void testFailedSubscribe() {
        CompletableFuture<Void> immediatelyCompleteFuture = CompletableFuture.completedFuture(null);
        CompletableFuture<Void> consumerInitFuture = new CompletableFuture<>();
        Collection<String> successTopics = Arrays.asList("a");
        Collection<String> errorTopics = Arrays.asList(UUID.randomUUID().toString());
        QueueInstance instance = createInstance(immediatelyCompleteFuture, immediatelyCompleteFuture,
                consumerInitFuture, successTopics, errorTopics);

        Collection<String> addedTopics = Arrays.asList("a");
        Collection<String> removedTopics = Arrays.asList("b");

        instance.queue.appendTopicsChangedOp(addedTopics, removedTopics, "");
        instance.queue.appendTopicsChangedOp(errorTopics, List.of(), "");
        instance.queue.appendTopicsChangedOp(addedTopics, removedTopics, "");

        // Consumer init failed after multi topics changes.
        // All the topics changes events should be skipped.
        consumerInitFuture.completeExceptionally(new RuntimeException("mocked ex"));
        Awaitility.await().untilAsserted(() -> {
            verify(instance.mockedListener, times(0)).onTopicsAdded(addedTopics);
            verify(instance.mockedListener, times(0)).onTopicsRemoved(removedTopics);
            verify(instance.mockedListener, times(0)).onTopicsAdded(errorTopics);
            verify(instance.mockedConsumer, times(0)).recheckTopicsChange();
        });

        // cleanup.
        instance.close();
    }
}
