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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import lombok.AllArgsConstructor;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

@Test(groups = "utils")
public class PatternConsumerUpdateQueueTest {

    private QueueInstance createInstance(CompletableFuture<Void> customizedRecheckFuture,
                                         CompletableFuture<Void> customizedPartialUpdateFuture) {
        PatternMultiTopicsConsumerImpl patternConsumer = mock(PatternMultiTopicsConsumerImpl.class);
        when(patternConsumer.recheckTopicsChange()).thenReturn(customizedRecheckFuture);

        PatternMultiTopicsConsumerImpl.TopicsChangedListener topicsChangeListener =
                mock(PatternMultiTopicsConsumerImpl.TopicsChangedListener.class);
        when(topicsChangeListener.onTopicsAdded(anyCollection())).thenReturn(customizedPartialUpdateFuture);
        when(topicsChangeListener.onTopicsRemoved(anyCollection())).thenReturn(customizedPartialUpdateFuture);

        PatternConsumerUpdateQueue queue = new PatternConsumerUpdateQueue(patternConsumer, topicsChangeListener);
        return new QueueInstance(queue, patternConsumer, topicsChangeListener);
    }

    private QueueInstance createInstance() {
        CompletableFuture<Void> completedFuture = CompletableFuture.completedFuture(null);
        return createInstance(completedFuture, completedFuture);
    }

    @AllArgsConstructor
    private static class QueueInstance {
        private PatternConsumerUpdateQueue queue;
        private PatternMultiTopicsConsumerImpl mockedConsumer;
        private PatternMultiTopicsConsumerImpl.TopicsChangedListener mockedListener;
    }

    @Test
    public void testTopicsChangedEvents() {
        QueueInstance instance = createInstance();

        Collection<String> topics = Arrays.asList("a");
        for (int i = 0; i < 10; i++) {
            instance.queue.appendTopicsAddedOp(topics);
            instance.queue.appendTopicsRemovedOp(topics);
        }
        Awaitility.await().untilAsserted(() -> {
            verify(instance.mockedListener, times(10)).onTopicsAdded(topics);
            verify(instance.mockedListener, times(10)).onTopicsRemoved(topics);
        });
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
    }

    @Test
    public void testDelayedRecheckTask() {
        CompletableFuture<Void> recheckFuture = new CompletableFuture<>();
        CompletableFuture<Void> partialUpdateFuture = CompletableFuture.completedFuture(null);
        QueueInstance instance = createInstance(recheckFuture, partialUpdateFuture);

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
    }

    @Test
    public void testCompositeTasks() {
        CompletableFuture<Void> recheckFuture = new CompletableFuture<>();
        CompletableFuture<Void> partialUpdateFuture = CompletableFuture.completedFuture(null);
        QueueInstance instance = createInstance(recheckFuture, partialUpdateFuture);

        Collection<String> topics = Arrays.asList("a");
        for (int i = 0; i < 10; i++) {
            instance.queue.appendRecheckOp();
            instance.queue.appendTopicsAddedOp(topics);
            instance.queue.appendTopicsRemovedOp(topics);
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
            verify(instance.mockedListener, times(0)).onTopicsAdded(topics);
            verify(instance.mockedListener, times(0)).onTopicsRemoved(topics);
        });
    }
}
