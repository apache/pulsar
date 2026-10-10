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
package org.apache.pulsar.client.impl.v5;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.naming.NamespaceName;
import org.testng.annotations.Test;

public class ScalableTopicsWatcherTest {

    private static final String A = "topic://public/default/a";
    private static final String B = "topic://public/default/b";
    private static final String C = "topic://public/default/c";

    /** Records the listener calls, one line each. */
    private static class RecordingListener implements ScalableTopicsWatcher.Listener {
        final List<String> calls = new CopyOnWriteArrayList<>();

        @Override
        public void onSnapshot(List<String> topics) {
            calls.add("snapshot " + topics.stream().sorted().toList());
        }

        @Override
        public void onDiff(List<String> added, List<String> removed) {
            calls.add("added " + added + " removed " + removed);
        }
    }

    /** A started watcher that has received its initial snapshot, of topics A and B. */
    private static ScalableTopicsWatcher watcherWithSnapshot() {
        PulsarClientImpl v4Client = mock(PulsarClientImpl.class);
        // The broker's events are fed in by the tests: the connection never opens.
        when(v4Client.getAnyBrokerProxyConnection()).thenReturn(new CompletableFuture<>());
        ScalableTopicsWatcher watcher =
                new ScalableTopicsWatcher(v4Client, NamespaceName.get("public/default"), Map.of());
        CompletableFuture<List<String>> initial = watcher.start();
        watcher.onSnapshot(List.of(A, B));
        assertThat(initial).isCompletedWithValue(List.of(A, B));
        return watcher;
    }

    @Test
    public void testSetListenerDeliversChangesReceivedBeforeIt() {
        ScalableTopicsWatcher watcher = watcherWithSnapshot();
        // C is created and B deleted while the caller attaches to A and B.
        watcher.onDiff(List.of(C), List.of());
        watcher.onDiff(List.of(), List.of(B));
        RecordingListener listener = new RecordingListener();

        watcher.setListener(listener);

        assertThat(listener.calls).containsExactly("added [" + C + "] removed [" + B + "]");
    }

    @Test
    public void testSetListenerDeliversNothingWhenNothingChangedSinceStart() {
        ScalableTopicsWatcher watcher = watcherWithSnapshot();
        RecordingListener listener = new RecordingListener();

        watcher.setListener(listener);
        assertThat(listener.calls).isEmpty();

        watcher.onDiff(List.of(C), List.of());
        assertThat(listener.calls).containsExactly("added [" + C + "] removed []");
    }

    @Test
    public void testTopicAddedAndRemovedBeforeSetListenerIsNotDelivered() {
        ScalableTopicsWatcher watcher = watcherWithSnapshot();
        watcher.onDiff(List.of(C), List.of());
        watcher.onDiff(List.of(), List.of(C));
        RecordingListener listener = new RecordingListener();

        watcher.setListener(listener);

        assertThat(listener.calls).isEmpty();
    }

    @Test
    public void testTopicRemovedAndAddedBeforeSetListenerIsDeliveredAsBoth() {
        ScalableTopicsWatcher watcher = watcherWithSnapshot();
        // B is deleted and created again: the listener must drop what it attached to and attach again.
        watcher.onDiff(List.of(), List.of(B));
        watcher.onDiff(List.of(B), List.of());
        RecordingListener listener = new RecordingListener();

        watcher.setListener(listener);

        assertThat(listener.calls).containsExactly("added [" + B + "] removed [" + B + "]");
    }

    @Test
    public void testTopicOfSnapshotAddedAgainThenRemovedBeforeSetListenerIsDeliveredAsRemoved() {
        ScalableTopicsWatcher watcher = watcherWithSnapshot();
        // The broker can report a topic of the initial snapshot as added again; B is then deleted.
        watcher.onDiff(List.of(B), List.of());
        watcher.onDiff(List.of(), List.of(B));
        RecordingListener listener = new RecordingListener();

        watcher.setListener(listener);

        assertThat(listener.calls).containsExactly("added [] removed [" + B + "]");
    }

    @Test
    public void testSnapshotBeforeSetListenerIsDeliveredWhole() {
        ScalableTopicsWatcher watcher = watcherWithSnapshot();
        watcher.onDiff(List.of(C), List.of());
        // A reconnect resyncs the set, and a change follows.
        watcher.onSnapshot(List.of(A, C));
        watcher.onDiff(List.of(B), List.of(A));
        RecordingListener listener = new RecordingListener();

        watcher.setListener(listener);

        assertThat(listener.calls).containsExactly("snapshot [" + B + ", " + C + "]");
        assertThat(watcher.currentSetForTesting()).containsExactlyInAnyOrder(B, C);
    }

    @Test(timeOut = 30_000)
    public void testChangeArrivingWhileListenerRunsIsDeliveredAfterIt() throws Exception {
        ScalableTopicsWatcher watcher = watcherWithSnapshot();
        watcher.onDiff(List.of(C), List.of());
        CountDownLatch delivering = new CountDownLatch(1);
        CountDownLatch resume = new CountDownLatch(1);
        RecordingListener listener = new RecordingListener() {
            @Override
            public void onDiff(List<String> added, List<String> removed) {
                super.onDiff(added, removed);
                delivering.countDown();
                awaitUninterruptibly(resume);
            }
        };
        Thread caller = new Thread(() -> watcher.setListener(listener));
        caller.start();
        try {
            assertThat(delivering.await(10, TimeUnit.SECONDS)).isTrue();

            // B is deleted, on the I/O thread, while the caller's thread is still delivering C: the I/O
            // thread must neither wait for that delivery nor call the listener alongside it.
            watcher.onDiff(List.of(), List.of(B));
            assertThat(listener.calls).containsExactly("added [" + C + "] removed []");
        } finally {
            // Let the caller's thread finish even if an assertion failed.
            resume.countDown();
            caller.join();
        }
        assertThat(listener.calls).containsExactly(
                "added [" + C + "] removed []",
                "added [] removed [" + B + "]");
    }

    private static void awaitUninterruptibly(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
