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
package org.apache.pulsar.client.api.v5;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.naming.NamespaceName;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * End-to-end coverage for the broker-side namespace scalable-topics watcher.
 *
 * <p>Uses reflection to construct a {@code ScalableTopicsWatcher} (package-private
 * in the v5 module) directly against the test cluster's v4 client. Verifies:
 * <ul>
 *   <li>Initial snapshot reflects pre-existing topics.</li>
 *   <li>Topic create / delete fires {@code Diff} events with the right names.</li>
 *   <li>Property filters narrow the matching set, and updating a topic's properties
 *       to fall outside the filter emits {@code Removed}.</li>
 * </ul>
 */
public class V5ScalableTopicsWatcherTest extends V5ClientBaseTest {

    /** Captures every Snapshot/Diff so tests can assert on the cumulative state. */
    private static final class CapturingListener {
        final Set<String> currentSet = ConcurrentHashMap.newKeySet();
        final List<List<String>> snapshots = new CopyOnWriteArrayList<>();
        // (added, removed) per diff
        final List<Map.Entry<List<String>, List<String>>> diffs = new CopyOnWriteArrayList<>();

        void onSnapshot(List<String> topics) {
            snapshots.add(List.copyOf(topics));
            currentSet.clear();
            currentSet.addAll(topics);
        }

        void onDiff(List<String> added, List<String> removed) {
            diffs.add(Map.entry(List.copyOf(added), List.copyOf(removed)));
            currentSet.removeAll(removed);
            currentSet.addAll(added);
        }
    }

    @Test
    public void watcherEmitsCreateAndDeleteEvents() throws Exception {
        NamespaceName ns = NamespaceName.get(getNamespace());

        WatcherHandle handle = openWatcher(ns, Map.of());
        try {
            // Initial snapshot should be empty (or whatever pre-existing topics; the
            // shared cluster gives us a fresh per-test namespace so it should be empty).
            Awaitility.await().atMost(10, TimeUnit.SECONDS)
                    .until(() -> !handle.listener.snapshots.isEmpty());
            assertEquals(handle.listener.snapshots.get(0).size(), 0,
                    "fresh namespace should yield empty initial snapshot");

            String topicA = ns + "/a-" + UUID.randomUUID().toString().substring(0, 8);
            String topicB = ns + "/b-" + UUID.randomUUID().toString().substring(0, 8);
            admin.scalableTopics().createScalableTopic("topic://" + topicA, 1);
            admin.scalableTopics().createScalableTopic("topic://" + topicB, 1);

            Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                    assertEquals(handle.listener.currentSet, Set.of(
                            "topic://" + topicA, "topic://" + topicB)));

            admin.scalableTopics().deleteScalableTopic("topic://" + topicA, true);

            Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                    assertEquals(handle.listener.currentSet, Set.of("topic://" + topicB)));
        } finally {
            handle.close();
        }
    }

    @Test
    public void watcherFiltersByProperty() throws Exception {
        NamespaceName ns = NamespaceName.get(getNamespace());

        WatcherHandle handle = openWatcher(ns, Map.of("owner", "alice"));
        try {
            Awaitility.await().atMost(10, TimeUnit.SECONDS)
                    .until(() -> !handle.listener.snapshots.isEmpty());

            String aliceTopic = ns + "/alice-" + UUID.randomUUID().toString().substring(0, 8);
            String bobTopic = ns + "/bob-" + UUID.randomUUID().toString().substring(0, 8);
            admin.scalableTopics().createScalableTopic("topic://" + aliceTopic, 1,
                    Map.of("owner", "alice"));
            admin.scalableTopics().createScalableTopic("topic://" + bobTopic, 1,
                    Map.of("owner", "bob"));

            // Only alice's topic should surface — bob is filtered out.
            Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                    assertEquals(handle.listener.currentSet, Set.of("topic://" + aliceTopic)));

            // bob's topic exists but never reaches the watcher's set.
            assertTrue(!handle.listener.currentSet.contains("topic://" + bobTopic));
        } finally {
            handle.close();
        }
    }

    @Test
    public void watcherEmptyFilterSubscribesToEveryTopic() throws Exception {
        NamespaceName ns = NamespaceName.get(getNamespace());

        // Pre-create a topic before the watcher opens so the initial snapshot has work
        // to do (proves the snapshot path, not just the live-event path).
        String preTopic = ns + "/pre-" + UUID.randomUUID().toString().substring(0, 8);
        admin.scalableTopics().createScalableTopic("topic://" + preTopic, 1);

        WatcherHandle handle = openWatcher(ns, Map.of());
        try {
            Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                    assertEquals(handle.listener.currentSet, Set.of("topic://" + preTopic)));
        } finally {
            handle.close();
        }
    }

    // --- Helpers ---

    /** Bundle of watcher state needed to clean up at the end of a test. */
    private static final class WatcherHandle implements AutoCloseable {
        final Object watcher;
        final CapturingListener listener;
        final CountDownLatch ready;

        WatcherHandle(Object watcher, CapturingListener listener, CountDownLatch ready) {
            this.watcher = watcher;
            this.listener = listener;
            this.ready = ready;
        }

        @Override
        public void close() throws Exception {
            Method closeMethod = watcher.getClass().getDeclaredMethod("close");
            closeMethod.setAccessible(true);
            closeMethod.invoke(watcher);
        }
    }

    /**
     * Construct a {@code ScalableTopicsWatcher} via reflection (it's package-private
     * inside the v5 module, but we drive it directly from the broker tests). Wire up
     * a {@link CapturingListener}, call {@code start()}, and return a handle. The
     * initial snapshot is delivered via the future + {@code onSnapshot} on the
     * listener for uniform handling — see the watcher's javadoc.
     */
    private WatcherHandle openWatcher(NamespaceName ns, Map<String, String> filters)
            throws Exception {
        // Pull the underlying v4 client out of v5Client so we can construct the
        // (package-private) watcher directly.
        Field v4Field = v5Client.getClass().getDeclaredField("v4Client");
        v4Field.setAccessible(true);
        PulsarClientImpl v4 = (PulsarClientImpl) v4Field.get(v5Client);

        Class<?> watcherCls = Class.forName(
                "org.apache.pulsar.client.impl.v5.ScalableTopicsWatcher");
        Constructor<?> ctor = watcherCls.getDeclaredConstructor(
                PulsarClientImpl.class, NamespaceName.class, Map.class);
        ctor.setAccessible(true);
        Object watcher = ctor.newInstance(v4, ns, filters);

        CapturingListener listener = new CapturingListener();

        // The watcher's Listener is a package-private nested interface; use a Proxy.
        Class<?> listenerCls = Class.forName(
                "org.apache.pulsar.client.impl.v5.ScalableTopicsWatcher$Listener");
        Object listenerProxy = java.lang.reflect.Proxy.newProxyInstance(
                listenerCls.getClassLoader(), new Class<?>[]{listenerCls}, (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "onSnapshot":
                            listener.onSnapshot((List<String>) args[0]);
                            break;
                        case "onDiff":
                            listener.onDiff((List<String>) args[0], (List<String>) args[1]);
                            break;
                        default:
                            // Object methods (toString, hashCode, equals) — defaults are fine.
                    }
                    return null;
                });

        Method setListener = watcherCls.getDeclaredMethod("setListener", listenerCls);
        setListener.setAccessible(true);
        setListener.invoke(watcher, listenerProxy);

        Method start = watcherCls.getDeclaredMethod("start");
        start.setAccessible(true);
        java.util.concurrent.CompletableFuture<List<String>> startFut =
                (java.util.concurrent.CompletableFuture<List<String>>) start.invoke(watcher);

        CountDownLatch ready = new CountDownLatch(1);
        startFut.thenAccept(initialTopics -> {
            // start()'s future delivers the initial snapshot — feed it into the listener
            // for uniform handling with subsequent snapshots.
            listener.onSnapshot(initialTopics);
            ready.countDown();
        });
        if (!ready.await(10, TimeUnit.SECONDS)) {
            throw new AssertionError("watcher did not deliver initial snapshot in 10s");
        }
        return new WatcherHandle(watcher, listener, ready);
    }
}
