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
package org.apache.pulsar.broker.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TopicPolicies;
import org.apache.pulsar.metadata.api.MetadataCache;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.NotificationType;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class MetadataStoreTopicPoliciesServiceTest {
    private static final TopicName TOPIC = TopicName.get("persistent://public/default/policy-refresh-test");

    private final AtomicReference<Consumer<Notification>> localNotification = new AtomicReference<>();
    private final AtomicReference<Consumer<Notification>> globalNotification = new AtomicReference<>();
    private OrderedExecutor notificationExecutor;
    private BrokerService brokerService;
    private MetadataCache<TopicPolicies> localCache;
    private MetadataCache<TopicPolicies> globalCache;
    private MetadataStoreTopicPoliciesService service;

    @BeforeMethod
    @SuppressWarnings("unchecked")
    public void setup() {
        notificationExecutor = OrderedExecutor.newBuilder().numThreads(2).name("topic-policy-notifications").build();
        PulsarService pulsar = mock(PulsarService.class);
        brokerService = mock(BrokerService.class);
        MetadataStoreExtended localStore = mock(MetadataStoreExtended.class);
        MetadataStoreExtended globalStore = mock(MetadataStoreExtended.class);
        localCache = mock(MetadataCache.class);
        globalCache = mock(MetadataCache.class);
        when(pulsar.getBrokerService()).thenReturn(brokerService);
        when(brokerService.getTopicPoliciesNotifyThread(any())).thenAnswer(invocation ->
                notificationExecutor.chooseThread(invocation.<TopicName>getArgument(0)));
        when(pulsar.getLocalMetadataStore()).thenReturn(localStore);
        when(pulsar.getConfigurationMetadataStore()).thenReturn(globalStore);
        when(localStore.getMetadataCache(TopicPolicies.class)).thenReturn(localCache);
        when(globalStore.getMetadataCache(TopicPolicies.class)).thenReturn(globalCache);
        doAnswer(invocation -> {
            localNotification.set(invocation.getArgument(0));
            return null;
        }).when(localStore).registerListener(any());
        doAnswer(invocation -> {
            globalNotification.set(invocation.getArgument(0));
            return null;
        }).when(globalStore).registerListener(any());
        service = new MetadataStoreTopicPoliciesService();
        service.start(pulsar);
    }

    @AfterMethod(alwaysRun = true)
    public void cleanup() throws Exception {
        service.close();
        notificationExecutor.shutdownNow();
        await().atMost(5, TimeUnit.SECONDS).until(notificationExecutor::isTerminated);
    }

    @DataProvider(name = "refreshCoalescing")
    public Object[][] refreshCoalescing() {
        return new Object[][]{{false}, {true}};
    }

    @Test(dataProvider = "refreshCoalescing")
    public void testNewNotificationsSupersedeAnOlderRead(boolean completionQueued) throws Exception {
        String path = MetadataStoreTopicPoliciesService.pathFor(TOPIC, false);
        TopicPolicies stale = new TopicPolicies();
        stale.setMaxProducerPerTopic(3);
        TopicPolicies updated = new TopicPolicies();
        updated.setMaxProducerPerTopic(7);
        CompletableFuture<Optional<TopicPolicies>> firstRead = new CompletableFuture<>();
        CountDownLatch firstReadStarted = new CountDownLatch(1);
        AtomicInteger reads = new AtomicInteger();
        when(localCache.get(path)).thenAnswer(invocation -> {
            if (reads.incrementAndGet() == 1) {
                firstReadStarted.countDown();
                return firstRead;
            }
            return CompletableFuture.completedFuture(Optional.of(updated));
        });
        Queue<TopicPolicies> received = new ConcurrentLinkedQueue<>();
        CountDownLatch updatedReceived = new CountDownLatch(1);
        service.registerListener(TOPIC, policies -> {
            received.add(policies);
            updatedReceived.countDown();
        });

        ExecutorService topicExecutor = notificationExecutor.chooseThread(TOPIC);
        CountDownLatch topicThreadBlocked = new CountDownLatch(1);
        CountDownLatch releaseTopicThread = new CountDownLatch(1);
        topicExecutor.execute(() -> {
            topicThreadBlocked.countDown();
            awaitLatch(releaseTopicThread);
        });
        ExecutorService countingExecutor = mock(ExecutorService.class);
        AtomicInteger dispatches = new AtomicInteger();
        doAnswer(invocation -> {
            topicExecutor.execute(invocation.getArgument(0));
            dispatches.incrementAndGet();
            return null;
        }).when(countingExecutor).execute(any());
        when(brokerService.getTopicPoliciesNotifyThread(TOPIC)).thenReturn(countingExecutor);

        try {
            assertThat(topicThreadBlocked.await(5, TimeUnit.SECONDS)).isTrue();
            localNotification.get().accept(new Notification(NotificationType.Modified, path));
            assertTrue(firstReadStarted.await(5, TimeUnit.SECONDS));
            if (completionQueued) {
                firstRead.complete(Optional.of(stale));
            }
            for (int i = 0; i < 1000; i++) {
                localNotification.get().accept(new Notification(NotificationType.Modified, path));
            }
            assertThat(reads).as("updates share the in-flight refresh").hasValue(1);
            assertThat(dispatches).as("updates do not add queued tasks").hasValue(completionQueued ? 1 : 0);
            firstRead.complete(Optional.of(stale));
            releaseTopicThread.countDown();

            assertTrue(updatedReceived.await(5, TimeUnit.SECONDS));
            topicExecutor.submit(() -> { }).get(5, TimeUnit.SECONDS);
            assertEquals(reads.get(), 2);
            assertEquals(received.size(), 1);
            assertEquals(received.peek().getMaxProducerPerTopic(), Integer.valueOf(7));
        } finally {
            releaseTopicThread.countDown();
        }
    }

    @Test
    public void testListenerReplacementKeepsAnExistingReadInFlight() throws Exception {
        String path = MetadataStoreTopicPoliciesService.pathFor(TOPIC, false);
        TopicPolicies stale = new TopicPolicies();
        stale.setMaxProducerPerTopic(3);
        TopicPolicies updated = new TopicPolicies();
        updated.setMaxProducerPerTopic(7);
        CompletableFuture<Optional<TopicPolicies>> firstRead = new CompletableFuture<>();
        AtomicInteger reads = new AtomicInteger();
        when(localCache.get(path)).thenAnswer(invocation -> reads.incrementAndGet() == 1
                ? firstRead : CompletableFuture.completedFuture(Optional.of(updated)));
        AtomicInteger oldCallbacks = new AtomicInteger();
        TopicPolicyListener oldListener = policies -> oldCallbacks.incrementAndGet();
        service.registerListener(TOPIC, oldListener);

        localNotification.get().accept(new Notification(NotificationType.Modified, path));
        assertThat(reads).hasValue(1);
        service.unregisterListener(TOPIC, oldListener);
        localNotification.get().accept(new Notification(NotificationType.Modified, path));
        List<Integer> received = new CopyOnWriteArrayList<>();
        CountDownLatch updatedReceived = new CountDownLatch(1);
        service.registerListener(TOPIC, policies -> {
            received.add(policies.getMaxProducerPerTopic());
            updatedReceived.countDown();
        });
        localNotification.get().accept(new Notification(NotificationType.Modified, path));
        assertThat(reads).as("listener replacement must retain the existing in-flight read").hasValue(1);
        firstRead.complete(Optional.of(stale));

        assertThat(updatedReceived.await(5, TimeUnit.SECONDS)).isTrue();
        notificationExecutor.chooseThread(TOPIC).submit(() -> { }).get(5, TimeUnit.SECONDS);
        assertThat(reads).hasValue(2);
        assertThat(oldCallbacks).hasValue(0);
        assertThat(received).containsExactly(7);
    }

    @Test
    public void testUpdatesDuringACallbackShareOnePendingRefresh() throws Exception {
        String path = MetadataStoreTopicPoliciesService.pathFor(TOPIC, false);
        TopicPolicies initial = new TopicPolicies();
        initial.setMaxProducerPerTopic(3);
        TopicPolicies updated = new TopicPolicies();
        updated.setMaxProducerPerTopic(7);
        AtomicInteger reads = new AtomicInteger();
        when(localCache.get(path)).thenAnswer(invocation -> CompletableFuture.completedFuture(
                Optional.of(reads.incrementAndGet() == 1 ? initial : updated)));
        CountDownLatch callbackEntered = new CountDownLatch(1);
        CountDownLatch releaseCallback = new CountDownLatch(1);
        CountDownLatch updatedReceived = new CountDownLatch(1);
        List<Integer> received = new CopyOnWriteArrayList<>();
        service.registerListener(TOPIC, policies -> {
            received.add(policies.getMaxProducerPerTopic());
            if (policies.getMaxProducerPerTopic() == 3) {
                callbackEntered.countDown();
                awaitLatch(releaseCallback);
            } else {
                updatedReceived.countDown();
            }
        });

        try {
            localNotification.get().accept(new Notification(NotificationType.Modified, path));
            assertThat(callbackEntered.await(5, TimeUnit.SECONDS)).isTrue();
            for (int i = 0; i < 1000; i++) {
                localNotification.get().accept(new Notification(NotificationType.Modified, path));
            }
            assertThat(reads).as("the running callback keeps its refresh in flight").hasValue(1);
            releaseCallback.countDown();

            assertThat(updatedReceived.await(5, TimeUnit.SECONDS)).isTrue();
            notificationExecutor.chooseThread(TOPIC).submit(() -> { }).get(5, TimeUnit.SECONDS);
            assertThat(reads).hasValue(2);
            assertThat(received).containsExactly(3, 7);
        } finally {
            releaseCallback.countDown();
        }
    }

    @Test
    public void testDeletedNotificationDiscardsAnOlderRead() throws Exception {
        String path = MetadataStoreTopicPoliciesService.pathFor(TOPIC, false);
        TopicPolicies stale = new TopicPolicies();
        stale.setMaxProducerPerTopic(3);
        CompletableFuture<Optional<TopicPolicies>> firstRead = new CompletableFuture<>();
        CountDownLatch firstReadStarted = new CountDownLatch(1);
        AtomicInteger callbacks = new AtomicInteger();
        when(localCache.get(path)).thenAnswer(invocation -> {
            firstReadStarted.countDown();
            return firstRead;
        });
        service.registerListener(TOPIC, policies -> callbacks.incrementAndGet());

        localNotification.get().accept(new Notification(NotificationType.Modified, path));
        assertTrue(firstReadStarted.await(5, TimeUnit.SECONDS));
        localNotification.get().accept(new Notification(NotificationType.Deleted, path));
        firstRead.complete(Optional.of(stale));

        notificationExecutor.chooseThread(TOPIC).submit(() -> { }).get(5, TimeUnit.SECONDS);
        assertEquals(callbacks.get(), 1);
    }

    @Test
    public void testDeletedNotificationsWithoutListenersDoNotQueueTasks() {
        String localPath = MetadataStoreTopicPoliciesService.pathFor(TOPIC, false);
        String globalPath = MetadataStoreTopicPoliciesService.pathFor(TOPIC, true);
        clearInvocations(brokerService);

        for (int i = 0; i < 1000; i++) {
            localNotification.get().accept(new Notification(NotificationType.Deleted, localPath));
            globalNotification.get().accept(new Notification(NotificationType.Deleted, globalPath));
        }

        verify(brokerService, never()).getTopicPoliciesNotifyThread(any());
    }

    @Test
    public void testQueuedDeletesAreCoalescedAcrossScopes() throws Exception {
        String localPath = MetadataStoreTopicPoliciesService.pathFor(TOPIC, false);
        String globalPath = MetadataStoreTopicPoliciesService.pathFor(TOPIC, true);
        ExecutorService topicExecutor = notificationExecutor.chooseThread(TOPIC);
        CountDownLatch topicThreadBlocked = new CountDownLatch(1);
        CountDownLatch releaseTopicThread = new CountDownLatch(1);
        topicExecutor.execute(() -> {
            topicThreadBlocked.countDown();
            awaitLatch(releaseTopicThread);
        });
        ExecutorService countingExecutor = mock(ExecutorService.class);
        AtomicInteger dispatches = new AtomicInteger();
        doAnswer(invocation -> {
            topicExecutor.execute(invocation.getArgument(0));
            dispatches.incrementAndGet();
            return null;
        }).when(countingExecutor).execute(any());
        when(brokerService.getTopicPoliciesNotifyThread(TOPIC)).thenReturn(countingExecutor);
        List<TopicPolicies> received = new CopyOnWriteArrayList<>();
        service.registerListener(TOPIC, received::add);

        try {
            assertThat(topicThreadBlocked.await(5, TimeUnit.SECONDS)).isTrue();
            for (int i = 0; i < 1000; i++) {
                boolean global = i % 2 == 0;
                (global ? globalNotification : localNotification).get().accept(
                        new Notification(NotificationType.Deleted, global ? globalPath : localPath));
            }
            assertThat(dispatches).as("duplicate deletes share one pending notification").hasValue(1);
            assertThat(received).isEmpty();

            releaseTopicThread.countDown();
            topicExecutor.submit(() -> { }).get(5, TimeUnit.SECONDS);
            assertThat(received).containsExactly((TopicPolicies) null);
        } finally {
            releaseTopicThread.countDown();
        }
    }

    @Test
    public void testDeletesSeparatedByAQueuedRefreshKeepTheirOrder() throws Exception {
        String localPath = MetadataStoreTopicPoliciesService.pathFor(TOPIC, false);
        String globalPath = MetadataStoreTopicPoliciesService.pathFor(TOPIC, true);
        TopicPolicies local = new TopicPolicies();
        local.setMaxProducerPerTopic(7);
        when(localCache.get(localPath)).thenReturn(CompletableFuture.completedFuture(Optional.of(local)));
        ExecutorService topicExecutor = notificationExecutor.chooseThread(TOPIC);
        CountDownLatch topicThreadBlocked = new CountDownLatch(1);
        CountDownLatch releaseTopicThread = new CountDownLatch(1);
        topicExecutor.execute(() -> {
            topicThreadBlocked.countDown();
            awaitLatch(releaseTopicThread);
        });
        ExecutorService countingExecutor = mock(ExecutorService.class);
        AtomicInteger dispatches = new AtomicInteger();
        doAnswer(invocation -> {
            topicExecutor.execute(invocation.getArgument(0));
            dispatches.incrementAndGet();
            return null;
        }).when(countingExecutor).execute(any());
        when(brokerService.getTopicPoliciesNotifyThread(TOPIC)).thenReturn(countingExecutor);
        List<String> received = new CopyOnWriteArrayList<>();
        service.registerListener(TOPIC, policies -> received.add(policies == null ? "deleted" : "local"));

        try {
            assertThat(topicThreadBlocked.await(5, TimeUnit.SECONDS)).isTrue();
            globalNotification.get().accept(new Notification(NotificationType.Deleted, globalPath));
            localNotification.get().accept(new Notification(NotificationType.Modified, localPath));
            await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> assertThat(dispatches).hasValue(2));
            globalNotification.get().accept(new Notification(NotificationType.Deleted, globalPath));
            assertThat(dispatches).as("the intervening local refresh ends the delete merge window").hasValue(3);

            releaseTopicThread.countDown();
            topicExecutor.submit(() -> { }).get(5, TimeUnit.SECONDS);
            assertThat(received).containsExactly("deleted", "local", "deleted");
        } finally {
            releaseTopicThread.countDown();
        }
    }

    @DataProvider(name = "queuedDeletionListenerReplacement")
    public Object[][] queuedDeletionListenerReplacement() {
        return new Object[][]{{false}, {true}};
    }

    @Test(dataProvider = "queuedDeletionListenerReplacement")
    public void testQueuedDeleteDoesNotNotifyAReplacementListenerWithoutANewEvent(boolean freshDelete)
            throws Exception {
        String localPath = MetadataStoreTopicPoliciesService.pathFor(TOPIC, false);
        String globalPath = MetadataStoreTopicPoliciesService.pathFor(TOPIC, true);
        ExecutorService topicExecutor = notificationExecutor.chooseThread(TOPIC);
        CountDownLatch topicThreadBlocked = new CountDownLatch(1);
        CountDownLatch releaseTopicThread = new CountDownLatch(1);
        topicExecutor.execute(() -> {
            topicThreadBlocked.countDown();
            awaitLatch(releaseTopicThread);
        });
        AtomicInteger oldCallbacks = new AtomicInteger();
        TopicPolicyListener oldListener = policies -> oldCallbacks.incrementAndGet();
        List<TopicPolicies> received = new CopyOnWriteArrayList<>();
        service.registerListener(TOPIC, oldListener);

        try {
            assertThat(topicThreadBlocked.await(5, TimeUnit.SECONDS)).isTrue();
            localNotification.get().accept(new Notification(NotificationType.Deleted, localPath));
            service.unregisterListener(TOPIC, oldListener);
            service.registerListener(TOPIC, received::add);
            if (freshDelete) {
                globalNotification.get().accept(new Notification(NotificationType.Deleted, globalPath));
            }

            releaseTopicThread.countDown();
            topicExecutor.submit(() -> { }).get(5, TimeUnit.SECONDS);
            assertThat(oldCallbacks).hasValue(0);
            if (freshDelete) {
                assertThat(received).containsExactly((TopicPolicies) null);
            } else {
                assertThat(received).as("an old delete must not reach a new listener registration").isEmpty();
            }
        } finally {
            releaseTopicThread.countDown();
        }
    }

    @Test
    public void testDeletedNotificationWaitsForAnOlderCallbackInProgress() throws Exception {
        String path = MetadataStoreTopicPoliciesService.pathFor(TOPIC, false);
        TopicPolicies stale = new TopicPolicies();
        stale.setMaxProducerPerTopic(3);
        CompletableFuture<Optional<TopicPolicies>> firstRead = new CompletableFuture<>();
        AtomicInteger reads = new AtomicInteger();
        when(localCache.get(path)).thenAnswer(invocation -> reads.incrementAndGet() == 1
                ? firstRead : CompletableFuture.completedFuture(Optional.empty()));
        CountDownLatch staleCallbackEntered = new CountDownLatch(1);
        CountDownLatch releaseStaleCallback = new CountDownLatch(1);
        CountDownLatch allCallbacksReceived = new CountDownLatch(2);
        List<String> received = new CopyOnWriteArrayList<>();
        service.registerListener(TOPIC, policies -> {
            if (policies != null) {
                staleCallbackEntered.countDown();
                awaitLatch(releaseStaleCallback);
            }
        });
        service.registerListener(TOPIC, policies -> {
            received.add(policies == null ? "deleted" : "stale");
            allCallbacksReceived.countDown();
        });

        localNotification.get().accept(new Notification(NotificationType.Modified, path));
        ExecutorService completion = Executors.newSingleThreadExecutor();
        try {
            Future<?> firstCompletion = completion.submit(() -> firstRead.complete(Optional.of(stale)));
            assertTrue(staleCallbackEntered.await(5, TimeUnit.SECONDS));
            firstCompletion.get(5, TimeUnit.SECONDS);
            localNotification.get().accept(new Notification(NotificationType.Deleted, path));
            assertThat(received).as("deletion must wait for the current notification").isEmpty();
            releaseStaleCallback.countDown();
            assertTrue(allCallbacksReceived.await(5, TimeUnit.SECONDS));
            assertThat(received).containsExactly("stale", "deleted");
            assertEquals(reads.get(), 1);
        } finally {
            releaseStaleCallback.countDown();
            completion.shutdownNow();
        }
    }

    @Test
    public void testLocalGlobalAndDeleteNotificationsUseTheTopicThread() throws Exception {
        String localPath = MetadataStoreTopicPoliciesService.pathFor(TOPIC, false);
        String globalPath = MetadataStoreTopicPoliciesService.pathFor(TOPIC, true);
        CompletableFuture<Optional<TopicPolicies>> localRead = new CompletableFuture<>();
        CompletableFuture<Optional<TopicPolicies>> globalRead = new CompletableFuture<>();
        CountDownLatch readsStarted = new CountDownLatch(2);
        when(localCache.get(localPath)).thenAnswer(invocation -> {
            readsStarted.countDown();
            return localRead;
        });
        when(globalCache.get(globalPath)).thenAnswer(invocation -> {
            readsStarted.countDown();
            return globalRead;
        });
        Thread topicThread = notificationExecutor.chooseThread(TOPIC)
                .submit(Thread::currentThread).get(5, TimeUnit.SECONDS);
        CountDownLatch localCallbackEntered = new CountDownLatch(1);
        CountDownLatch releaseLocalCallback = new CountDownLatch(1);
        CountDownLatch policiesReceived = new CountDownLatch(2);
        CountDownLatch deletionReceived = new CountDownLatch(1);
        AtomicInteger activeCallbacks = new AtomicInteger();
        AtomicInteger maxActiveCallbacks = new AtomicInteger();
        List<String> received = new CopyOnWriteArrayList<>();
        List<Thread> callbackThreads = new CopyOnWriteArrayList<>();
        service.registerListener(TopicName.get(TOPIC + "-partition-0"), policies -> {
            int active = activeCallbacks.incrementAndGet();
            maxActiveCallbacks.accumulateAndGet(active, Math::max);
            callbackThreads.add(Thread.currentThread());
            try {
                received.add(policies == null ? "deleted" : policies.isGlobalPolicies() ? "global" : "local");
                if (policies != null && !policies.isGlobalPolicies()) {
                    localCallbackEntered.countDown();
                    awaitLatch(releaseLocalCallback);
                }
            } finally {
                activeCallbacks.decrementAndGet();
                if (policies == null) {
                    deletionReceived.countDown();
                } else {
                    policiesReceived.countDown();
                }
            }
        });

        ExecutorService localCompletion = Executors.newSingleThreadExecutor();
        ExecutorService globalCompletion = Executors.newSingleThreadExecutor();
        try {
            localNotification.get().accept(new Notification(NotificationType.Modified, localPath));
            globalNotification.get().accept(new Notification(NotificationType.Modified, globalPath));
            assertThat(readsStarted.await(5, TimeUnit.SECONDS)).isTrue();
            localCompletion.submit(() -> localRead.complete(Optional.of(new TopicPolicies())))
                    .get(5, TimeUnit.SECONDS);
            assertThat(localCallbackEntered.await(5, TimeUnit.SECONDS)).isTrue();
            globalCompletion.submit(() -> globalRead.complete(Optional.of(new TopicPolicies())))
                    .get(5, TimeUnit.SECONDS);
            assertThat(received).as("global notification must wait for the local callback").containsExactly("local");
            releaseLocalCallback.countDown();
            assertThat(policiesReceived.await(5, TimeUnit.SECONDS)).isTrue();
            notificationExecutor.chooseThread(TOPIC).submit(() -> { }).get(5, TimeUnit.SECONDS);

            localCompletion.submit(() -> localNotification.get()
                    .accept(new Notification(NotificationType.Deleted, localPath))).get(5, TimeUnit.SECONDS);
            assertThat(deletionReceived.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(received).containsExactly("local", "global", "deleted");
            assertThat(callbackThreads).containsExactly(topicThread, topicThread, topicThread);
            assertThat(maxActiveCallbacks).hasValue(1);
        } finally {
            releaseLocalCallback.countDown();
            localCompletion.shutdownNow();
            globalCompletion.shutdownNow();
        }
    }

    @DataProvider(name = "queuedRefreshInvalidation")
    public Object[][] queuedRefreshInvalidation() {
        return new Object[][]{{"modified"}, {"deleted"}, {"unregistered"}, {"closed"}};
    }

    @Test(dataProvider = "queuedRefreshInvalidation")
    public void testQueuedCompletionRechecksStateBeforeNotifying(String invalidation) throws Exception {
        String path = MetadataStoreTopicPoliciesService.pathFor(TOPIC, false);
        TopicPolicies stale = new TopicPolicies();
        stale.setMaxProducerPerTopic(3);
        TopicPolicies updated = new TopicPolicies();
        updated.setMaxProducerPerTopic(7);
        CompletableFuture<Optional<TopicPolicies>> firstRead = new CompletableFuture<>();
        CountDownLatch firstReadStarted = new CountDownLatch(1);
        AtomicInteger reads = new AtomicInteger();
        when(localCache.get(path)).thenAnswer(invocation -> {
            firstReadStarted.countDown();
            return reads.incrementAndGet() == 1 ? firstRead
                    : CompletableFuture.completedFuture(Optional.of(updated));
        });
        CountDownLatch topicThreadBlocked = new CountDownLatch(1);
        CountDownLatch releaseTopicThread = new CountDownLatch(1);
        notificationExecutor.chooseThread(TOPIC).execute(() -> {
            topicThreadBlocked.countDown();
            awaitLatch(releaseTopicThread);
        });
        List<TopicPolicies> received = new CopyOnWriteArrayList<>();
        CountDownLatch callbackReceived = new CountDownLatch(1);
        TopicPolicyListener listener = policies -> {
            received.add(policies);
            callbackReceived.countDown();
        };
        service.registerListener(TOPIC, listener);
        try {
            assertThat(topicThreadBlocked.await(5, TimeUnit.SECONDS)).isTrue();
            localNotification.get().accept(new Notification(NotificationType.Modified, path));
            assertThat(firstReadStarted.await(5, TimeUnit.SECONDS)).isTrue();
            await().atMost(5, TimeUnit.SECONDS).until(() -> firstRead.getNumberOfDependents() > 0);
            firstRead.complete(Optional.of(stale));
            assertThat(received).as("completion must be queued on the blocked topic thread").isEmpty();
            switch (invalidation) {
                case "modified" -> localNotification.get().accept(new Notification(NotificationType.Modified, path));
                case "deleted" -> localNotification.get().accept(new Notification(NotificationType.Deleted, path));
                case "unregistered" -> service.unregisterListener(TOPIC, listener);
                case "closed" -> service.close();
                default -> throw new AssertionError("Unexpected invalidation: " + invalidation);
            }
            assertThat(received).as("metadata notifications must not invoke listeners inline").isEmpty();
            releaseTopicThread.countDown();
            if (invalidation.equals("modified") || invalidation.equals("deleted")) {
                assertThat(callbackReceived.await(5, TimeUnit.SECONDS)).isTrue();
            }
            notificationExecutor.chooseThread(TOPIC).submit(() -> { }).get(5, TimeUnit.SECONDS);
            switch (invalidation) {
                case "modified" -> assertThat(received).singleElement()
                        .extracting(TopicPolicies::getMaxProducerPerTopic).isEqualTo(7);
                case "deleted" -> assertThat(received).containsExactly((TopicPolicies) null);
                case "unregistered", "closed" -> assertThat(received).isEmpty();
                default -> throw new AssertionError("Unexpected invalidation: " + invalidation);
            }
        } finally {
            releaseTopicThread.countDown();
        }
    }

    @Test
    public void testRejectedCompletionAllowsTheNextNotificationToRefresh() throws Exception {
        String path = MetadataStoreTopicPoliciesService.pathFor(TOPIC, false);
        TopicPolicies updated = new TopicPolicies();
        updated.setMaxProducerPerTopic(7);
        AtomicInteger reads = new AtomicInteger();
        when(localCache.get(path)).thenAnswer(invocation -> {
            reads.incrementAndGet();
            return CompletableFuture.completedFuture(Optional.of(updated));
        });
        ExecutorService topicExecutor = notificationExecutor.chooseThread(TOPIC);
        Thread topicThread = topicExecutor.submit(Thread::currentThread).get(5, TimeUnit.SECONDS);
        ExecutorService rejectingExecutor = mock(ExecutorService.class);
        AtomicInteger dispatches = new AtomicInteger();
        doAnswer(invocation -> {
            if (dispatches.incrementAndGet() == 1) {
                throw new RejectedExecutionException("Reject the first topic notification");
            }
            topicExecutor.execute(invocation.getArgument(0));
            return null;
        }).when(rejectingExecutor).execute(any());
        when(brokerService.getTopicPoliciesNotifyThread(TOPIC)).thenReturn(rejectingExecutor);
        List<Thread> callbackThreads = new CopyOnWriteArrayList<>();
        CountDownLatch callbackReceived = new CountDownLatch(1);
        service.registerListener(TOPIC, policies -> {
            callbackThreads.add(Thread.currentThread());
            callbackReceived.countDown();
        });

        localNotification.get().accept(new Notification(NotificationType.Modified, path));
        assertThat(callbackThreads).as("rejection must not invoke listeners inline").isEmpty();
        localNotification.get().accept(new Notification(NotificationType.Modified, path));

        assertThat(callbackReceived.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(reads).hasValue(2);
        assertThat(dispatches).hasValue(2);
        assertThat(callbackThreads).containsExactly(topicThread);
    }

    @Test
    public void testRejectedDeleteReloadsLatestPoliciesOnTheTopicThread() throws Exception {
        String path = MetadataStoreTopicPoliciesService.pathFor(TOPIC, true);
        TopicPolicies recreated = new TopicPolicies();
        recreated.setMaxProducerPerTopic(7);
        AtomicInteger reads = new AtomicInteger();
        when(globalCache.get(path)).thenAnswer(invocation -> {
            reads.incrementAndGet();
            return CompletableFuture.completedFuture(Optional.of(recreated));
        });
        ExecutorService topicExecutor = notificationExecutor.chooseThread(TOPIC);
        Thread topicThread = topicExecutor.submit(Thread::currentThread).get(5, TimeUnit.SECONDS);
        CountDownLatch topicThreadBlocked = new CountDownLatch(1);
        CountDownLatch releaseTopicThread = new CountDownLatch(1);
        topicExecutor.execute(() -> {
            topicThreadBlocked.countDown();
            awaitLatch(releaseTopicThread);
        });
        ExecutorService rejectingExecutor = mock(ExecutorService.class);
        AtomicInteger dispatches = new AtomicInteger();
        CountDownLatch latestPoliciesQueued = new CountDownLatch(1);
        doAnswer(invocation -> {
            if (dispatches.incrementAndGet() == 1) {
                throw new RejectedExecutionException("Reject the deletion notification");
            }
            topicExecutor.execute(invocation.getArgument(0));
            latestPoliciesQueued.countDown();
            return null;
        }).when(rejectingExecutor).execute(any());
        when(brokerService.getTopicPoliciesNotifyThread(TOPIC)).thenReturn(rejectingExecutor);
        List<Thread> callbackThreads = new CopyOnWriteArrayList<>();
        List<TopicPolicies> received = new CopyOnWriteArrayList<>();
        CountDownLatch callbackReceived = new CountDownLatch(1);
        service.registerListener(TOPIC, policies -> {
            callbackThreads.add(Thread.currentThread());
            received.add(policies);
            callbackReceived.countDown();
        });

        try {
            assertThat(topicThreadBlocked.await(5, TimeUnit.SECONDS)).isTrue();
            // The node was recreated after this delete event, before recovery could read it again.
            globalNotification.get().accept(new Notification(NotificationType.Deleted, path));
            assertThat(latestPoliciesQueued.await(5, TimeUnit.SECONDS))
                    .as("rejected deletion must reload the latest global policies").isTrue();
            assertThat(received).as("rejection must not invoke listeners on the metadata thread").isEmpty();
            releaseTopicThread.countDown();

            assertThat(callbackReceived.await(5, TimeUnit.SECONDS)).isTrue();
            topicExecutor.submit(() -> { }).get(5, TimeUnit.SECONDS);
            assertThat(reads).hasValue(1);
            assertThat(dispatches).hasValue(2);
            assertThat(callbackThreads).containsExactly(topicThread);
            assertThat(received).singleElement().satisfies(policies -> {
                assertThat(policies).as("do not replay a stale deletion after the node was recreated").isNotNull();
                assertThat(policies.isGlobalPolicies()).isTrue();
                assertThat(policies.getMaxProducerPerTopic()).isEqualTo(7);
            });
        } finally {
            releaseTopicThread.countDown();
        }
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            assertThat(latch.await(10, TimeUnit.SECONDS)).as("blocked callback is released").isTrue();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }
}
