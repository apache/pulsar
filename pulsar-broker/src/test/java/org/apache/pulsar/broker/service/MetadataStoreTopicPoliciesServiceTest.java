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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
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

    @Test
    public void testDeletedNotificationWaitsForAnOlderCallbackInProgress() throws Exception {
        String path = MetadataStoreTopicPoliciesService.pathFor(TOPIC, false);
        TopicPolicies stale = new TopicPolicies();
        stale.setMaxProducerPerTopic(3);
        CompletableFuture<Optional<TopicPolicies>> firstRead = new CompletableFuture<>();
        when(localCache.get(path)).thenReturn(firstRead);
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
        } finally {
            releaseStaleCallback.countDown();
            completion.shutdownNow();
        }
    }

    @DataProvider(name = "queuedNotificationLifecycle")
    public Object[][] queuedNotificationLifecycle() {
        return new Object[][]{
                {"closed", false}, {"unregistered", false}, {"replaced", false},
                {"closed", true}, {"unregistered", true}, {"replaced", true}
        };
    }

    @Test(dataProvider = "queuedNotificationLifecycle")
    public void testQueuedNotificationUsesDispatchSnapshot(String invalidation, boolean keepOtherListener)
            throws Exception {
        String path = MetadataStoreTopicPoliciesService.pathFor(TOPIC, false);
        when(localCache.get(path)).thenReturn(CompletableFuture.completedFuture(Optional.of(new TopicPolicies())));
        CountDownLatch topicThreadBlocked = new CountDownLatch(1);
        CountDownLatch releaseTopicThread = new CountDownLatch(1);
        ExecutorService topicExecutor = notificationExecutor.chooseThread(TOPIC);
        topicExecutor.execute(() -> {
            topicThreadBlocked.countDown();
            awaitLatch(releaseTopicThread);
        });
        AtomicInteger oldCallbacks = new AtomicInteger();
        AtomicInteger replacementCallbacks = new AtomicInteger();
        TopicPolicyListener listener = policies -> oldCallbacks.incrementAndGet();
        if (keepOtherListener) {
            service.registerListener(TopicName.get(TOPIC + "-partition-1"), policies -> { });
        }
        service.registerListener(TOPIC, listener);
        try {
            assertThat(topicThreadBlocked.await(5, TimeUnit.SECONDS)).isTrue();
            localNotification.get().accept(new Notification(NotificationType.Modified, path));
            assertThat(oldCallbacks).as("listeners must not run on the metadata thread").hasValue(0);
            if (invalidation.equals("closed")) {
                service.close();
            } else {
                service.unregisterListener(TOPIC, listener);
                if (invalidation.equals("replaced")) {
                    service.registerListener(TOPIC, policies -> replacementCallbacks.incrementAndGet());
                }
            }
            releaseTopicThread.countDown();
            topicExecutor.submit(() -> { }).get(5, TimeUnit.SECONDS);
            assertThat(oldCallbacks)
                    .as("queued notifications keep their listener snapshot unless the service is closed")
                    .hasValue(invalidation.equals("closed") ? 0 : 1);
            assertThat(replacementCallbacks).as("an old notification must not reach a new registration").hasValue(0);
        } finally {
            releaseTopicThread.countDown();
        }
    }

    @Test
    public void testQueuedNotificationDoesNotOverrideNewListenerInitialization() throws Exception {
        String path = MetadataStoreTopicPoliciesService.pathFor(TOPIC, false);
        TopicPolicies oldValue = new TopicPolicies();
        oldValue.setMaxProducerPerTopic(11);
        TopicPolicies loadedValue = new TopicPolicies();
        loadedValue.setMaxProducerPerTopic(22);
        when(localCache.get(path)).thenReturn(CompletableFuture.completedFuture(Optional.of(oldValue)));
        TopicName partition0 = TopicName.get(TOPIC + "-partition-0");
        TopicName partition1 = TopicName.get(TOPIC + "-partition-1");
        TopicPolicyListener oldListener = policies -> { };
        service.registerListener(partition1, policies -> { });
        service.registerListener(partition0, oldListener);
        ExecutorService topicExecutor = notificationExecutor.chooseThread(TOPIC);
        CountDownLatch topicThreadBlocked = new CountDownLatch(1);
        CountDownLatch releaseTopicThread = new CountDownLatch(1);
        List<Integer> appliedValues = new CopyOnWriteArrayList<>();
        topicExecutor.execute(() -> {
            topicThreadBlocked.countDown();
            awaitLatch(releaseTopicThread);
        });
        try {
            assertThat(topicThreadBlocked.await(5, TimeUnit.SECONDS)).isTrue();
            localNotification.get().accept(new Notification(NotificationType.Modified, path));
            service.unregisterListener(partition0, oldListener);
            TopicPolicyListenerWrapper replacement = new TopicPolicyListenerWrapper(
                    policies -> appliedValues.add(policies.getMaxProducerPerTopic()));
            service.registerListener(partition0, replacement);
            topicExecutor.execute(() -> replacement.completeInitialization(null, loadedValue));
            releaseTopicThread.countDown();
            topicExecutor.submit(() -> { }).get(5, TimeUnit.SECONDS);
            assertThat(appliedValues)
                    .as("a notification queued before registration must not override the newly loaded policies")
                    .containsExactly(22);
        } finally {
            releaseTopicThread.countDown();
        }
    }

    @DataProvider(name = "rejectedNotifications")
    public Object[][] rejectedNotifications() {
        return new Object[][]{{false, false}, {true, false}, {false, true}};
    }

    @Test(dataProvider = "rejectedNotifications")
    public void testRejectedNotificationDoesNotRunInline(boolean global, boolean deleted) throws Exception {
        String path = MetadataStoreTopicPoliciesService.pathFor(TOPIC, global);
        when((global ? globalCache : localCache).get(path))
                .thenReturn(CompletableFuture.completedFuture(Optional.of(new TopicPolicies())));
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
        Consumer<Notification> metadataNotification = (global ? globalNotification : localNotification).get();
        Notification notification = new Notification(
                deleted ? NotificationType.Deleted : NotificationType.Modified, path);

        assertThatCode(() -> metadataNotification.accept(notification)).doesNotThrowAnyException();
        assertThat(callbackThreads).as("rejection must not invoke listeners inline").isEmpty();
        metadataNotification.accept(notification);

        assertThat(callbackReceived.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(dispatches).hasValue(2);
        assertThat(callbackThreads).containsExactly(topicThread);
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
