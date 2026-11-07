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
package org.apache.pulsar.broker.resources;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.NotificationType;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.api.extended.SessionEvent;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TopicResourcesTest {

    private MetadataStoreExtended metadataStore;
    private TopicResources topicResources;

    @BeforeMethod
    public void setup() {
        metadataStore = mock(MetadataStoreExtended.class);
        topicResources = new TopicResources(metadataStore);
    }

    @Test
    public void testConstructorRegistersAsListener() {
        verify(metadataStore).registerCancellableListener(any());
    }

    @Test
    public void testCloseUnregistersListeners() {
        Runnable cancelListener = mock(Runnable.class);
        Runnable cancelSessionListener = mock(Runnable.class);
        when(metadataStore.registerCancellableListener(any())).thenReturn(cancelListener);
        when(metadataStore.registerCancellableSessionListener(any())).thenReturn(cancelSessionListener);
        TopicResources resources = new TopicResources(metadataStore);

        resources.close();

        verify(cancelListener).run();
        verify(cancelSessionListener).run();
    }

    @Test
    public void testListenerInvokedWhenTopicCreated() {
        TopicListener listener = mock(TopicListener.class);
        when(listener.getNamespaceName()).thenReturn(NamespaceName.get("tenant/namespace"));
        topicResources.registerPersistentTopicListener(listener);
        topicResources.handleNotification(new Notification(NotificationType.Created,
                "/managed-ledgers/tenant/namespace/persistent/topic"));
        verify(listener).onTopicEvent("persistent://tenant/namespace/topic", NotificationType.Created);
    }

    @Test
    public void testListenerInvokedWhenTopicDeleted() {
        TopicListener listener = mock(TopicListener.class);
        when(listener.getNamespaceName()).thenReturn(NamespaceName.get("tenant/namespace"));
        topicResources.registerPersistentTopicListener(listener);
        topicResources.handleNotification(new Notification(NotificationType.Deleted,
                "/managed-ledgers/tenant/namespace/persistent/topic"));
        verify(listener).onTopicEvent("persistent://tenant/namespace/topic", NotificationType.Deleted);
    }

    @Test
    public void testListenerNotInvokedWhenSubscriptionCreated() {
        TopicListener listener = mock(TopicListener.class);
        when(listener.getNamespaceName()).thenReturn(NamespaceName.get("tenant/namespace"));
        topicResources.registerPersistentTopicListener(listener);
        verify(listener).getNamespaceName();
        topicResources.handleNotification(new Notification(NotificationType.Created,
                "/managed-ledgers/tenant/namespace/persistent/topic/subscription"));
        verifyNoMoreInteractions(listener);
    }

    @Test
    public void testListenerNotInvokedWhenTopicCreatedInOtherNamespace() {
        TopicListener listener = mock(TopicListener.class);
        when(listener.getNamespaceName()).thenReturn(NamespaceName.get("tenant/namespace"));
        topicResources.registerPersistentTopicListener(listener);
        verify(listener).getNamespaceName();
        topicResources.handleNotification(new Notification(NotificationType.Created,
                "/managed-ledgers/tenant/namespace2/persistent/topic"));
        verifyNoMoreInteractions(listener);
    }

    @Test
    public void testListenerNotInvokedWhenTopicModified() {
        TopicListener listener = mock(TopicListener.class);
        when(listener.getNamespaceName()).thenReturn(NamespaceName.get("tenant/namespace"));
        topicResources.registerPersistentTopicListener(listener);
        verify(listener).getNamespaceName();
        topicResources.handleNotification(new Notification(NotificationType.Modified,
                "/managed-ledgers/tenant/namespace/persistent/topic"));
        verifyNoMoreInteractions(listener);
    }

    @Test
    public void testListenerNotInvokedForSystemTopicChanges() {
        TopicListener listener = mock(TopicListener.class);
        when(listener.getNamespaceName()).thenReturn(NamespaceName.get("tenant/namespace"));
        topicResources.registerPersistentTopicListener(listener);
        verify(listener).getNamespaceName();
        topicResources.handleNotification(new Notification(NotificationType.Created,
                "/managed-ledgers/tenant/namespace/persistent/__change_events"));
        verifyNoMoreInteractions(listener);
    }

    @Test
    public void testListenerNotInvokedAfterDeregistered() {
        TopicListener listener = mock(TopicListener.class);
        when(listener.getNamespaceName()).thenReturn(NamespaceName.get("tenant/namespace"));
        topicResources.registerPersistentTopicListener(listener);
        verify(listener).getNamespaceName();
        topicResources.handleNotification(new Notification(NotificationType.Created,
                "/managed-ledgers/tenant/namespace/persistent/topic"));
        verify(listener).onTopicEvent("persistent://tenant/namespace/topic", NotificationType.Created);
        topicResources.deregisterPersistentTopicListener(listener);
        topicResources.handleNotification(new Notification(NotificationType.Created,
                "/managed-ledgers/tenant/namespace/persistent/topic2"));
        verifyNoMoreInteractions(listener);
    }

    @Test
    public void testListenerInvokedWithDecodedTopicName() {
        TopicListener listener = mock(TopicListener.class);
        when(listener.getNamespaceName()).thenReturn(NamespaceName.get("tenant/namespace"));
        topicResources.registerPersistentTopicListener(listener);
        verify(listener).getNamespaceName();
        topicResources.handleNotification(new Notification(NotificationType.Created,
                "/managed-ledgers/tenant/namespace/persistent/topic%3Atest"));
        verify(listener).onTopicEvent("persistent://tenant/namespace/topic:test", NotificationType.Created);
    }

    @Test
    public void testNamespaceContainsDotsShouldntMatchAny() {
        TopicListener listener = mock(TopicListener.class);
        when(listener.getNamespaceName()).thenReturn(NamespaceName.get("tenant/name.pace"));
        topicResources.registerPersistentTopicListener(listener);
        verify(listener).getNamespaceName();
        topicResources.handleNotification(new Notification(NotificationType.Created,
                "/managed-ledgers/tenant/namespace/persistent/topic"));
        verifyNoMoreInteractions(listener);
        topicResources.handleNotification(new Notification(NotificationType.Created,
                "/managed-ledgers/tenant/name.pace/persistent/topic"));
        verify(listener).onTopicEvent("persistent://tenant/name.pace/topic", NotificationType.Created);
    }

    @Test
    public void testListenersAreDispatchedOnlyWithinTheirNamespace() {
        TopicListener first = mock(TopicListener.class);
        TopicListener second = mock(TopicListener.class);
        TopicListener other = mock(TopicListener.class);
        when(first.getNamespaceName()).thenReturn(NamespaceName.get("tenant/namespace"));
        when(second.getNamespaceName()).thenReturn(NamespaceName.get("tenant/namespace"));
        when(other.getNamespaceName()).thenReturn(NamespaceName.get("tenant/namespace2"));
        topicResources.registerPersistentTopicListener(first);
        topicResources.registerPersistentTopicListener(second);
        topicResources.registerPersistentTopicListener(other);

        topicResources.handleNotification(new Notification(NotificationType.Created,
                "/managed-ledgers/tenant/namespace/persistent/topic"));

        verify(first).onTopicEvent("persistent://tenant/namespace/topic", NotificationType.Created);
        verify(second).onTopicEvent("persistent://tenant/namespace/topic", NotificationType.Created);
        verify(other, never()).onTopicEvent(any(), any());
        assertThat(topicResources.namespaceListenerBucketCount()).isEqualTo(2);
    }

    @Test
    public void testInvalidMetadataPathsDoNotDispatchTopicEvents() {
        TopicListener listener = mock(TopicListener.class);
        when(listener.getNamespaceName()).thenReturn(NamespaceName.get("tenant/namespace"));
        topicResources.registerPersistentTopicListener(listener);

        for (String path : List.of(
                "/managed-ledgers/tenant/namespace/persistent/",
                "/managed-ledgers/tenant/namespace/persistent/topic/subscription",
                "/managed-ledgers/tenant/namespace/persistent/topic/",
                "/managed-ledgers/tenant/namespace/non-persistent/topic",
                "/managed-ledgers/tenant/namespace/segment/topic",
                "/managed-ledgers/tenant/namespace/segment/topic/persistent/descriptor",
                "/managed-ledgers-extra/tenant/namespace/persistent/topic",
                "/managed-ledgers/tenant/cluster/ns/persistent/topic",
                "/managed-ledgers/tenant/namespace2/persistent/topic")) {
            topicResources.handleNotification(new Notification(NotificationType.Created, path));
        }

        verify(listener).getNamespaceName();
        verifyNoMoreInteractions(listener);
    }

    @Test
    public void testNamespaceNamedPersistentIsMatchedCorrectly() {
        TopicListener listener = mock(TopicListener.class);
        when(listener.getNamespaceName()).thenReturn(NamespaceName.get("tenant/persistent"));
        topicResources.registerPersistentTopicListener(listener);

        topicResources.handleNotification(new Notification(NotificationType.Created,
                "/managed-ledgers/tenant/persistent/persistent/topic"));

        verify(listener).onTopicEvent("persistent://tenant/persistent/topic", NotificationType.Created);
    }

    @Test
    public void testTopicNamedPersistentIsDispatchedButItsSubscriptionIsIgnored() {
        TopicListener listener = mock(TopicListener.class);
        when(listener.getNamespaceName()).thenReturn(NamespaceName.get("tenant/namespace"));
        topicResources.registerPersistentTopicListener(listener);

        String topicPath = "/managed-ledgers/tenant/namespace/persistent/persistent";
        topicResources.handleNotification(new Notification(NotificationType.Created, topicPath));
        topicResources.handleNotification(new Notification(NotificationType.Deleted, topicPath));
        topicResources.handleNotification(new Notification(NotificationType.Created, topicPath + "/sub"));
        topicResources.handleNotification(new Notification(NotificationType.Deleted, topicPath + "/sub"));

        verify(listener).getNamespaceName();
        verify(listener).onTopicEvent("persistent://tenant/namespace/persistent", NotificationType.Created);
        verify(listener).onTopicEvent("persistent://tenant/namespace/persistent", NotificationType.Deleted);
        verifyNoMoreInteractions(listener);
    }

    @Test
    public void testNamespaceAndTopicBothNamedPersistentDoNotConfusePathParsing() {
        TopicListener listener = mock(TopicListener.class);
        when(listener.getNamespaceName()).thenReturn(NamespaceName.get("tenant/persistent"));
        topicResources.registerPersistentTopicListener(listener);

        String topicPath = "/managed-ledgers/tenant/persistent/persistent/persistent";
        topicResources.handleNotification(new Notification(NotificationType.Created, topicPath));
        topicResources.handleNotification(new Notification(NotificationType.Created, topicPath + "/sub"));

        verify(listener).getNamespaceName();
        verify(listener).onTopicEvent("persistent://tenant/persistent/persistent", NotificationType.Created);
        verifyNoMoreInteractions(listener);
    }

    @Test
    public void testEqualListenerReregistrationMovesNamespaceAndRetainsOriginalCallback() {
        CountingTopicListener original = new CountingTopicListener("same", NamespaceName.get("tenant/first"));
        CountingTopicListener replacement = new CountingTopicListener("same", NamespaceName.get("tenant/second"));
        topicResources.registerPersistentTopicListener(original);
        topicResources.registerPersistentTopicListener(replacement);

        topicResources.handleNotification(new Notification(NotificationType.Created,
                "/managed-ledgers/tenant/first/persistent/old"));
        topicResources.handleNotification(new Notification(NotificationType.Created,
                "/managed-ledgers/tenant/second/persistent/new"));

        assertThat(original.events.get()).isEqualTo(1);
        assertThat(replacement.events.get()).isZero();
        assertThat(topicResources.namespaceListenerBucketCount()).isEqualTo(1);

        topicResources.deregisterPersistentTopicListener(replacement);
        assertThat(topicResources.namespaceListenerBucketCount()).isZero();
    }

    @Test
    public void testEqualListenerReregistrationInSameNamespaceDoesNotDuplicateCallbacks() {
        NamespaceName namespace = NamespaceName.get("tenant/namespace");
        CountingTopicListener original = new CountingTopicListener("same", namespace);
        CountingTopicListener replacement = new CountingTopicListener("same", namespace);
        topicResources.registerPersistentTopicListener(original);
        topicResources.registerPersistentTopicListener(replacement);

        topicResources.handleNotification(new Notification(NotificationType.Created,
                "/managed-ledgers/tenant/namespace/persistent/topic"));

        assertThat(original.events.get()).isEqualTo(1);
        assertThat(replacement.events.get()).isZero();
        assertThat(topicResources.namespaceListenerBucketCount()).isEqualTo(1);
    }

    @Test
    public void testRepeatedNamespaceMigrationKeepsOriginalListenerAndReleasesOldBuckets() {
        CountingTopicListener original = new CountingTopicListener("same", NamespaceName.get("tenant/first"));
        CountingTopicListener second = new CountingTopicListener("same", NamespaceName.get("tenant/second"));
        topicResources.registerPersistentTopicListener(original);
        topicResources.registerPersistentTopicListener(second);
        topicResources.registerPersistentTopicListener(original);

        topicResources.handleNotification(new Notification(NotificationType.Created,
                "/managed-ledgers/tenant/second/persistent/old"));
        topicResources.handleNotification(new Notification(NotificationType.Created,
                "/managed-ledgers/tenant/first/persistent/current"));

        assertThat(original.events.get()).isEqualTo(1);
        assertThat(second.events.get()).isZero();
        assertThat(topicResources.namespaceListenerBucketCount()).isEqualTo(1);

        topicResources.deregisterPersistentTopicListener(second);
        assertThat(topicResources.namespaceListenerBucketCount()).isZero();
    }

    @Test
    public void testListenerCanMigrateNamespaceDuringNotification() {
        NamespaceName first = NamespaceName.get("tenant/first");
        NamespaceName second = NamespaceName.get("tenant/second");
        AtomicReference<NamespaceName> namespace = new AtomicReference<>(first);
        TopicListener listener = mock(TopicListener.class);
        when(listener.getNamespaceName()).thenAnswer(__ -> namespace.get());
        doAnswer(__ -> {
            namespace.set(second);
            topicResources.registerPersistentTopicListener(listener);
            return null;
        }).when(listener).onTopicEvent("persistent://tenant/first/topic", NotificationType.Created);
        topicResources.registerPersistentTopicListener(listener);

        topicResources.handleNotification(new Notification(NotificationType.Created,
                "/managed-ledgers/tenant/first/persistent/topic"));
        topicResources.handleNotification(new Notification(NotificationType.Created,
                "/managed-ledgers/tenant/first/persistent/stale"));
        topicResources.handleNotification(new Notification(NotificationType.Created,
                "/managed-ledgers/tenant/second/persistent/current"));

        verify(listener).onTopicEvent("persistent://tenant/first/topic", NotificationType.Created);
        verify(listener, never()).onTopicEvent("persistent://tenant/first/stale", NotificationType.Created);
        verify(listener).onTopicEvent("persistent://tenant/second/current", NotificationType.Created);
        assertThat(topicResources.namespaceListenerBucketCount()).isEqualTo(1);
    }

    @Test(timeOut = 15000)
    public void testConcurrentNamespaceMigrationsAndNotifications() throws Exception {
        NamespaceName first = NamespaceName.get("tenant/first");
        NamespaceName second = NamespaceName.get("tenant/second");
        AtomicReference<NamespaceName> namespace = new AtomicReference<>(first);
        TopicListener listener = mock(TopicListener.class);
        when(listener.getNamespaceName()).thenAnswer(__ -> namespace.get());
        AtomicInteger eventCount = new AtomicInteger();
        doAnswer(__ -> {
            eventCount.incrementAndGet();
            return null;
        }).when(listener).onTopicEvent(any(), any());
        topicResources.registerPersistentTopicListener(listener);

        Notification firstNotification = new Notification(NotificationType.Created,
                "/managed-ledgers/tenant/first/persistent/topic");
        Notification secondNotification = new Notification(NotificationType.Created,
                "/managed-ledgers/tenant/second/persistent/topic");
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<?> migration = executor.submit(() -> {
                start.await();
                for (int i = 0; i < 2000; i++) {
                    namespace.set((i & 1) == 0 ? second : first);
                    topicResources.registerPersistentTopicListener(listener);
                }
                return null;
            });
            Future<?> notifications = executor.submit(() -> {
                start.await();
                for (int i = 0; i < 2000; i++) {
                    topicResources.handleNotification(firstNotification);
                    topicResources.handleNotification(secondNotification);
                }
                return null;
            });
            start.countDown();
            migration.get(10, TimeUnit.SECONDS);
            notifications.get(10, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
        }

        namespace.set(first);
        topicResources.registerPersistentTopicListener(listener);
        assertThat(topicResources.namespaceListenerBucketCount()).isEqualTo(1);
        int previousEvents = eventCount.get();
        topicResources.handleNotification(secondNotification);
        assertThat(eventCount.get()).isEqualTo(previousEvents);
        topicResources.handleNotification(firstNotification);
        assertThat(eventCount.get()).isEqualTo(previousEvents + 1);
        topicResources.deregisterPersistentTopicListener(listener);
        assertThat(topicResources.namespaceListenerBucketCount()).isZero();
    }

    @Test
    public void testRemovingLastListenerReleasesNamespaceBucket() {
        TopicListener first = mock(TopicListener.class);
        TopicListener second = mock(TopicListener.class);
        when(first.getNamespaceName()).thenReturn(NamespaceName.get("tenant/one"));
        when(second.getNamespaceName()).thenReturn(NamespaceName.get("tenant/two"));
        topicResources.registerPersistentTopicListener(first);
        topicResources.registerPersistentTopicListener(second);
        assertThat(topicResources.namespaceListenerBucketCount()).isEqualTo(2);

        topicResources.deregisterPersistentTopicListener(first);
        assertThat(topicResources.namespaceListenerBucketCount()).isEqualTo(1);
        topicResources.deregisterPersistentTopicListener(second);
        assertThat(topicResources.namespaceListenerBucketCount()).isZero();

        topicResources.registerPersistentTopicListener(first);
        topicResources.handleNotification(new Notification(NotificationType.Created,
                "/managed-ledgers/tenant/one/persistent/recreated"));
        verify(first).onTopicEvent("persistent://tenant/one/recreated", NotificationType.Created);
    }

    @SuppressWarnings({"deprecation", "unchecked"})
    @Test
    public void testLegacyBiConsumerReregistrationMovesNamespace() {
        BiConsumer<String, NotificationType> callback = mock(BiConsumer.class);
        topicResources.registerPersistentTopicListener(NamespaceName.get("tenant/first"), callback);
        topicResources.registerPersistentTopicListener(NamespaceName.get("tenant/second"), callback);

        topicResources.handleNotification(new Notification(NotificationType.Created,
                "/managed-ledgers/tenant/first/persistent/old"));
        topicResources.handleNotification(new Notification(NotificationType.Created,
                "/managed-ledgers/tenant/second/persistent/new"));

        verify(callback).accept("persistent://tenant/second/new", NotificationType.Created);
        verifyNoMoreInteractions(callback);
        assertThat(topicResources.namespaceListenerBucketCount()).isEqualTo(1);

        topicResources.deregisterPersistentTopicListener(callback);
        assertThat(topicResources.namespaceListenerBucketCount()).isZero();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void testSessionEventsReachAllNamespacesWhenOneListenerFails() {
        TopicListener failing = mock(TopicListener.class);
        TopicListener healthy = mock(TopicListener.class);
        when(failing.getNamespaceName()).thenReturn(NamespaceName.get("tenant/one"));
        when(healthy.getNamespaceName()).thenReturn(NamespaceName.get("tenant/two"));
        topicResources.registerPersistentTopicListener(failing);
        topicResources.registerPersistentTopicListener(healthy);
        doThrow(new IllegalStateException("test failure"))
                .when(failing).onSessionEvent(SessionEvent.Reconnected);

        ArgumentCaptor<Consumer<SessionEvent>> sessionListener = ArgumentCaptor.forClass(Consumer.class);
        verify(metadataStore).registerCancellableSessionListener(sessionListener.capture());
        sessionListener.getValue().accept(SessionEvent.Reconnected);

        verify(failing).onSessionEvent(SessionEvent.Reconnected);
        verify(healthy).onSessionEvent(SessionEvent.Reconnected);
    }

    @Test
    public void testListenerCanDeregisterItselfDuringNotification() {
        TopicListener listener = mock(TopicListener.class);
        when(listener.getNamespaceName()).thenReturn(NamespaceName.get("tenant/namespace"));
        doAnswer(__ -> {
            topicResources.deregisterPersistentTopicListener(listener);
            return null;
        }).when(listener).onTopicEvent(any(), any());
        topicResources.registerPersistentTopicListener(listener);

        topicResources.handleNotification(new Notification(NotificationType.Created,
                "/managed-ledgers/tenant/namespace/persistent/topic"));

        assertThat(topicResources.namespaceListenerBucketCount()).isZero();
    }

    @Test(timeOut = 15000)
    public void testConcurrentRegistrationsAndNotifications() throws Exception {
        CountingTopicListener listener = new CountingTopicListener("concurrent", NamespaceName.get("tenant/namespace"));
        Notification notification = new Notification(NotificationType.Created,
                "/managed-ledgers/tenant/namespace/persistent/topic");
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<?> registration = executor.submit(() -> {
                start.await();
                for (int i = 0; i < 2000; i++) {
                    topicResources.registerPersistentTopicListener(listener);
                    topicResources.deregisterPersistentTopicListener(listener);
                }
                return null;
            });
            Future<?> notifications = executor.submit(() -> {
                start.await();
                for (int i = 0; i < 2000; i++) {
                    topicResources.handleNotification(notification);
                }
                return null;
            });
            start.countDown();
            registration.get(10, TimeUnit.SECONDS);
            notifications.get(10, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
        }

        assertThat(topicResources.namespaceListenerBucketCount()).isZero();
        topicResources.registerPersistentTopicListener(listener);
        int eventsBefore = listener.events.get();
        topicResources.handleNotification(notification);
        assertThat(listener.events.get()).isEqualTo(eventsBefore + 1);
        topicResources.deregisterPersistentTopicListener(listener);
        assertThat(topicResources.namespaceListenerBucketCount()).isZero();
    }

    @SuppressWarnings({"deprecation", "unchecked"})
    @Test
    public void testBiConsumerListenerNotInvokedAfterDeregistered() {
        BiConsumer<String, NotificationType> listener = mock(BiConsumer.class);
        topicResources.registerPersistentTopicListener(NamespaceName.get("tenant/namespace"), listener);
        topicResources.handleNotification(new Notification(NotificationType.Created,
                "/managed-ledgers/tenant/namespace/persistent/topic"));
        verify(listener).accept("persistent://tenant/namespace/topic", NotificationType.Created);
        topicResources.deregisterPersistentTopicListener(listener);
        topicResources.handleNotification(new Notification(NotificationType.Created,
                "/managed-ledgers/tenant/namespace/persistent/topic2"));
        verifyNoMoreInteractions(listener);
    }

    @Test
    public void testListSegmentTopics() throws Exception {
        String segmentDomain = "/managed-ledgers/tenant/namespace/segment";
        when(metadataStore.getChildren(segmentDomain))
                .thenReturn(CompletableFuture.completedFuture(List.of("topic", "my%20topic")));
        when(metadataStore.getChildren(segmentDomain + "/topic"))
                .thenReturn(CompletableFuture.completedFuture(List.of("0000-7fff-1", "8000-ffff-2")));
        when(metadataStore.getChildren(segmentDomain + "/my%20topic"))
                .thenReturn(CompletableFuture.completedFuture(List.of("0000-ffff-0")));

        assertThat(topicResources.listSegmentTopicsAsync(NamespaceName.get("tenant/namespace")).get())
                .containsExactlyInAnyOrder(
                        "segment://tenant/namespace/topic/0000-7fff-1",
                        "segment://tenant/namespace/topic/8000-ffff-2",
                        "segment://tenant/namespace/my topic/0000-ffff-0");
    }

    private static final class CountingTopicListener implements TopicListener {
        private final String identity;
        private final NamespaceName namespace;
        private final AtomicInteger events = new AtomicInteger();

        private CountingTopicListener(String identity, NamespaceName namespace) {
            this.identity = identity;
            this.namespace = namespace;
        }

        @Override
        public NamespaceName getNamespaceName() {
            return namespace;
        }

        @Override
        public void onTopicEvent(String topicName, NotificationType notificationType) {
            events.incrementAndGet();
        }

        @Override
        public void onSessionEvent(SessionEvent sessionEvent) {
            // No session state is required by these test listeners.
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof CountingTopicListener other && identity.equals(other.identity);
        }

        @Override
        public int hashCode() {
            return identity.hashCode();
        }
    }
}
