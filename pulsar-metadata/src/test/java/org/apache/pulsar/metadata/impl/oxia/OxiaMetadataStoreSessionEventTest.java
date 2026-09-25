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
package org.apache.pulsar.metadata.impl.oxia;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.Notification;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import org.apache.pulsar.metadata.api.extended.SessionEvent;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Verifies that the session watcher wiring of {@link OxiaMetadataStore} delivers the session
 * events of {@link OxiaSessionWatcher} through the ordinary
 * {@link org.apache.pulsar.metadata.api.extended.MetadataStoreExtended#registerSessionListener}
 * contract, and that the reserved canary namespace is never forwarded as ordinary metadata
 * notifications.
 */
class OxiaMetadataStoreSessionEventTest {

    private AsyncOxiaClient client;

    @BeforeMethod
    void setup() {
        client = mock(AsyncOxiaClient.class);
    }

    private OxiaMetadataStore storeWithSessionWatcher(
            List<SessionEvent> sessionEvents, List<org.apache.pulsar.metadata.api.Notification> notifications) {
        when(client.put(anyString(), any(), any())).thenReturn(CompletableFuture.completedFuture(null));

        OxiaMetadataStore store = new OxiaMetadataStore(client, "identity", true);
        store.registerSessionListener(sessionEvents::add);
        store.registerListener(notifications::add);
        return store;
    }

    @Test
    void canaryDeletionIsDeliveredAsSessionEventsAndNotForwardedAsNotification() throws Exception {
        List<SessionEvent> sessionEvents = new CopyOnWriteArrayList<>();
        List<org.apache.pulsar.metadata.api.Notification> notifications = new CopyOnWriteArrayList<>();

        try (OxiaMetadataStore store = storeWithSessionWatcher(sessionEvents, notifications)) {
            ArgumentCaptor<String> putKey = ArgumentCaptor.forClass(String.class);
            verify(client, timeout(5_000)).put(putKey.capture(), any(), any());
            String canaryKey = putKey.getValue();
            assertThat(canaryKey).startsWith(OxiaSessionWatcher.CANARY_KEY_PREFIX);

            ArgumentCaptor<Consumer<Notification>> notificationCallback = notificationCallbackCaptor();
            verify(client).notifications(notificationCallback.capture());

            notificationCallback.getValue().accept(new Notification.KeyDeleted(canaryKey));

            await().atMost(5, SECONDS)
                    .untilAsserted(() -> assertThat(sessionEvents).containsExactly(
                            SessionEvent.SessionLost, SessionEvent.SessionReestablished));
            assertThat(notifications).isEmpty();
        }
    }

    /**
     * The core ordering property: the canary sorts before every '/'-rooted path, and the
     * notifications of one batch are delivered in key order, so a sweep that deletes the canary
     * and ordinary records together delivers the synthesized SessionLost to the session
     * listeners before the Deleted notifications of the swept records.
     */
    @Test
    void sessionLostIsDeliveredBeforeTheSweptRecordsDeletedNotifications() throws Exception {
        List<SessionEvent> sessionEvents = new CopyOnWriteArrayList<>();
        List<org.apache.pulsar.metadata.api.Notification> notifications = new CopyOnWriteArrayList<>();
        List<String> deliveryOrder = new CopyOnWriteArrayList<>();

        when(client.put(anyString(), any(), any())).thenReturn(CompletableFuture.completedFuture(null));
        try (OxiaMetadataStore store = new OxiaMetadataStore(client, "identity", true)) {
            store.registerSessionListener(event -> {
                sessionEvents.add(event);
                deliveryOrder.add("session:" + event);
            });
            store.registerListener(notification -> {
                notifications.add(notification);
                deliveryOrder.add("notification:" + notification.getPath());
            });

            ArgumentCaptor<String> putKey = ArgumentCaptor.forClass(String.class);
            verify(client, timeout(5_000)).put(putKey.capture(), any(), any());
            ArgumentCaptor<Consumer<Notification>> notificationCallback = notificationCallbackCaptor();
            verify(client).notifications(notificationCallback.capture());

            // One sweep batch, in key order: the canary first, then an ordinary record.
            notificationCallback.getValue().accept(new Notification.KeyDeleted(putKey.getValue()));
            notificationCallback.getValue().accept(new Notification.KeyDeleted("/ledgers/available/bookie-1"));

            String sweptRecord = "notification:/ledgers/available/bookie-1";
            await().atMost(5, SECONDS).until(() -> deliveryOrder.contains(sweptRecord));
            assertThat(deliveryOrder).first().isEqualTo("session:" + SessionEvent.SessionLost);
            assertThat(deliveryOrder.indexOf(sweptRecord))
                    .isGreaterThan(deliveryOrder.indexOf("session:" + SessionEvent.SessionLost));
        }
    }

    @Test
    void canaryNotificationsOfOtherInstancesCauseNeitherEventsNorNotifications() throws Exception {
        List<SessionEvent> sessionEvents = new CopyOnWriteArrayList<>();
        List<org.apache.pulsar.metadata.api.Notification> notifications = new CopyOnWriteArrayList<>();

        try (OxiaMetadataStore store = storeWithSessionWatcher(sessionEvents, notifications)) {
            ArgumentCaptor<Consumer<Notification>> notificationCallback = notificationCallbackCaptor();
            verify(client).notifications(notificationCallback.capture());

            // A foreign canary belongs to another store instance: it carries no state of this
            // instance, so it causes no session event, and the reserved namespace is never
            // forwarded as an ordinary notification either.
            notificationCallback.getValue().accept(new Notification.KeyDeleted(
                    OxiaSessionWatcher.CANARY_KEY_PREFIX + "another-instance"));

            await().during(1, SECONDS).atMost(3, SECONDS)
                    .untilAsserted(() -> {
                        assertThat(sessionEvents).isEmpty();
                        assertThat(notifications).isEmpty();
                    });
        }
    }

    @Test
    void noCanaryIsWrittenWhenTheSessionWatcherIsNotRequested() throws Exception {
        List<org.apache.pulsar.metadata.api.Notification> notifications = new CopyOnWriteArrayList<>();

        try (OxiaMetadataStore store = new OxiaMetadataStore(client, "identity", false)) {
            store.registerListener(notifications::add);
            ArgumentCaptor<Consumer<Notification>> notificationCallback = notificationCallbackCaptor();
            verify(client).notifications(notificationCallback.capture());

            verify(client, never()).put(anyString(), any(), any());

            // The reserved namespace stays suppressed even without the watcher: a foreign
            // canary is not this store's state and must not surface as a notification.
            notificationCallback.getValue().accept(new Notification.KeyDeleted(
                    OxiaSessionWatcher.CANARY_KEY_PREFIX + "another-instance"));

            await().during(1, SECONDS).atMost(3, SECONDS)
                    .untilAsserted(() -> assertThat(notifications).isEmpty());
        }
    }

    @SuppressWarnings("unchecked")
    private ArgumentCaptor<Consumer<Notification>> notificationCallbackCaptor() {
        return ArgumentCaptor.forClass((Class) Consumer.class);
    }
}
