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
package org.apache.pulsar.metadata.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pulsar.metadata.api.extended.SessionEvent;
import org.apache.zookeeper.AddWatchMode;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.client.ZKClientConfig;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

@Test
public class ZKSessionWatcherTest {

    @Test
    public void testClosedEventShouldNotBeTreatedAsReconnectedAfterSessionLost() throws Exception {
        List<SessionEvent> events = new CopyOnWriteArrayList<>();
        try (ZKSessionWatcher watcher = newSessionWatcher(events)) {
            watcher.setSessionInvalid();
            watcher.process(new WatchedEvent(EventType.None, KeeperState.Closed, null));

            assertTrue(events.isEmpty(),
                    "Closed is a terminal state for the old ZooKeeper handle and must not be treated as "
                            + "Reconnected or SessionReestablished, but received " + events);
        }
    }

    @Test
    public void testOnlySyncConnectedShouldBeTreatedAsReconnectedAfterSessionLost() throws Exception {
        List<SessionEvent> events = new CopyOnWriteArrayList<>();
        try (ZKSessionWatcher watcher = newSessionWatcher(events)) {
            watcher.setSessionInvalid();
            watcher.process(new WatchedEvent(EventType.None, KeeperState.SyncConnected, null));

            assertEquals(events, Arrays.asList(SessionEvent.Reconnected, SessionEvent.SessionReestablished));
        }
    }

    @Test
    public void testNonSyncConnectedEventsShouldNotBeTreatedAsReconnectedAfterSessionLost() throws Exception {
        for (KeeperState keeperState : Arrays.asList(
                KeeperState.Disconnected,
                KeeperState.AuthFailed,
                KeeperState.ConnectedReadOnly,
                KeeperState.SaslAuthenticated,
                KeeperState.Closed)) {
            List<SessionEvent> events = new CopyOnWriteArrayList<>();
            try (ZKSessionWatcher watcher = newSessionWatcher(events)) {
                watcher.setSessionInvalid();
                watcher.process(new WatchedEvent(EventType.None, keeperState, null));

                assertTrue(events.stream().noneMatch(event -> event == SessionEvent.Reconnected
                                || event == SessionEvent.SessionReestablished),
                        keeperState + " must not be treated as a ZooKeeper reconnection event, but received "
                                + events);
            }
        }
    }

    @Test
    public void testAuthFailedProbeShouldMarkSessionLostForTerminalClientState() throws Exception {
        List<SessionEvent> events = new CopyOnWriteArrayList<>();
        ZooKeeper zk = newSessionZooKeeper();
        when(zk.getState()).thenReturn(States.AUTH_FAILED);
        completeExistsWith(zk, KeeperException.Code.AUTHFAILED);

        try (ZKSessionWatcher watcher = newSessionWatcher(zk, events)) {
            watcher.checkConnectionStatus();
            watcher.process(new WatchedEvent(EventType.None, KeeperState.AuthFailed, null));
            watcher.checkConnectionStatus();

            assertThat(events).containsExactly(SessionEvent.SessionLost);
        }
    }

    @Test
    public void testAuthFailedStateFromSuccessfulProbeShouldMarkSessionLost() throws Exception {
        List<SessionEvent> events = new CopyOnWriteArrayList<>();
        ZooKeeper zk = newSessionZooKeeper();
        when(zk.getState()).thenReturn(States.AUTH_FAILED);
        completeExistsWith(zk, KeeperException.Code.OK);

        try (ZKSessionWatcher watcher = newSessionWatcher(zk, events)) {
            watcher.checkConnectionStatus();
            watcher.checkConnectionStatus();

            assertThat(events).containsExactly(SessionEvent.SessionLost);
        }
    }

    @Test
    public void testAuthFailedEventWhileConnectingShouldWaitForConnectionState() throws Exception {
        List<SessionEvent> events = new CopyOnWriteArrayList<>();
        ZooKeeper zk = newSessionZooKeeper();
        when(zk.getState()).thenReturn(States.CONNECTING);

        try (ZKSessionWatcher watcher = newSessionWatcher(zk, events)) {
            watcher.process(new WatchedEvent(EventType.None, KeeperState.AuthFailed, null));

            assertThat(events).isEmpty();
        }
    }

    @Test
    public void testTerminalAuthFailedThenSyncConnectedReestablishesSession() throws Exception {
        List<SessionEvent> events = new CopyOnWriteArrayList<>();
        ZooKeeper zk = newSessionZooKeeper();
        when(zk.getState()).thenReturn(States.AUTH_FAILED, States.CONNECTED);

        try (ZKSessionWatcher watcher = newSessionWatcher(zk, events)) {
            watcher.process(new WatchedEvent(EventType.None, KeeperState.AuthFailed, null));
            watcher.process(new WatchedEvent(EventType.None, KeeperState.Closed, null));
            watcher.process(new WatchedEvent(EventType.None, KeeperState.SyncConnected, null));

            assertThat(events).containsExactly(
                    SessionEvent.SessionLost, SessionEvent.Reconnected, SessionEvent.SessionReestablished);
        }
    }

    @Test
    public void testSessionReplacedWhileListenerIsNotifiedReestablishesWithNextSession() throws Exception {
        ZooKeeper zk = mock(ZooKeeper.class);
        when(zk.getSessionTimeout()).thenReturn(600_000);
        AtomicLong sessionId = new AtomicLong(1);
        when(zk.getSessionId()).thenAnswer(invocation -> sessionId.get());
        List<SessionEvent> events = new CopyOnWriteArrayList<>();
        AtomicReference<ZKSessionWatcher> watcherRef = new AtomicReference<>();
        AtomicBoolean replaceOnce = new AtomicBoolean(true);

        try (ZKSessionWatcher watcher = new ZKSessionWatcher(zk, event -> {
            events.add(event);
            if (event == SessionEvent.SessionReestablished && replaceOnce.getAndSet(false)) {
                watcherRef.get().setSessionReplaced();
            }
        }, false)) {
            watcherRef.set(watcher);

            watcher.process(new WatchedEvent(EventType.None, KeeperState.Expired, null));
            watcher.process(new WatchedEvent(EventType.None, KeeperState.Closed, null));
            assertThat(events).containsExactly(SessionEvent.SessionLost);

            watcher.process(new WatchedEvent(EventType.None, KeeperState.SyncConnected, null));
            assertThat(events).containsExactly(
                    SessionEvent.SessionLost, SessionEvent.Reconnected, SessionEvent.SessionReestablished);

            // The old session may still report a successful probe or a close event while it is being replaced.
            watcher.process(new WatchedEvent(EventType.None, KeeperState.SyncConnected, null));
            watcher.process(new WatchedEvent(EventType.None, KeeperState.Closed, null));
            assertThat(events).containsExactly(
                    SessionEvent.SessionLost, SessionEvent.Reconnected, SessionEvent.SessionReestablished);

            sessionId.set(2);
            watcher.process(new WatchedEvent(EventType.None, KeeperState.SyncConnected, null));
            assertThat(events).containsExactly(
                    SessionEvent.SessionLost, SessionEvent.Reconnected, SessionEvent.SessionReestablished,
                    SessionEvent.Reconnected, SessionEvent.SessionReestablished);
        }
    }

    @Test
    public void testReplacedSessionIdRejectsOldEventBeforeGenerationPublication() throws Exception {
        PulsarZooKeeperClient zk = mock(PulsarZooKeeperClient.class);
        AtomicLong sessionId = new AtomicLong(1);
        when(zk.getSessionTimeout()).thenReturn(600_000);
        when(zk.getSessionId()).thenAnswer(invocation -> sessionId.get());
        when(zk.getCurrentHandleGeneration()).thenReturn(1L);
        List<SessionEvent> events = new CopyOnWriteArrayList<>();

        try (ZKSessionWatcher watcher = new ZKSessionWatcher(zk, events::add, false)) {
            watcher.setSessionReplaced();

            // Publishing a replacement writes the new handle before its generation. During that small window,
            // the source generation alone still matches, so the source session ID must reject H1's event.
            sessionId.set(2);
            watcher.process(new ZKSessionWatcher.SessionEventSnapshot(
                    sessionEvent(KeeperState.SyncConnected), 1, 1, States.CONNECTED));

            assertThat(events).isEmpty();
        }
    }

    @Test
    public void testQueuedTerminalAuthFailedIsProcessedBeforeReplacementConnects() throws Exception {
        List<SessionEvent> events = new CopyOnWriteArrayList<>();
        PulsarZooKeeperClient zk = mock(PulsarZooKeeperClient.class);
        AtomicLong sessionId = new AtomicLong(1);
        AtomicLong handleGeneration = new AtomicLong(1);
        AtomicReference<States> clientState = new AtomicReference<>(States.AUTH_FAILED);
        when(zk.getSessionTimeout()).thenReturn(600_000);
        when(zk.getSessionId()).thenAnswer(invocation -> sessionId.get());
        when(zk.getCurrentHandleGeneration()).thenAnswer(invocation -> handleGeneration.get());
        when(zk.getState()).thenAnswer(invocation -> clientState.get());
        when(zk.getClientConfig()).thenReturn(new ZKClientConfig());
        doAnswer(invocation -> {
            VoidCallback callback = invocation.getArgument(3);
            callback.processResult(KeeperException.Code.OK.intValue(), "/", null);
            return null;
        }).when(zk).addWatch(eq("/"), any(), eq(AddWatchMode.PERSISTENT_RECURSIVE), any(), isNull());
        CountDownLatch eventExecutorBlocked = new CountDownLatch(1);
        CountDownLatch releaseEventExecutor = new CountDownLatch(1);

        try (ZKMetadataStore store = new ZKMetadataStore(zk, false)) {
            store.registerSessionListener(events::add);
            store.getEventExecutor().execute(() -> {
                eventExecutorBlocked.countDown();
                try {
                    releaseEventExecutor.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            assertThat(eventExecutorBlocked.await(5, TimeUnit.SECONDS)).isTrue();

            // Queue H1's terminal failure, then publish H2 before the event executor processes it.
            store.processSessionWatcher(new PulsarZooKeeperClient.HandleStateWatchedEvent(
                    sessionEvent(KeeperState.AuthFailed), 1, 1, States.AUTH_FAILED));
            sessionId.set(2);
            handleGeneration.set(2);
            clientState.set(States.CONNECTED);
            releaseEventExecutor.countDown();

            Awaitility.await().atMost(5, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(events).containsExactly(SessionEvent.SessionLost));

            // Only H2's connection can recover the logical metadata session.
            store.processSessionWatcher(new PulsarZooKeeperClient.HandleStateWatchedEvent(
                    sessionEvent(KeeperState.SyncConnected), 2, 2, States.CONNECTED));
            Awaitility.await().atMost(5, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(events).containsExactly(
                            SessionEvent.SessionLost,
                            SessionEvent.Reconnected,
                            SessionEvent.SessionReestablished));
        } finally {
            releaseEventExecutor.countDown();
        }
    }

    @Test
    public void testTerminalEventFromOlderHandleIsIgnoredAfterReplacementConnects() throws Exception {
        PulsarZooKeeperClient zk = mock(PulsarZooKeeperClient.class);
        AtomicLong sessionId = new AtomicLong(1);
        AtomicLong handleGeneration = new AtomicLong(1);
        when(zk.getSessionTimeout()).thenReturn(600_000);
        when(zk.getSessionId()).thenAnswer(invocation -> sessionId.get());
        when(zk.getCurrentHandleGeneration()).thenAnswer(invocation -> handleGeneration.get());
        List<SessionEvent> events = new CopyOnWriteArrayList<>();

        try (ZKSessionWatcher watcher = new ZKSessionWatcher(zk, events::add, false)) {
            // H2 is published before H1's queued terminal event is processed. H1's session loss still matters
            // until H2's own SyncConnected event reaches this watcher.
            sessionId.set(2);
            handleGeneration.set(2);
            watcher.process(new ZKSessionWatcher.SessionEventSnapshot(
                    sessionEvent(KeeperState.Expired), 1, 1, States.CLOSED));

            watcher.process(new ZKSessionWatcher.SessionEventSnapshot(
                    sessionEvent(KeeperState.SyncConnected), 2, 2, States.CONNECTED));

            // H1 may deliver a duplicate terminal event after H2 has completed logical recovery.
            watcher.process(new ZKSessionWatcher.SessionEventSnapshot(
                    sessionEvent(KeeperState.Expired), 1, 1, States.CLOSED));

            assertThat(events).containsExactly(
                    SessionEvent.SessionLost,
                    SessionEvent.Reconnected,
                    SessionEvent.SessionReestablished);
        }
    }

    @Test
    public void testTerminalEventFromOlderHandleIsIgnoredAfterReplacementProbeReconnects() throws Exception {
        PulsarZooKeeperClient zk = mock(PulsarZooKeeperClient.class);
        AtomicLong sessionId = new AtomicLong(1);
        AtomicLong handleGeneration = new AtomicLong(1);
        when(zk.getSessionTimeout()).thenReturn(600_000);
        when(zk.getSessionId()).thenAnswer(invocation -> sessionId.get());
        when(zk.getCurrentHandleGeneration()).thenAnswer(invocation -> handleGeneration.get());
        when(zk.getCurrentHandleIdentity()).thenAnswer(invocation ->
                new PulsarZooKeeperClient.CurrentHandleIdentity(handleGeneration.get(), sessionId.get()));
        when(zk.runStateUpdateIfCurrentHandle(any(), any())).thenAnswer(invocation -> {
            PulsarZooKeeperClient.CurrentHandleIdentity expectedIdentity = invocation.getArgument(0);
            if (!expectedIdentity.equals(zk.getCurrentHandleIdentity())) {
                return false;
            }
            Runnable action = invocation.getArgument(1);
            action.run();
            return true;
        });
        when(zk.getState()).thenReturn(States.CONNECTED);
        List<SessionEvent> events = new CopyOnWriteArrayList<>();
        completeExistsWith(zk, KeeperException.Code.OK);

        try (ZKSessionWatcher watcher = new ZKSessionWatcher(zk, events::add, false)) {
            watcher.setSessionInvalid();
            sessionId.set(2);
            handleGeneration.set(2);
            watcher.checkConnectionStatus();
            assertThat(events).containsExactly(SessionEvent.Reconnected, SessionEvent.SessionReestablished);

            watcher.process(new ZKSessionWatcher.SessionEventSnapshot(
                    sessionEvent(KeeperState.Expired), 1, 1, States.CLOSED));
            assertThat(events).containsExactly(SessionEvent.Reconnected, SessionEvent.SessionReestablished);
        }
    }

    @Test
    public void testProbeListenerRunsOutsideCurrentHandleUpdate() throws Exception {
        PulsarZooKeeperClient zk = mock(PulsarZooKeeperClient.class);
        PulsarZooKeeperClient.CurrentHandleIdentity handleIdentity =
                new PulsarZooKeeperClient.CurrentHandleIdentity(1, 1);
        AtomicBoolean updatingCurrentHandle = new AtomicBoolean();
        AtomicBoolean listenerCalledDuringUpdate = new AtomicBoolean();
        List<SessionEvent> events = new CopyOnWriteArrayList<>();
        when(zk.getSessionTimeout()).thenReturn(30_000);
        when(zk.getSessionId()).thenReturn(1L);
        when(zk.getCurrentHandleGeneration()).thenReturn(1L);
        when(zk.getCurrentHandleIdentity()).thenReturn(handleIdentity);
        when(zk.runStateUpdateIfCurrentHandle(any(), any())).thenAnswer(invocation -> {
            Runnable stateUpdate = invocation.getArgument(1);
            updatingCurrentHandle.set(true);
            try {
                stateUpdate.run();
            } finally {
                updatingCurrentHandle.set(false);
            }
            return true;
        });
        when(zk.getState()).thenReturn(States.CONNECTING);
        completeExistsWith(zk, KeeperException.Code.CONNECTIONLOSS);

        try (ZKSessionWatcher watcher = new ZKSessionWatcher(zk, event -> {
            listenerCalledDuringUpdate.set(updatingCurrentHandle.get());
            events.add(event);
        }, false)) {
            watcher.checkConnectionStatus();

            assertThat(events).containsExactly(SessionEvent.ConnectionLost);
            assertThat(listenerCalledDuringUpdate).isFalse();
        }
    }

    @Test
    public void testProbeFromOlderHandleIsIgnoredWhenSessionIdsAreUnassigned() throws Exception {
        PulsarZooKeeperClient zk = mock(PulsarZooKeeperClient.class);
        AtomicLong handleGeneration = new AtomicLong(1);
        when(zk.getSessionTimeout()).thenReturn(600_000);
        when(zk.getSessionId()).thenReturn(0L);
        when(zk.getCurrentHandleGeneration()).thenAnswer(invocation -> handleGeneration.get());
        when(zk.getCurrentHandleIdentity()).thenAnswer(invocation ->
                new PulsarZooKeeperClient.CurrentHandleIdentity(handleGeneration.get(), 0));
        when(zk.runStateUpdateIfCurrentHandle(any(), any())).thenAnswer(invocation -> {
            PulsarZooKeeperClient.CurrentHandleIdentity expectedIdentity = invocation.getArgument(0);
            if (!expectedIdentity.equals(zk.getCurrentHandleIdentity())) {
                return false;
            }
            Runnable action = invocation.getArgument(1);
            action.run();
            return true;
        });
        when(zk.getState()).thenReturn(States.CONNECTED);
        List<SessionEvent> events = new CopyOnWriteArrayList<>();
        doAnswer(invocation -> {
            handleGeneration.set(2);
            StatCallback callback = invocation.getArgument(2);
            callback.processResult(KeeperException.Code.OK.intValue(), "/", null, null);
            return null;
        }).when(zk).exists(eq("/"), eq(false), any(StatCallback.class), isNull());

        try (ZKSessionWatcher watcher = new ZKSessionWatcher(zk, events::add, false)) {
            watcher.setSessionInvalid();
            watcher.checkConnectionStatus();

            assertThat(events).isEmpty();
        }
    }

    @Test
    public void testQueuedSyncConnectedFromReplacedSessionIsIgnored() throws Exception {
        List<SessionEvent> events = new CopyOnWriteArrayList<>();
        PulsarZooKeeperClient zk = mock(PulsarZooKeeperClient.class);
        AtomicLong sessionId = new AtomicLong(1);
        AtomicLong handleGeneration = new AtomicLong(1);
        when(zk.getSessionTimeout()).thenReturn(600_000);
        when(zk.getSessionId()).thenAnswer(invocation -> sessionId.get());
        when(zk.getCurrentHandleGeneration()).thenAnswer(invocation -> handleGeneration.get());
        when(zk.getState()).thenReturn(States.CONNECTED);
        when(zk.getClientConfig()).thenReturn(new ZKClientConfig());
        doAnswer(invocation -> {
            VoidCallback callback = invocation.getArgument(3);
            callback.processResult(KeeperException.Code.CONNECTIONLOSS.intValue(), "/", null);
            return null;
        }).when(zk).addWatch(eq("/"), any(), eq(AddWatchMode.PERSISTENT_RECURSIVE), any(), isNull());

        CountDownLatch eventExecutorBlocked = new CountDownLatch(1);
        CountDownLatch releaseEventExecutor = new CountDownLatch(1);
        try (ZKMetadataStore store = new ZKMetadataStore(zk, false)) {
            store.registerSessionListener(events::add);
            store.processSessionWatcher(sessionEvent(KeeperState.Expired));
            Awaitility.await().atMost(5, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(events).containsExactly(SessionEvent.SessionLost));

            // The persistent watch recreation fails, so session 1 is marked as replaced.
            store.processSessionWatcher(new PulsarZooKeeperClient.HandleStateWatchedEvent(
                    sessionEvent(KeeperState.SyncConnected), 1, 1, States.CONNECTED));
            Awaitility.await().atMost(5, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertThat(events).containsExactly(
                            SessionEvent.SessionLost, SessionEvent.Reconnected));

            store.getEventExecutor().execute(() -> {
                eventExecutorBlocked.countDown();
                try {
                    releaseEventExecutor.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            assertThat(eventExecutorBlocked.await(5, TimeUnit.SECONDS)).isTrue();

            // Queue another SyncConnected from session 1, then replace it with session 2 before processing.
            store.processSessionWatcher(new PulsarZooKeeperClient.HandleStateWatchedEvent(
                    sessionEvent(KeeperState.SyncConnected), 1, 1, States.CONNECTED));
            CountDownLatch queuedEventProcessed = new CountDownLatch(1);
            store.getEventExecutor().execute(queuedEventProcessed::countDown);
            sessionId.set(2);
            handleGeneration.set(2);
            releaseEventExecutor.countDown();
            assertThat(queuedEventProcessed.await(5, TimeUnit.SECONDS)).isTrue();

            // Drain notifications that the queued session event might have produced.
            CountDownLatch notificationsProcessed = new CountDownLatch(1);
            store.getEventExecutor().execute(notificationsProcessed::countDown);
            assertThat(notificationsProcessed.await(5, TimeUnit.SECONDS)).isTrue();

            assertThat(events).containsExactly(SessionEvent.SessionLost, SessionEvent.Reconnected);
        } finally {
            releaseEventExecutor.countDown();
        }
    }

    @Test
    public void testConnectedReadOnlyProbeShouldNotBeTreatedAsWritableConnection() throws Exception {
        List<SessionEvent> events = new CopyOnWriteArrayList<>();
        ZooKeeper zk = newSessionZooKeeper();
        when(zk.getState()).thenReturn(States.CONNECTEDREADONLY);
        completeExistsWith(zk, KeeperException.Code.OK);

        try (ZKSessionWatcher watcher = newSessionWatcher(zk, events)) {
            watcher.checkConnectionStatus();
            watcher.checkConnectionStatus();

            assertThat(events).containsExactly(SessionEvent.ConnectionLost);
        }
    }

    @Test
    public void testConnectedReadOnlyThenWritableProbeDoesNotReestablishSession() throws Exception {
        List<SessionEvent> events = new CopyOnWriteArrayList<>();
        ZooKeeper zk = newSessionZooKeeper();
        when(zk.getState()).thenReturn(States.CONNECTEDREADONLY);
        completeExistsWith(zk, KeeperException.Code.OK);

        try (ZKSessionWatcher watcher = newSessionWatcher(zk, events)) {
            watcher.checkConnectionStatus();

            when(zk.getState()).thenReturn(States.CONNECTED);
            watcher.checkConnectionStatus();

            assertThat(events).containsExactly(SessionEvent.ConnectionLost, SessionEvent.Reconnected);
        }
    }

    @Test
    public void testReadOnlyProbesWaitForWritableConnectionWithoutSessionLoss() throws Exception {
        List<SessionEvent> events = new CopyOnWriteArrayList<>();
        ZooKeeper zk = newSessionZooKeeper();
        when(zk.getState()).thenReturn(States.CONNECTEDREADONLY);
        completeExistsWith(zk, KeeperException.Code.OK);

        try (ZKSessionWatcher watcher = newSessionWatcher(zk, events)) {
            watcher.checkConnectionStatus();
            watcher.checkConnectionStatus();

            assertThat(events).containsExactly(SessionEvent.ConnectionLost);

            when(zk.getState()).thenReturn(States.CONNECTED);
            watcher.checkConnectionStatus();
            watcher.process(new WatchedEvent(EventType.None, KeeperState.SyncConnected, null));

            assertThat(events).containsExactly(SessionEvent.ConnectionLost, SessionEvent.Reconnected);
        }
    }

    @Test
    public void testClosedHandleAfterExpirationWaitsForWritableConnectionAndDeduplicatesRecovery() throws Exception {
        List<SessionEvent> events = new CopyOnWriteArrayList<>();
        ZooKeeper zk = newSessionZooKeeper();
        when(zk.getState()).thenReturn(States.CLOSED);
        completeExistsWith(zk, KeeperException.Code.OK);

        try (ZKSessionWatcher watcher = newSessionWatcher(zk, events)) {
            watcher.process(new WatchedEvent(EventType.None, KeeperState.Expired, null));
            watcher.process(new WatchedEvent(EventType.None, KeeperState.Closed, null));
            watcher.process(new WatchedEvent(EventType.None, KeeperState.Closed, null));
            watcher.checkConnectionStatus();
            assertThat(events).containsExactly(SessionEvent.SessionLost);

            when(zk.getState()).thenReturn(States.CONNECTED);
            watcher.checkConnectionStatus();
            watcher.process(new WatchedEvent(EventType.None, KeeperState.SyncConnected, null));
            watcher.checkConnectionStatus();

            assertThat(events).containsExactly(
                    SessionEvent.SessionLost, SessionEvent.Reconnected, SessionEvent.SessionReestablished);
        }
    }

    @Test
    public void testWritableConnectedProbeReestablishesSession() throws Exception {
        List<SessionEvent> events = new CopyOnWriteArrayList<>();
        ZooKeeper zk = newSessionZooKeeper();
        when(zk.getState()).thenReturn(States.CONNECTED);
        completeExistsWith(zk, KeeperException.Code.OK);

        try (ZKSessionWatcher watcher = newSessionWatcher(zk, events)) {
            watcher.setSessionInvalid();
            watcher.checkConnectionStatus();

            assertThat(events).containsExactly(SessionEvent.Reconnected, SessionEvent.SessionReestablished);
        }
    }

    private static ZKSessionWatcher newSessionWatcher(List<SessionEvent> events) {
        return newSessionWatcher(newSessionZooKeeper(), events);
    }

    private static ZKSessionWatcher newSessionWatcher(ZooKeeper zk, List<SessionEvent> events) {
        return new ZKSessionWatcher(zk, events::add, false);
    }

    private static ZooKeeper newSessionZooKeeper() {
        ZooKeeper zk = mock(ZooKeeper.class);
        when(zk.getSessionTimeout()).thenReturn(30_000);
        when(zk.getSessionId()).thenReturn(0x1234L);
        when(zk.getState()).thenReturn(States.CONNECTED);
        when(zk.getClientConfig()).thenReturn(new ZKClientConfig());
        return zk;
    }

    private static void completeExistsWith(ZooKeeper zk, KeeperException.Code code) {
        doAnswer(invocation -> {
            StatCallback callback = invocation.getArgument(2);
            callback.processResult(code.intValue(), "/", null, null);
            return null;
        }).when(zk).exists(eq("/"), eq(false), any(StatCallback.class), isNull());
    }

    private static WatchedEvent sessionEvent(KeeperState state) {
        return new WatchedEvent(EventType.None, state, null);
    }
}
