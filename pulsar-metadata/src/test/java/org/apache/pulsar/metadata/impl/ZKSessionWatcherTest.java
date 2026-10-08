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
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.pulsar.metadata.api.extended.SessionEvent;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
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
    public void testAuthFailedProbeShouldNotBeTreatedAsReconnected() throws Exception {
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
    public void testAuthFailedStateFromSuccessfulProbeShouldNotBeTreatedAsReconnected() throws Exception {
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
        when(zk.getState()).thenReturn(States.CONNECTEDREADONLY, States.CONNECTED);
        completeExistsWith(zk, KeeperException.Code.OK);

        try (ZKSessionWatcher watcher = newSessionWatcher(zk, events)) {
            watcher.checkConnectionStatus();
            watcher.checkConnectionStatus();

            assertThat(events).containsExactly(SessionEvent.ConnectionLost, SessionEvent.Reconnected);
        }
    }

    @Test
    public void testConnectedReadOnlyShouldNotBecomeSessionLostWithoutExpiredEvent() throws Exception {
        List<SessionEvent> events = new CopyOnWriteArrayList<>();
        ZooKeeper zk = newSessionZooKeeper(120);
        when(zk.getState()).thenReturn(States.CONNECTEDREADONLY);

        ZKSessionWatcher watcher = newSessionWatcher(zk, events);
        watcher.close();
        watcher.process(new WatchedEvent(EventType.None, KeeperState.Disconnected, null));
        long timeoutDeadline = System.nanoTime() + Duration.ofMillis(120).toNanos();
        Awaitility.await().atMost(Duration.ofSeconds(1))
                .until(() -> System.nanoTime() >= timeoutDeadline);
        watcher.process(new WatchedEvent(EventType.None, KeeperState.ConnectedReadOnly, null));

        assertThat(events).containsExactly(SessionEvent.ConnectionLost);
    }

    @Test
    public void testClosedProbeWaitsForWritableConnectionBeforeReestablishingSession() throws Exception {
        List<SessionEvent> events = new CopyOnWriteArrayList<>();
        ZooKeeper zk = newSessionZooKeeper();
        when(zk.getState()).thenReturn(States.CLOSED, States.CONNECTED);
        completeExistsWith(zk, KeeperException.Code.OK);

        try (ZKSessionWatcher watcher = newSessionWatcher(zk, events)) {
            watcher.setSessionInvalid();
            watcher.checkConnectionStatus();
            assertThat(events).isEmpty();

            watcher.checkConnectionStatus();
            assertThat(events).containsExactly(SessionEvent.Reconnected, SessionEvent.SessionReestablished);
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
        return new ZKSessionWatcher(zk, events::add);
    }

    private static ZooKeeper newSessionZooKeeper() {
        return newSessionZooKeeper(30_000);
    }

    private static ZooKeeper newSessionZooKeeper(int sessionTimeoutMillis) {
        ZooKeeper zk = mock(ZooKeeper.class);
        when(zk.getSessionTimeout()).thenReturn(sessionTimeoutMillis);
        when(zk.getSessionId()).thenReturn(0x1234L);
        when(zk.getState()).thenReturn(States.CONNECTED);
        return zk;
    }

    private static void completeExistsWith(ZooKeeper zk, KeeperException.Code code) {
        doAnswer(invocation -> {
            StatCallback callback = invocation.getArgument(2);
            callback.processResult(code.intValue(), "/", null, null);
            return null;
        }).when(zk).exists(eq("/"), eq(false), any(StatCallback.class), isNull());
    }
}
