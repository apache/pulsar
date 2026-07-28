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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.pulsar.metadata.api.extended.SessionEvent;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
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

    private static ZKSessionWatcher newSessionWatcher(List<SessionEvent> events) {
        ZooKeeper zk = mock(ZooKeeper.class);
        when(zk.getSessionTimeout()).thenReturn(30_000);
        when(zk.getSessionId()).thenReturn(0x1234L);
        return new ZKSessionWatcher(zk, events::add);
    }
}
