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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Cleanup;
import org.apache.pulsar.metadata.api.extended.SessionEvent;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.testng.annotations.Test;

public class ZKSessionWatcherTest {

    /**
     * When ZKMetadataStore can't use a reestablished session, such as when it fails to recreate its persistent watch,
     * it marks the session replaced while it's notified and makes PulsarZooKeeperClient replace the session. The new
     * session must send SessionReestablished again, so that the listeners, such as the lock managers that revalidate
     * their locks, get it, and the replaced session's own events must not reestablish it first.
     */
    @Test(timeOut = 30_000)
    public void sessionReplacedWhileNotifiedIsReestablishedWithNextSession() throws Exception {
        ZooKeeper zk = mock(ZooKeeper.class);
        // a long session timeout, so that the periodic connection check doesn't run during the test
        when(zk.getSessionTimeout()).thenReturn(600_000);
        AtomicLong sessionId = new AtomicLong(1);
        when(zk.getSessionId()).thenAnswer(invocation -> sessionId.get());
        List<SessionEvent> events = new CopyOnWriteArrayList<>();
        AtomicReference<ZKSessionWatcher> watcher = new AtomicReference<>();
        AtomicBoolean replaceOnce = new AtomicBoolean(true);
        @Cleanup
        ZKSessionWatcher sessionWatcher = new ZKSessionWatcher(zk, event -> {
            events.add(event);
            if (event == SessionEvent.SessionReestablished && replaceOnce.getAndSet(false)) {
                watcher.get().setSessionReplaced();
            }
        });
        watcher.set(sessionWatcher);

        // the session expired, and PulsarZooKeeperClient replaced it with session 1, closing the previous handle
        sessionWatcher.process(sessionEvent(Watcher.Event.KeeperState.Expired));
        sessionWatcher.process(sessionEvent(Watcher.Event.KeeperState.Closed));
        assertThat(events).containsExactly(SessionEvent.SessionLost);
        sessionWatcher.process(sessionEvent(Watcher.Event.KeeperState.SyncConnected));
        assertThat(events).containsExactly(SessionEvent.SessionLost, SessionEvent.Reconnected,
                SessionEvent.SessionReestablished);

        // The listener marked session 1 replaced. A connection check of session 1 that still succeeds, and the
        // Closed event of its handle, don't reestablish it.
        sessionWatcher.process(sessionEvent(Watcher.Event.KeeperState.SyncConnected));
        sessionWatcher.process(sessionEvent(Watcher.Event.KeeperState.Closed));
        assertThat(events).containsExactly(SessionEvent.SessionLost, SessionEvent.Reconnected,
                SessionEvent.SessionReestablished);

        // session 2 connects
        sessionId.set(2);
        sessionWatcher.process(sessionEvent(Watcher.Event.KeeperState.SyncConnected));
        assertThat(events).containsExactly(SessionEvent.SessionLost, SessionEvent.Reconnected,
                SessionEvent.SessionReestablished, SessionEvent.Reconnected, SessionEvent.SessionReestablished);
    }

    private static WatchedEvent sessionEvent(Watcher.Event.KeeperState state) {
        return new WatchedEvent(Watcher.Event.EventType.None, state, null);
    }
}
