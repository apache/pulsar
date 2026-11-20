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
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pulsar.metadata.api.extended.SessionEvent;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper.States;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ZKSessionWatcherProbeTimeoutTest {
    @DataProvider
    public Object[][] booleanValues() {
        return new Object[][]{{true}, {false}};
    }

    @Test(dataProvider = "booleanValues")
    public void testSilentProbesRetainDeadlineWhenDisconnectionIsConfirmed(boolean useEvent) throws Exception {
        try (ProbeContext context = new ProbeContext()) {
            context.probe();
            context.advanceTime(500);
            context.probe();
            assertThat(context.events).isEmpty();

            context.advanceTime(500);
            context.state.set(States.CONNECTING);
            if (useEvent) {
                context.watcher.process(sessionEvent(KeeperState.Disconnected));
            } else {
                context.probe();
            }
            assertThat(context.events).containsExactly(SessionEvent.ConnectionLost);

            context.advanceTime(251);
            context.probe();
            assertThat(context.events).containsExactly(SessionEvent.ConnectionLost, SessionEvent.SessionLost);
        }
    }

    @Test(dataProvider = "booleanValues")
    public void testRecoveryClearsSilentProbeTimeBeforeUnrelatedDisconnect(boolean useProbeResponse) throws Exception {
        try (ProbeContext context = new ProbeContext()) {
            StatCallback callback = context.probe();
            context.advanceTime(1_300);
            if (useProbeResponse) {
                context.reply(callback);
            } else {
                context.watcher.process(sessionEvent(KeeperState.SyncConnected));
            }
            assertThat(context.events).isEmpty();

            context.disconnect();
            assertThat(context.events).containsExactly(SessionEvent.ConnectionLost);
            context.advanceTime(1_251);
            context.probe();
            assertThat(context.events).containsExactly(SessionEvent.ConnectionLost, SessionEvent.SessionLost);
        }
    }

    @Test
    public void testOldCallbackDoesNotClearNewerUnansweredProbeTime() throws Exception {
        try (ProbeContext context = new ProbeContext()) {
            StatCallback oldCallback = context.probe();
            context.watcher.process(sessionEvent(KeeperState.SyncConnected));
            context.probe();
            context.advanceTime(1_000);
            context.reply(oldCallback);
            assertThat(context.events).isEmpty();

            context.disconnect();
            assertThat(context.events).containsExactly(SessionEvent.ConnectionLost);
            context.advanceTime(251);
            context.probe();
            assertThat(context.events).containsExactly(SessionEvent.ConnectionLost, SessionEvent.SessionLost);
        }
    }

    @DataProvider
    public Object[][] replacementSessionIds() {
        return new Object[][]{{11L, 22L}, {0L, 0L}};
    }

    @Test(dataProvider = "replacementSessionIds")
    public void testReplacementDiscardsOldTimingAndIgnoresOldCallback(long oldSessionId, long newSessionId)
            throws Exception {
        try (ProbeContext context = new ProbeContext(oldSessionId)) {
            StatCallback oldCallback = context.probe();
            context.advanceTime(1_300);
            context.identity.set(new PulsarZooKeeperClient.CurrentHandleIdentity(2, newSessionId));
            context.probe();
            context.advanceTime(1_000);
            context.reply(oldCallback);

            context.disconnect();
            assertThat(context.events).containsExactly(SessionEvent.ConnectionLost);
            context.advanceTime(251);
            context.probe();
            assertThat(context.events).containsExactly(SessionEvent.ConnectionLost, SessionEvent.SessionLost);
        }
    }

    @Test(dataProvider = "booleanValues")
    public void testUnansweredProbesKeepConfirmedDeadline(boolean closed) throws Exception {
        try (ProbeContext context = new ProbeContext()) {
            context.state.set(States.CONNECTING);
            context.probe();
            assertThat(context.events).containsExactly(SessionEvent.ConnectionLost);

            context.state.set(closed ? States.CLOSED : States.CONNECTED);
            context.advanceTime(1_251);
            context.probe();
            context.probe();
            assertThat(context.events).containsExactly(SessionEvent.ConnectionLost, SessionEvent.SessionLost);
        }
    }

    @Test
    public void testTimeoutAfterRecoveryEventDoesNotStartAnOldDeadline() throws Exception {
        try (ProbeContext context = new ProbeContext()) {
            doAnswer(invocation -> {
                context.watcher.process(sessionEvent(KeeperState.SyncConnected));
                return null;
            }).when(context.zk).exists(eq("/"), eq(false), any(StatCallback.class), isNull());
            context.watcher.checkConnectionStatus();
            context.advanceTime(1_300);

            context.disconnect();
            assertThat(context.events).containsExactly(SessionEvent.ConnectionLost);
        }
    }

    @Test
    public void testClosingWatcherStopsConfirmedDeadline() throws Exception {
        try (ProbeContext context = new ProbeContext()) {
            context.disconnect();
            context.watcher.close();
            context.state.set(States.CLOSED);
            context.advanceTime(1_251);
            context.watcher.checkConnectionStatus();

            assertThat(context.events).containsExactly(SessionEvent.ConnectionLost);
            context.watcher.process(sessionEvent(KeeperState.SyncConnected));
            assertThat(context.events).containsExactly(SessionEvent.ConnectionLost);
        }
    }

    private static WatchedEvent sessionEvent(KeeperState state) {
        return new WatchedEvent(EventType.None, state, null);
    }

    private static class ProbeContext implements AutoCloseable {
        private final PulsarZooKeeperClient zk = mock(PulsarZooKeeperClient.class);
        private final AtomicReference<PulsarZooKeeperClient.CurrentHandleIdentity> identity;
        private final AtomicReference<States> state = new AtomicReference<>(States.CONNECTED);
        private final AtomicLong nanoTime = new AtomicLong(TimeUnit.SECONDS.toNanos(1));
        private final List<SessionEvent> events = new CopyOnWriteArrayList<>();
        private final List<StatCallback> callbacks = new CopyOnWriteArrayList<>();
        private final ZKSessionWatcher watcher;

        ProbeContext() {
            this(11);
        }

        ProbeContext(long sessionId) {
            identity = new AtomicReference<>(new PulsarZooKeeperClient.CurrentHandleIdentity(1, sessionId));
            when(zk.getSessionTimeout()).thenReturn(1_500);
            when(zk.getSessionId()).thenAnswer(invocation -> identity.get().sessionId());
            when(zk.getCurrentHandleGeneration()).thenAnswer(invocation -> identity.get().generation());
            when(zk.getCurrentHandleIdentity()).thenAnswer(invocation -> identity.get());
            when(zk.getState()).thenAnswer(invocation -> state.get());
            when(zk.runStateUpdateIfCurrentHandle(any(), any())).thenAnswer(invocation -> {
                if (!identity.get().equals(invocation.getArgument(0))) {
                    return false;
                }
                Runnable action = invocation.getArgument(1);
                action.run();
                return true;
            });
            doAnswer(invocation -> {
                callbacks.add(invocation.getArgument(2));
                return null;
            }).when(zk).exists(eq("/"), eq(false), any(StatCallback.class), isNull());
            watcher = new ZKSessionWatcher(zk, events::add, false, nanoTime::get);
        }

        StatCallback probe() {
            watcher.checkConnectionStatus();
            return callbacks.get(callbacks.size() - 1);
        }

        void advanceTime(long millis) {
            nanoTime.addAndGet(TimeUnit.MILLISECONDS.toNanos(millis));
        }

        void reply(StatCallback callback) {
            callback.processResult(KeeperException.Code.OK.intValue(), "/", null, null);
        }

        void disconnect() {
            state.set(States.CONNECTING);
            watcher.process(sessionEvent(KeeperState.Disconnected));
        }

        @Override
        public void close() throws Exception {
            watcher.close();
        }
    }
}
