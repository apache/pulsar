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
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Cleanup;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.pulsar.metadata.BaseMetadataStoreTest;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

public class PulsarZooKeeperClientTest extends BaseMetadataStoreTest {

    @Test
    public void testRunStateUpdateIfCurrentHandleRejectsReplacedHandleIdentity() throws Exception {
        @Cleanup
        PulsarZooKeeperClient zkClient = newClient();
        PulsarZooKeeperClient.CurrentHandleIdentity initialIdentity = zkClient.getCurrentHandleIdentity();
        AtomicInteger actionRuns = new AtomicInteger();

        assertThat(zkClient.runStateUpdateIfCurrentHandle(initialIdentity, actionRuns::incrementAndGet)).isTrue();
        assertThat(actionRuns).hasValue(1);

        zkClient.process(sessionEvent(KeeperState.Expired));
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(() -> zkClient.getCurrentHandleGeneration() > initialIdentity.generation()
                        && zkClient.getZooKeeperHandle().getState() == States.CONNECTED
                        && !zkClient.isReconnectInProgress());

        assertThat(zkClient.runStateUpdateIfCurrentHandle(initialIdentity, actionRuns::incrementAndGet)).isFalse();
        assertThat(actionRuns).hasValue(1);
    }

    @Test
    public void testTerminalAuthFailedRecreatesZookeeperHandle() throws Exception {
        @Cleanup
        PulsarZooKeeperClient zkClient = newClient();

        zkClient.process(sessionEvent(KeeperState.Expired));
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(() -> zkClient.getZooKeeperHandle() != null
                        && zkClient.getZooKeeperHandle().getState() == States.CONNECTED
                        && !zkClient.isReconnectInProgress());
        ZooKeeper previousZooKeeper = zkClient.getZooKeeperHandle();
        ZooKeeper failedZooKeeper = mock(ZooKeeper.class);
        when(failedZooKeeper.getState()).thenReturn(States.AUTH_FAILED);
        when(failedZooKeeper.getSessionId()).thenReturn(previousZooKeeper.getSessionId());
        zkClient.setZooKeeperHandle(failedZooKeeper);
        previousZooKeeper.close();

        zkClient.process(sessionEvent(KeeperState.AuthFailed));

        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(() -> zkClient.getZooKeeperHandle() != failedZooKeeper
                        && zkClient.getZooKeeperHandle().getState() == States.CONNECTED
                        && !zkClient.isReconnectInProgress());
    }

    @Test
    public void testInvalidationWhileReconnectCompletesIsNotDropped() throws Exception {
        CountDownLatch firstReconnectCompleted = new CountDownLatch(1);
        CountDownLatch releaseFirstReconnect = new CountDownLatch(1);
        StatsLogger statsLogger = mock(StatsLogger.class, RETURNS_DEEP_STUBS);
        OpStatsLogger createClientStats = mock(OpStatsLogger.class);
        when(statsLogger.scope("zk").getOpStatsLogger("create_client")).thenReturn(createClientStats);
        doAnswer(invocation -> {
            firstReconnectCompleted.countDown();
            releaseFirstReconnect.await();
            return null;
        }).when(createClientStats).registerSuccessfulEvent(anyLong(), eq(TimeUnit.MICROSECONDS));

        @Cleanup
        PulsarZooKeeperClient zkClient = PulsarZooKeeperClient.newBuilder()
                .connectString(zks.getConnectionString())
                .sessionTimeoutMs(3000)
                .statsLogger(statsLogger)
                .build();

        ZooKeeper firstReplacement;
        try {
            zkClient.process(sessionEvent(KeeperState.Expired));
            assertThat(firstReconnectCompleted.await(5, TimeUnit.SECONDS)).isTrue();
            firstReplacement = zkClient.getZooKeeperHandle();
            assertThat(firstReplacement.getState()).isEqualTo(States.CONNECTED);
            zkClient.process(sessionEvent(KeeperState.Expired));
        } finally {
            releaseFirstReconnect.countDown();
        }

        Awaitility.await().atMost(10, TimeUnit.SECONDS)
                .until(() -> zkClient.getZooKeeperHandle() != firstReplacement
                        && zkClient.getZooKeeperHandle().getState() == States.CONNECTED
                        && !zkClient.isReconnectInProgress());
    }

    @Test
    public void testDuplicateInvalidationDoesNotReplaceRecoveredHandleAgain() throws Exception {
        CountDownLatch firstReconnectCompleted = new CountDownLatch(1);
        CountDownLatch releaseFirstReconnect = new CountDownLatch(1);
        StatsLogger statsLogger = mock(StatsLogger.class, RETURNS_DEEP_STUBS);
        OpStatsLogger createClientStats = mock(OpStatsLogger.class);
        when(statsLogger.scope("zk").getOpStatsLogger("create_client")).thenReturn(createClientStats);
        doAnswer(invocation -> {
            firstReconnectCompleted.countDown();
            releaseFirstReconnect.await();
            return null;
        }).when(createClientStats).registerSuccessfulEvent(anyLong(), eq(TimeUnit.MICROSECONDS));

        @Cleanup
        PulsarZooKeeperClient zkClient = PulsarZooKeeperClient.newBuilder()
                .connectString(zks.getConnectionString())
                .sessionTimeoutMs(3000)
                .statsLogger(statsLogger)
                .build();
        WatchedEvent invalidation = sessionEvent(KeeperState.Expired);

        ZooKeeper firstReplacement;
        try {
            zkClient.getInitialHandleWatcher().process(invalidation);
            assertThat(firstReconnectCompleted.await(5, TimeUnit.SECONDS)).isTrue();
            firstReplacement = zkClient.getZooKeeperHandle();
            assertThat(firstReplacement.getState()).isEqualTo(States.CONNECTED);

            // The same terminal event from the original handle is delivered again before reconnect bookkeeping
            // is cleared. It must not be interpreted as an invalidation of firstReplacement.
            zkClient.getInitialHandleWatcher().process(invalidation);
        } finally {
            releaseFirstReconnect.countDown();
        }

        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(() -> !zkClient.isReconnectInProgress());
        assertThat(zkClient.getZooKeeperHandle()).isSameAs(firstReplacement);
        assertThat(firstReplacement.getState()).isEqualTo(States.CONNECTED);
    }

    @Test
    public void testBlockingChildWatcherDoesNotRejectConnectedReplacement() throws Exception {
        CountDownLatch childWatcherEntered = new CountDownLatch(1);
        CountDownLatch releaseChildWatcher = new CountDownLatch(1);
        AtomicBoolean blockFirstReplacement = new AtomicBoolean(true);
        Watcher blockingChildWatcher = event -> {
            if (event instanceof PulsarZooKeeperClient.HandleStateWatchedEvent handleStateEvent
                    && handleStateEvent.getHandleGeneration() > 0
                    && event.getState() == KeeperState.SyncConnected
                    && blockFirstReplacement.compareAndSet(true, false)) {
                childWatcherEntered.countDown();
                try {
                    releaseChildWatcher.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        };

        @Cleanup
        PulsarZooKeeperClient zkClient = PulsarZooKeeperClient.newBuilder()
                .connectString(zks.getConnectionString())
                .sessionTimeoutMs(1000)
                .watchers(Collections.singleton(blockingChildWatcher))
                .build();

        ZooKeeper connectedReplacement;
        try {
            zkClient.process(sessionEvent(KeeperState.Expired));
            assertThat(childWatcherEntered.await(5, TimeUnit.SECONDS)).isTrue();
            connectedReplacement = zkClient.getZooKeeperHandle();
            assertThat(connectedReplacement.getState()).isEqualTo(States.CONNECTED);

            Awaitility.await().atMost(5, TimeUnit.SECONDS)
                    .until(() -> !zkClient.isReconnectInProgress());
            assertThat(zkClient.getZooKeeperHandle()).isSameAs(connectedReplacement);
        } finally {
            releaseChildWatcher.countDown();
        }
    }

    @Test
    public void testTerminalAuthFailedWaitsForReplacementConnection() throws Exception {
        @Cleanup
        PulsarZooKeeperClient zkClient = newClient();
        zkClient.process(sessionEvent(KeeperState.Expired));
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(() -> zkClient.getZooKeeperHandle() != null
                        && zkClient.getZooKeeperHandle().getState() == States.CONNECTED
                        && !zkClient.isReconnectInProgress());
        ZooKeeper previousZooKeeper = zkClient.getZooKeeperHandle();
        ZooKeeper failedZooKeeper = mock(ZooKeeper.class);
        when(failedZooKeeper.getState()).thenReturn(States.AUTH_FAILED);
        zkClient.setZooKeeperHandle(failedZooKeeper);
        previousZooKeeper.close();

        zks.stop();
        try {
            zkClient.process(sessionEvent(KeeperState.AuthFailed));
            Awaitility.await().atMost(5, TimeUnit.SECONDS)
                    .until(() -> zkClient.getZooKeeperHandle() != failedZooKeeper);
            Awaitility.await().during(Duration.ofMillis(500)).atMost(Duration.ofSeconds(2))
                    .until(zkClient::isReconnectInProgress);
        } finally {
            zks.start();
        }

        Awaitility.await().atMost(15, TimeUnit.SECONDS)
                .until(() -> zkClient.getZooKeeperHandle().getState() == States.CONNECTED
                        && !zkClient.isReconnectInProgress());
    }

    @Test
    public void testCloseStopsReconnectWhileReplacementIsConnecting() throws Exception {
        PulsarZooKeeperClient zkClient = newClient();

        zks.stop();
        try {
            zkClient.process(sessionEvent(KeeperState.Expired));
            Awaitility.await().atMost(5, TimeUnit.SECONDS)
                    .until(() -> zkClient.getZooKeeperHandle() != null
                            && zkClient.isReconnectInProgress());
            ZooKeeper replacementZooKeeper = zkClient.getZooKeeperHandle();

            zkClient.close();

            Awaitility.await().during(Duration.ofMillis(500)).atMost(Duration.ofSeconds(2))
                    .until(() -> zkClient.getZooKeeperHandle() == replacementZooKeeper
                            && replacementZooKeeper.getState() == States.CLOSED
                            && !zkClient.isReconnectInProgress());
        } finally {
            zkClient.close();
            zks.start();
        }
    }

    private PulsarZooKeeperClient newClient() throws Exception {
        return PulsarZooKeeperClient.newBuilder()
                .connectString(zks.getConnectionString())
                .sessionTimeoutMs(3000)
                .build();
    }

    private static WatchedEvent sessionEvent(KeeperState state) {
        return new WatchedEvent(EventType.None, state, null);
    }
}
