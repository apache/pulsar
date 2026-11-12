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
package org.apache.pulsar.broker.transaction.coordinator.v5;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Cleanup;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.transaction.metadata.TxnPaths;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.Option;
import org.apache.pulsar.metadata.api.coordination.CoordinationService;
import org.apache.pulsar.metadata.api.extended.SessionEvent;
import org.apache.pulsar.metadata.coordination.impl.CoordinationServiceImpl;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.apache.pulsar.transaction.coordinator.TransactionCoordinatorID;
import org.apache.pulsar.transaction.coordinator.exceptions.CoordinatorException.CoordinatorNotFoundException;
import org.apache.zookeeper.MockZooKeeper;
import org.apache.zookeeper.MockZooKeeperSession;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class TransactionCoordinatorV5SessionTest {
    private static final TransactionCoordinatorID TC_ID = TransactionCoordinatorID.get(0);
    private static final String LEADER_PATH = TxnPaths.tcLeaderPath(0);

    @Test(timeOut = 30000)
    public void handshakeRequiresLeadershipConfirmedInTheCurrentSession() throws Exception {
        @Cleanup("shutdown") MockZooKeeper zk = MockZooKeeper.newInstance();
        try (var sessionA = MockZooKeeperSession.newInstance(zk, false);
             var sessionB = MockZooKeeperSession.newInstance(zk, false);
             var storeA = new GatedMetadataStore(sessionA);
             var storeB = new ZKMetadataStore(sessionB);
             var coordinationA = new CoordinationServiceImpl(storeA);
             var coordinationB = new CoordinationServiceImpl(storeB)) {
            var brokerA = broker("a", storeA, coordinationA);
            var brokerB = broker("b", storeB, coordinationB);
            var lost = new AtomicBoolean();
            var losses = new AtomicLong();
            // PulsarService receives session events before the coordination service.
            @Cleanup("run") Runnable cancel = storeA.registerCancellableSessionListener(event -> {
                if (event == SessionEvent.SessionLost) {
                    losses.incrementAndGet();
                    lost.set(true);
                } else if (event == SessionEvent.SessionReestablished) {
                    lost.set(false);
                }
            });
            when(brokerA.isMetadataSessionLost()).thenAnswer(__ -> lost.get());
            when(brokerA.getMetadataSessionLossCount()).thenAnswer(__ -> losses.get());
            var a = new TransactionCoordinatorV5(brokerA);
            var b = new TransactionCoordinatorV5(brokerB);
            var oldRead = new PausedRead();
            var newRead = new PausedRead();
            var handshakeRead = new PausedRead();
            try {
                a.start();
                await().until(() -> a.isLeaderFor(0));
                a.handleClientConnect(TC_ID).get();

                // Both reads see A's lock. Hold their replies until after A loses its session.
                storeA.nextRead.set(handshakeRead);
                var handshake = a.handleClientConnect(TC_ID);
                assertThat(handshakeRead.result.get(5, TimeUnit.SECONDS).orElseThrow().getStat().isCreatedBySelf())
                        .isTrue();
                storeA.nextRead.set(oldRead);
                storeA.sendSessionEvent(SessionEvent.Reconnected);
                assertThat(oldRead.result.get(5, TimeUnit.SECONDS).orElseThrow().getStat().isCreatedBySelf()).isTrue();
                storeA.sendSessionEvent(SessionEvent.SessionLost);
                await().untilTrue(lost);
                sessionA.close();
                sessionA.setSessionId(sessionB.getSessionId() + 1L);
                b.start();
                await().until(() -> b.isLeaderFor(0));
                assertThat(storeB.get(LEADER_PATH).get().orElseThrow().getStat().isCreatedBySelf()).isTrue();

                storeA.nextRead.set(newRead);
                storeA.sendSessionEvent(SessionEvent.SessionReestablished);
                await().untilFalse(lost);
                assertThat(a.isLeaderFor(0)).isTrue();
                handshakeRead.resume.complete(null);
                assertThatThrownBy(() -> handshake.get(1, TimeUnit.SECONDS))
                        .hasCauseInstanceOf(CoordinatorNotFoundException.class);

                // The previous session's positive read completes. Revalidation of the new session
                // has read B's lock but has not delivered the result to A's election yet.
                oldRead.resume.complete(null);
                assertThat(newRead.result.get(5, TimeUnit.SECONDS).orElseThrow().getStat().isCreatedBySelf()).isFalse();
                assertThat(a.isLeaderFor(0)).isTrue();
                b.handleClientConnect(TC_ID).get();
                assertRejected(a);

                newRead.resume.complete(null);
                await().until(() -> !a.isLeaderFor(0));
                assertRejected(a);
                b.handleClientConnect(TC_ID).get();

                b.close();
                await().until(() -> a.isLeaderFor(0));
                assertThat(storeA.get(LEADER_PATH).get().orElseThrow().getStat().isCreatedBySelf()).isTrue();
                a.handleClientConnect(TC_ID).get();
            } finally {
                handshakeRead.resume.complete(null);
                oldRead.resume.complete(null);
                newRead.resume.complete(null);
                a.close();
                b.close();
                await().until(() -> storeB.get(LEADER_PATH).get().isEmpty());
            }
        }
    }

    @Test(timeOut = 30000)
    public void handshakeAcceptsLockOwnerAfterFailedSessionRevalidation() throws Exception {
        @Cleanup("shutdown") MockZooKeeper zk = MockZooKeeper.newInstance();
        try (var session = MockZooKeeperSession.newInstance(zk, false);
             var observerSession = MockZooKeeperSession.newInstance(zk, false);
             var store = new GatedMetadataStore(session);
             var observer = new ZKMetadataStore(observerSession);
             var coordination = new CoordinationServiceImpl(store)) {
            var tc = new TransactionCoordinatorV5(broker("a", store, coordination));
            var failedRead = new PausedRead();
            try {
                tc.start();
                await().until(() -> tc.isLeaderFor(0));
                tc.handleClientConnect(TC_ID).get();

                store.sendSessionEvent(SessionEvent.SessionLost);
                session.close();
                session.setSessionId(observerSession.getSessionId() + 1);
                // Reconnected lets the replacement session acquire the node before SessionReestablished.
                store.sendSessionEvent(SessionEvent.Reconnected);
                await().until(() -> observer.get(LEADER_PATH).get().isPresent());
                assertThat(store.get(LEADER_PATH).get().orElseThrow().getStat().isCreatedBySelf()).isTrue();

                store.nextRead.set(failedRead);
                store.sendSessionEvent(SessionEvent.SessionReestablished);
                failedRead.result.get(5, TimeUnit.SECONDS);
                failedRead.resume.completeExceptionally(new MetadataStoreException("injected read failure"));
                // The handshake can read the lock even though the election's own read failed.
                tc.handleClientConnect(TC_ID).get(1, TimeUnit.SECONDS);
            } finally {
                failedRead.resume.complete(null);
                tc.close();
                await().until(() -> observer.get(LEADER_PATH).get().isEmpty());
            }
        }
    }

    private static PulsarService broker(String name, ZKMetadataStore store, CoordinationService coordination) {
        var broker = mock(PulsarService.class);
        var config = new ServiceConfiguration();
        config.setTransactionCoordinatorScalableTopicsParallelism(1);
        when(broker.getConfiguration()).thenReturn(config);
        when(broker.getBrokerId()).thenReturn(name);
        when(broker.getBrokerServiceUrl()).thenReturn("pulsar://" + name + ":6650");
        when(broker.getLocalMetadataStore()).thenReturn(store);
        when(broker.getCoordinationService()).thenReturn(coordination);
        return broker;
    }

    private static void assertRejected(TransactionCoordinatorV5 tc) {
        assertThatThrownBy(() -> tc.handleClientConnect(TC_ID).get(1, TimeUnit.SECONDS))
                .hasCauseInstanceOf(CoordinatorNotFoundException.class);
    }

    private static final class PausedRead {
        private final CompletableFuture<Optional<GetResult>> result = new CompletableFuture<>();
        private final CompletableFuture<Void> resume = new CompletableFuture<>();
    }

    private static final class GatedMetadataStore extends ZKMetadataStore {
        private final AtomicReference<PausedRead> nextRead = new AtomicReference<>();

        GatedMetadataStore(MockZooKeeperSession session) {
            super(session);
        }

        @Override
        public CompletableFuture<Optional<GetResult>> get(String path, Set<Option> opts) {
            var result = super.get(path, opts);
            PausedRead pause = LEADER_PATH.equals(path) ? nextRead.getAndSet(null) : null;
            if (pause != null) {
                FutureUtil.completeAfter(pause.result, result);
                return result.thenCombine(pause.resume, (value, __) -> value);
            }
            return result;
        }

        void sendSessionEvent(SessionEvent event) {
            receivedSessionEvent(event);
        }
    }
}
