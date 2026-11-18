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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pulsar.metadata.TestZKServer;
import org.apache.pulsar.metadata.api.HasPublicationId;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.Option.VerifyPublication;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.MultiCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.MockZooKeeper;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.ZooKeeper;
import org.testng.annotations.Test;

public class ZKMetadataStoreTest {

    private static final class Value implements HasPublicationId {
        private String id;
        private final String payload;

        Value(String payload) {
            this.payload = payload;
            setPublication(UUID.randomUUID().toString());
        }

        @Override
        public String getPublication() {
            return id;
        }

        @Override
        public Value setPublication(String publication) {
            this.id = publication;
            return this;
        }

        byte[] encode() {
            return (id + ":" + payload).getBytes(StandardCharsets.UTF_8);
        }

        CompletableFuture<Stat> writeTo(MetadataStore store, String path, long expectedVersion) {
            return store.put(path, encode(), Optional.of(expectedVersion), Set.of(VerifyPublication.INSTANCE));
        }
    }

    private static void loseNextMultiResponse(ZooKeeper client, ZooKeeper server) {
        doAnswer(invocation -> {
            Iterable<Op> ops = invocation.getArgument(0);
            MultiCallback callback = invocation.getArgument(1);
            server.multi(ops, (rc, path, ctx, results) ->
                    callback.processResult(Code.CONNECTIONLOSS.intValue(), path, ctx, null), invocation.getArgument(2));
            return null;
        }).doAnswer(delegatesTo(server)).when(client).multi(any(), any(MultiCallback.class), any());
    }

    @Test(timeOut = 30_000)
    public void confirmOnlyMatchingPublicationsAtTheExpectedNextVersion() throws Exception {
        try (MockZooKeeper server = MockZooKeeper.newInstance()) {
            ZooKeeper client = mock(ZooKeeper.class, delegatesTo(server));
            try (ZKMetadataStore store = new ZKMetadataStore(client, false)) {
                String path = "/publication";
                Value create = new Value("payload");
                loseNextMultiResponse(client, server);
                var created = create.writeTo(store, path, -1).join();
                assertThat(created.getVersion()).isZero();
                assertThat(created).isEqualTo(store.get(path).join().orElseThrow().getStat());
                assertThat(create.writeTo(store, path, -1).join().getVersion()).isZero();

                Value update = new Value("payload");
                byte[] second = update.encode();
                loseNextMultiResponse(client, server);
                assertThat(update.writeTo(store, path, 0).join().getVersion()).isOne();
                // Replaying the same publication can also be confirmed without observing a connection loss.
                assertThat(update.writeTo(store, path, 0).join().getVersion()).isOne();

                // A different write based on version 0 loses to the first update, even in the same session.
                Value competitor = new Value("payload");
                assertThatThrownBy(() -> competitor.writeTo(store, path, 0).join())
                        .hasCauseInstanceOf(MetadataStoreException.BadVersionException.class);
                loseNextMultiResponse(client, server);
                assertThatThrownBy(() -> competitor.writeTo(store, path, 0).join())
                        .hasCauseInstanceOf(MetadataStoreException.BadVersionException.class);

                // An ordinary CAS with exactly the winning bytes must still report the conflict.
                loseNextMultiResponse(client, server);
                assertThatThrownBy(() -> store.put(path, second, Optional.of(0L)).join())
                        .hasCauseInstanceOf(MetadataStoreException.BadVersionException.class);
                Value changedPayload = new Value("other").setPublication(update.getPublication());
                loseNextMultiResponse(client, server);
                assertThatThrownBy(() -> changedPayload.writeTo(store, path, 0).join())
                        .hasCauseInstanceOf(MetadataStoreException.BadVersionException.class);
                // A later version can retain the same bytes, for example if a legacy client copied the record.
                store.put(path, second, Optional.of(1L)).join();
                loseNextMultiResponse(client, server);
                assertThatThrownBy(() -> update.writeTo(store, path, 0).join())
                        .hasCauseInstanceOf(MetadataStoreException.BadVersionException.class);
                assertThat(store.get(path).join().orElseThrow().getValue()).isEqualTo(second);
                // Without a publication ID, even identical bytes at the expected next version cannot confirm a write.
                byte[] withoutId = "legacy".getBytes(StandardCharsets.UTF_8);
                store.put(path, withoutId, Optional.of(2L)).join();
                loseNextMultiResponse(client, server);
                assertThatThrownBy(() -> store.put(path, withoutId, Optional.of(2L)).join())
                        .hasCauseInstanceOf(MetadataStoreException.BadVersionException.class);
            }
        }
    }

    @Test(timeOut = 30_000)
    public void confirmIndividualCreateAndUpdateAfterTheirRepliesAreLost() throws Exception {
        var config = MetadataStoreConfig.builder().batchingMaxOperations(2).batchingMaxDelayMillis(60_000).build();
        try (var server = new TestZKServer();
             var connected = new ZKMetadataStore(server.getConnectionString(), MetadataStoreConfig.builder().build(),
                     false)) {
            server.setMultiReadLimits(100, 64 * 1024);
            String path = "/publication";
            ZooKeeper zk = connected.getZkClient();
            ZooKeeper client = mock(ZooKeeper.class, delegatesTo(zk));
            connected.put("/existing", new byte[0], Optional.of(-1L)).join();
            var committed = new AtomicInteger();
            doAnswer(inv -> {
                StringCallback callback = inv.getArgument(4);
                zk.create(path, inv.getArgument(1), inv.getArgument(2), inv.getArgument(3), (rc, p, ctx, name) -> {
                    if (rc == Code.OK.intValue()) {
                        committed.incrementAndGet();
                        rc = Code.CONNECTIONLOSS.intValue();
                    }
                    callback.processResult(rc, p, ctx, null);
                }, null);
                return null;
            }).when(client).create(eq(path), any(), any(), any(), any(StringCallback.class), any());
            doAnswer(inv -> {
                StatCallback callback = inv.getArgument(3);
                zk.setData(path, inv.getArgument(1), inv.getArgument(2), (rc, p, ctx, stat) -> {
                    if (rc == Code.OK.intValue()) {
                        committed.incrementAndGet();
                        rc = Code.CONNECTIONLOSS.intValue();
                    }
                    callback.processResult(rc, p, ctx, null);
                }, null);
                return null;
            }).when(client).setData(eq(path), any(), anyInt(), any(StatCallback.class), any());

            try (var store = new ZKMetadataStore(client, config)) {
                for (long expectedVersion : new long[]{-1, 0}) {
                    Value publication = new Value("x".repeat(96 * 1024));
                    var conflict = store.put("/existing", new byte[0], Optional.of(-1L));
                    var write = publication.writeTo(store, path, expectedVersion);
                    // Crossing the batch threshold flushes the two writes above together. The first fails,
                    // so ZooKeeper must execute the publication separately.
                    store.put("/flush-" + expectedVersion, new byte[0], Optional.of(-1L)).join();
                    assertThatThrownBy(conflict::join)
                            .hasCauseInstanceOf(MetadataStoreException.BadVersionException.class);
                    assertThat(write.join().getVersion()).isEqualTo(expectedVersion + 1);
                    assertThat(zk.getData(path, false, null)).isEqualTo(publication.encode());
                }
            }
            assertThat(committed.get()).isEqualTo(2);
        }
    }

    @Test(timeOut = 30_000)
    public void confirmingCreateFromAnotherSessionReturnsTheReadStat() throws Exception {
        var config = MetadataStoreConfig.builder().build();
        try (var server = new TestZKServer();
             var first = new ZKMetadataStore(server.getConnectionString(), config, false);
             var second = new ZKMetadataStore(server.getConnectionString(), config, false)) {
            String path = "/publication";
            var publication = new Value("cursor");
            assertThat(publication.writeTo(first, path, -1).join().isCreatedBySelf()).isTrue();
            assertThat(second.getZkClient().getSessionId()).isNotEqualTo(first.getZkClient().getSessionId());
            var confirmed = publication.writeTo(second, path, -1).join();
            assertThat(confirmed).isEqualTo(second.get(path).join().orElseThrow().getStat());
            assertThat(confirmed.isCreatedBySelf()).isFalse();
        }
    }

    @Test(timeOut = 30_000)
    public void closingStoreFailsPendingVerification() throws Exception {
        try (var server = MockZooKeeper.newInstance()) {
            ZooKeeper client = mock(ZooKeeper.class, delegatesTo(server));
            var store = new ZKMetadataStore(client, false);
            try {
                var publication = new Value("cursor");
                publication.writeTo(store, "/publication", -1).join();
                var reading = new CompletableFuture<Void>();
                doAnswer(inv -> {
                    DataCallback callback = inv.getArgument(2);
                    callback.processResult(Code.OPERATIONTIMEOUT.intValue(), "/publication", null, null, null);
                    reading.complete(null);
                    return null;
                }).when(client).getData(eq("/publication"), eq(false), any(DataCallback.class), any());
                var replay = publication.writeTo(store, "/publication", -1);
                reading.get(5, TimeUnit.SECONDS);
                store.close();
                assertThatThrownBy(replay::join)
                        .hasCauseInstanceOf(MetadataStoreException.AlreadyClosedException.class);
            } finally {
                store.close();
            }
        }
    }

    @Test(timeOut = 30_000)
    public void unconfirmedWriteAndFailedVerificationKeepTheirErrors() throws Exception {
        try (var server = MockZooKeeper.newInstance()) {
            ZooKeeper client = mock(ZooKeeper.class, delegatesTo(server));
            try (var store = new ZKMetadataStore(client, false)) {
                String path = "/publication";
                var publication = new Value("cursor");
                doAnswer(inv -> {
                    MultiCallback callback = inv.getArgument(1);
                    callback.processResult(Code.OPERATIONTIMEOUT.intValue(), path, null, null);
                    return null;
                }).doAnswer(delegatesTo(server)).when(client).multi(any(), any(MultiCallback.class), any());
                assertThatThrownBy(() -> publication.writeTo(store, path, -1).join())
                        .hasRootCauseInstanceOf(KeeperException.OperationTimeoutException.class);
                publication.writeTo(store, path, -1).join();
                doAnswer(inv -> {
                    DataCallback callback = inv.getArgument(2);
                    callback.processResult(Code.NOAUTH.intValue(), path, null, null, null);
                    return null;
                }).when(client).getData(eq(path), eq(false), any(DataCallback.class), any());
                assertThatThrownBy(() -> publication.writeTo(store, path, -1).join())
                        .hasRootCauseInstanceOf(KeeperException.NoAuthException.class);
            }
        }
    }

    @Test(timeOut = 30_000)
    public void constructorFailureClosesResources() {
        String metadataStoreName = "failed-zk-store-" + UUID.randomUUID();
        MetadataStoreConfig config = MetadataStoreConfig.builder()
                .metadataStoreName(metadataStoreName)
                .sessionTimeoutMillis(100)
                .build();

        assertThatThrownBy(() -> new ZKMetadataStore("127.0.0.1:1", config, false))
                .isInstanceOf(MetadataStoreException.class);

        assertThat(Thread.getAllStackTraces().keySet())
                .filteredOn(Thread::isAlive)
                .extracting(Thread::getName)
                .noneMatch(name -> name.startsWith(metadataStoreName));
    }
}
