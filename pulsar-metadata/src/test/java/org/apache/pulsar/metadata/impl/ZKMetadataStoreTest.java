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
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.metadata.api.HasPublicationId;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.Option.VerifyPublication;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.zookeeper.AsyncCallback.MultiCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.MockZooKeeper;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.ZooKeeper;
import org.testng.annotations.Test;

public class ZKMetadataStoreTest {

    private record Value(String id, String payload) implements HasPublicationId {
        Value(String payload) {
            this(UUID.randomUUID().toString(), payload);
        }

        @Override
        public String getPublication() {
            return id;
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
                assertThat(created.isCreatedBySelf()).isTrue();
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
                Value changedPayload = new Value(update.id(), "other");
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
