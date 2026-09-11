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
package org.apache.pulsar.metadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertNotEquals;
import io.oxia.testcontainers.OxiaContainer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import lombok.Cleanup;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreFactory;
import org.apache.pulsar.metadata.api.Option;
import org.apache.pulsar.metadata.api.Stat;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Native sequence-key tests against Oxia.
 *
 * <p>Oxia must receive {@link Option.PartitionKey} alongside {@link Option.SequenceKeysDeltas} —
 * sequence allocation is shard-local, so all writes that share a sequence prefix have to land in
 * the same shard. The tests use a multi-shard cluster to keep the routing path honest.
 */
public class OxiaSequenceKeysTest {

    private static final int SHARDS = 3;
    private OxiaContainer oxiaServer;

    @BeforeClass
    public void start() {
        oxiaServer = new OxiaContainer(OxiaContainer.DEFAULT_IMAGE_NAME).withShards(SHARDS);
        oxiaServer.start();
    }

    @AfterClass(alwaysRun = true)
    public void stop() {
        if (oxiaServer != null) {
            oxiaServer.close();
            oxiaServer = null;
        }
    }

    private MetadataStore newStore() throws Exception {
        return MetadataStoreFactory.create(
                "oxia://" + oxiaServer.getServiceAddress(),
                MetadataStoreConfig.builder().fsyncEnable(false).build());
    }

    @Test
    public void singleDimensionSequence() throws Exception {
        @Cleanup
        MetadataStore store = newStore();

        String prefix = "/seq-single-" + System.nanoTime();
        Set<Option> opts = Set.of(
                new Option.PartitionKey("seq-pk"),
                new Option.SequenceKeysDeltas(List.of(1L)));

        Stat first = store.put(prefix, "a".getBytes(StandardCharsets.UTF_8), Optional.empty(), opts).get();
        Stat second = store.put(prefix, "b".getBytes(StandardCharsets.UTF_8), Optional.empty(), opts).get();
        Stat third = store.put(prefix, "c".getBytes(StandardCharsets.UTF_8), Optional.empty(), opts).get();

        // Each call yields a distinct, monotonically-increasing key derived from the prefix.
        assertThat(first.getPath()).startsWith(prefix);
        assertNotEquals(first.getPath(), prefix, "Stat path should be the synthesized key, not the prefix");
        assertThat(second.getPath()).isGreaterThan(first.getPath());
        assertThat(third.getPath()).isGreaterThan(second.getPath());

        // The synthesized records actually exist and round-trip with the same partition key.
        Set<Option> readOpts = Set.of(new Option.PartitionKey("seq-pk"));
        assertThat(store.get(first.getPath(), readOpts).get()).isPresent();
        assertThat(store.get(third.getPath(), readOpts).get()).isPresent();
    }

    @Test
    public void multiDimensionSequence() throws Exception {
        @Cleanup
        MetadataStore store = newStore();

        String prefix = "/seq-multi-" + System.nanoTime();

        // Two-dimension sequence: dim 0 always increments, dim 1 increments only when caller asks
        // for it (delta=0 means "keep dim 1 unchanged").
        Set<Option> bumpBoth = Set.of(
                new Option.PartitionKey("seq-pk"),
                new Option.SequenceKeysDeltas(List.of(1L, 1L)));
        Set<Option> bumpFirstOnly = Set.of(
                new Option.PartitionKey("seq-pk"),
                new Option.SequenceKeysDeltas(List.of(1L, 0L)));

        Stat r0 = store.put(prefix, new byte[]{0}, Optional.empty(), bumpBoth).get();
        Stat r1 = store.put(prefix, new byte[]{1}, Optional.empty(), bumpFirstOnly).get();
        Stat r2 = store.put(prefix, new byte[]{2}, Optional.empty(), bumpBoth).get();

        // All three should be lexicographically increasing — dim 0 ticks every call.
        assertThat(r0.getPath()).startsWith(prefix);
        assertThat(r1.getPath()).isGreaterThan(r0.getPath());
        assertThat(r2.getPath()).isGreaterThan(r1.getPath());
    }

    @Test
    public void subscribeSequence() throws Exception {
        @Cleanup
        MetadataStore store = newStore();

        String prefix = "/seq-watch-" + System.nanoTime();
        Set<Option> opts = Set.of(
                new Option.PartitionKey("seq-pk"),
                new Option.SequenceKeysDeltas(List.of(1L)));
        Set<Option> subOpts = Set.of(new Option.PartitionKey("seq-pk"));

        ConcurrentLinkedQueue<String> received = new ConcurrentLinkedQueue<>();
        @Cleanup
        AutoCloseable handle = store.subscribeSequence(prefix, received::add, subOpts);

        Stat r1 = store.put(prefix, new byte[]{1}, Optional.empty(), opts).get();
        Stat r2 = store.put(prefix, new byte[]{2}, Optional.empty(), opts).get();
        Stat r3 = store.put(prefix, new byte[]{3}, Optional.empty(), opts).get();

        // The listener may collapse intermediate updates, but the final value it ever reports must
        // be the latest sequence key — i.e. r3.getPath().
        Awaitility.await().untilAsserted(() ->
                assertThat(received).isNotEmpty().last().asString().isEqualTo(r3.getPath()));
        // Earlier emissions, when present, must point to one of the writes we performed.
        for (String s : received) {
            assertThat(s).isIn(r1.getPath(), r2.getPath(), r3.getPath());
        }
    }
}
