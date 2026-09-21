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
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Supplier;
import lombok.Cleanup;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreFactory;
import org.apache.pulsar.metadata.api.Option;
import org.apache.pulsar.metadata.api.Stat;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * Cross-backend tests for {@link Option.SequenceKeysDeltas} and
 * {@link MetadataStore#subscribeSequence}.
 *
 * <p>For Oxia these are the native primitives; for the other backends the same behavior is
 * synthesized via the CAS counter sidecar + listener bridge in {@code AbstractMetadataStore}.
 * Both paths must produce monotonically increasing keys with the Oxia byte-format and deliver
 * subscription updates on writes.
 */
public class SequenceKeysTest extends BaseMetadataStoreTest {

    @Test(dataProvider = "impl")
    public void singleDimensionSequence(String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStore store = MetadataStoreFactory.create(urlSupplier.get(),
                MetadataStoreConfig.builder().fsyncEnable(false).build());

        String prefix = newKey();
        Set<Option> opts = optsFor(provider, new Option.SequenceKeysDeltas(List.of(1L)));

        Stat r1 = store.put(prefix, "a".getBytes(StandardCharsets.UTF_8), Optional.empty(), opts).get();
        Stat r2 = store.put(prefix, "b".getBytes(StandardCharsets.UTF_8), Optional.empty(), opts).get();
        Stat r3 = store.put(prefix, "c".getBytes(StandardCharsets.UTF_8), Optional.empty(), opts).get();

        // Key shape: <prefix>-<seq:%020d> for both native (Oxia) and the compat layer.
        assertThat(r1.getPath()).matches("\\Q" + prefix + "\\E-\\d{20}");
        assertThat(r2.getPath()).isGreaterThan(r1.getPath());
        assertThat(r3.getPath()).isGreaterThan(r2.getPath());

        Set<Option> readOpts = readOptsFor(provider);
        assertThat(store.get(r1.getPath(), readOpts).get()).isPresent();
        assertThat(store.get(r3.getPath(), readOpts).get()).isPresent();
    }

    @Test(dataProvider = "impl")
    public void multiDimensionSequence(String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStore store = MetadataStoreFactory.create(urlSupplier.get(),
                MetadataStoreConfig.builder().fsyncEnable(false).build());

        String prefix = newKey();
        // Two dimensions: dim 0 always ticks, dim 1 ticks only when caller asks.
        Set<Option> bumpBoth = optsFor(provider, new Option.SequenceKeysDeltas(List.of(1L, 1L)));
        Set<Option> bumpFirstOnly = optsFor(provider, new Option.SequenceKeysDeltas(List.of(1L, 0L)));

        Stat r0 = store.put(prefix, new byte[]{0}, Optional.empty(), bumpBoth).get();
        Stat r1 = store.put(prefix, new byte[]{1}, Optional.empty(), bumpFirstOnly).get();
        Stat r2 = store.put(prefix, new byte[]{2}, Optional.empty(), bumpBoth).get();

        assertThat(r0.getPath()).matches("\\Q" + prefix + "\\E-\\d{20}-\\d{20}");
        assertThat(r1.getPath()).isGreaterThan(r0.getPath());
        assertThat(r2.getPath()).isGreaterThan(r1.getPath());
    }

    @Test(dataProvider = "impl")
    public void subscribeSequence(String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStore store = MetadataStoreFactory.create(urlSupplier.get(),
                MetadataStoreConfig.builder().fsyncEnable(false).build());

        String prefix = newKey();
        Set<Option> putOpts = optsFor(provider, new Option.SequenceKeysDeltas(List.of(1L)));
        Set<Option> subOpts = readOptsFor(provider);

        ConcurrentLinkedQueue<String> received = new ConcurrentLinkedQueue<>();
        @Cleanup
        AutoCloseable handle = store.subscribeSequence(prefix, received::add, subOpts);

        Stat r1 = store.put(prefix, new byte[]{1}, Optional.empty(), putOpts).get();
        Stat r2 = store.put(prefix, new byte[]{2}, Optional.empty(), putOpts).get();
        Stat r3 = store.put(prefix, new byte[]{3}, Optional.empty(), putOpts).get();

        // Updates may be collapsed; the final emission must be the latest sequence key.
        Awaitility.await().untilAsserted(() ->
                assertThat(received).isNotEmpty().last().asString().isEqualTo(r3.getPath()));
        // Every emission must point to one of the writes we performed (no spurious paths).
        for (String s : received) {
            assertThat(s).isIn(r1.getPath(), r2.getPath(), r3.getPath());
        }
    }

    /**
     * Add the routing-hint partition key for Oxia (which requires it for sequence-keys); other
     * backends ignore it.
     */
    private static Set<Option> optsFor(String provider, Option.SequenceKeysDeltas deltas) {
        if ("Oxia".equals(provider)) {
            return Set.of(deltas, new Option.PartitionKey("seq-pk"));
        }
        return Set.of(deltas);
    }

    private static Set<Option> readOptsFor(String provider) {
        if ("Oxia".equals(provider)) {
            return Set.of(new Option.PartitionKey("seq-pk"));
        }
        return Set.of();
    }

}
