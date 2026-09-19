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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import lombok.Cleanup;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.Option;
import org.apache.pulsar.metadata.api.ScanConsumer;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.testng.annotations.Test;

public class MetadataStoreScanChildrenTest extends BaseMetadataStoreTest {

    /** Records all callbacks for assertion in tests. */
    private static final class CollectingConsumer implements ScanConsumer {
        final List<GetResult> records = new ArrayList<>();
        final AtomicReference<Throwable> error = new AtomicReference<>();
        final CountDownLatch done = new CountDownLatch(1);

        @Override
        public void onNext(GetResult result) {
            records.add(result);
        }

        @Override
        public void onError(Throwable throwable) {
            error.set(throwable);
            done.countDown();
        }

        @Override
        public void onCompleted() {
            done.countDown();
        }

        void awaitDone() throws InterruptedException {
            assertTrue(done.await(30, TimeUnit.SECONDS), "scan did not terminate within 30s");
        }
    }

    @Test(dataProvider = "impl")
    public void streamsAllChildrenWithValues(String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStoreExtended store = MetadataStoreExtended.create(
                urlSupplier.get(), MetadataStoreConfig.builder().build());

        String parent = newKey();
        Set<String> expectedNames = Set.of("a", "b", "c");
        for (String name : expectedNames) {
            store.put(parent + "/" + name, name.getBytes(StandardCharsets.UTF_8), Optional.of(-1L)).join();
        }

        CollectingConsumer consumer = new CollectingConsumer();
        store.scanChildren(parent, consumer).join();
        consumer.awaitDone();

        assertEquals(consumer.records.size(), 3);
        Set<String> seenPaths = new HashSet<>();
        Set<String> seenValues = new HashSet<>();
        for (GetResult r : consumer.records) {
            seenPaths.add(r.getStat().getPath());
            seenValues.add(new String(r.getValue(), StandardCharsets.UTF_8));
        }
        assertEquals(seenPaths, Set.of(parent + "/a", parent + "/b", parent + "/c"));
        assertEquals(seenValues, expectedNames);
    }

    @Test(dataProvider = "impl")
    public void parentWithNoChildrenCompletesEmpty(String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStoreExtended store = MetadataStoreExtended.create(
                urlSupplier.get(), MetadataStoreConfig.builder().build());

        String parent = newKey();

        CollectingConsumer consumer = new CollectingConsumer();
        store.scanChildren(parent, consumer).join();
        consumer.awaitDone();

        assertEquals(consumer.records.size(), 0);
    }

    @Test(dataProvider = "impl")
    public void doesNotEmitDescendantsBeyondImmediateChildren(String provider, Supplier<String> urlSupplier)
            throws Exception {
        // scanChildren is hierarchy-aware: deeper paths (children of children) are NOT emitted.
        @Cleanup
        MetadataStoreExtended store = MetadataStoreExtended.create(
                urlSupplier.get(), MetadataStoreConfig.builder().build());

        String parent = newKey();
        String child = parent + "/child";
        String grandchild = child + "/inner";

        store.put(child, "C".getBytes(StandardCharsets.UTF_8), Optional.of(-1L)).join();
        store.put(grandchild, "G".getBytes(StandardCharsets.UTF_8), Optional.of(-1L)).join();

        CollectingConsumer consumer = new CollectingConsumer();
        store.scanChildren(parent, consumer).join();
        consumer.awaitDone();

        assertEquals(consumer.records.size(), 1);
        assertEquals(consumer.records.get(0).getStat().getPath(), child);
    }

    @Test(dataProvider = "impl")
    public void skipsSequenceCounterSidecars(String provider, Supplier<String> urlSupplier) throws Exception {
        // When SequenceKeysDeltas is used on a non-native backend, the abstract layer writes a
        // sidecar counter document under the same parent. scanChildren must not surface it as a
        // user record (its value is the binary counter encoding, not the caller's payload).
        @Cleanup
        MetadataStoreExtended store = MetadataStoreExtended.create(
                urlSupplier.get(), MetadataStoreConfig.builder().build());

        String parent = newKey();
        String prefix = parent + "/op";
        Set<Option> putOpts = "Oxia".equals(provider)
                ? Set.of(new Option.SequenceKeysDeltas(List.of(1L)), new Option.PartitionKey(prefix))
                : Set.of(new Option.SequenceKeysDeltas(List.of(1L)));
        store.put(prefix, "a".getBytes(StandardCharsets.UTF_8), Optional.empty(), putOpts).join();
        store.put(prefix, "b".getBytes(StandardCharsets.UTF_8), Optional.empty(), putOpts).join();

        CollectingConsumer consumer = new CollectingConsumer();
        store.scanChildren(parent, consumer).join();
        consumer.awaitDone();

        // Only the two appended records; the counter sidecar (if any) is filtered out.
        assertEquals(consumer.records.size(), 2);
        for (GetResult r : consumer.records) {
            assertTrue(!r.getStat().getPath().endsWith("__seq_counter__"),
                    "counter sidecar leaked through scanChildren: " + r.getStat().getPath());
        }
    }

    @Test(dataProvider = "impl")
    public void closedStoreRejectsScan(String provider, Supplier<String> urlSupplier) throws Exception {
        MetadataStoreExtended store = MetadataStoreExtended.create(
                urlSupplier.get(), MetadataStoreConfig.builder().build());
        store.close();

        CollectingConsumer consumer = new CollectingConsumer();
        CompletionException ex = expectThrows(CompletionException.class,
                () -> store.scanChildren("/anything", consumer).join());
        assertTrue(ex.getCause() instanceof MetadataStoreException.AlreadyClosedException,
                "expected AlreadyClosedException, got: " + ex.getCause());
    }
}
