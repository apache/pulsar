/**
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
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import lombok.Cleanup;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.MetadataStoreException.BadVersionException;
import org.apache.pulsar.metadata.api.MetadataStoreException.NotFoundException;
import org.apache.pulsar.metadata.api.MetadataStoreFactory;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.NotificationType;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.api.extended.CreateOption;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.assertj.core.util.Lists;
import org.testng.annotations.Test;

public class MetadataStoreBatchingTest extends BaseMetadataStoreTest {

    @Test(dataProvider = "impl")
    public void testBatching(String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStore store = MetadataStoreFactory.create(urlSupplier.get(), MetadataStoreConfig.builder()
                        .batchingEnabled(true)
                        .batchingMaxDelayMillis(1_000)
                .build());

        String key1 = newKey();
        store.put(key1, new byte[0], Optional.empty()).join();

        String key2 = newKey();

        CompletableFuture<Optional<GetResult>> f1 = store.get(key1);
        CompletableFuture<Optional<GetResult>> f2 = store.get(key2);

        Optional<GetResult> r1 = f1.join();
        Optional<GetResult> r2 = f2.join();

        assertTrue(r1.isPresent());
        assertFalse(r2.isPresent());
    }

    @Test(dataProvider = "impl")
    public void testPutVersionErrors(String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStore store = MetadataStoreFactory.create(urlSupplier.get(), MetadataStoreConfig.builder()
                .batchingEnabled(true)
                .batchingMaxDelayMillis(1_000)
                .build());

        String key1 = newKey();

        CompletableFuture<Stat> f1 = store.put(key1 + "/a", new byte[0], Optional.empty()); // Should succeed
        CompletableFuture<Stat> f2 = store.put(key1 + "/b", new byte[0], Optional.of(1L)); // Should fail
        CompletableFuture<Stat> f3 = store.put(key1 + "/c", new byte[0], Optional.of(-1L)); // Should succeed
        CompletableFuture<Void> f4 = store.delete(key1 + "/d", Optional.empty()); // Should fail

        assertEquals(f1.join().getVersion(), 0L);

        try {
            f2.join();
        } catch (CompletionException ce) {
            assertEquals(ce.getCause().getClass(), BadVersionException.class);
        }

        assertEquals(f3.join().getVersion(), 0L);

        try {
            f4.join();
        } catch (CompletionException ce) {
            assertEquals(ce.getCause().getClass(), NotFoundException.class);
        }
    }

    @Test(dataProvider = "impl")
    public void testSequential(String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStoreExtended store = MetadataStoreExtended.create(urlSupplier.get(), MetadataStoreConfig.builder()
                .batchingEnabled(true)
                .batchingMaxDelayMillis(1_000)
                .build());

        String key1 = newKey();

        List<CompletableFuture<Stat>> putFutures = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            CompletableFuture<Stat> f =
                    store.put(key1 + "/x", new byte[0], Optional.of(-1L), EnumSet.of(CreateOption.Sequential));
            putFutures.add(f);
        }

        FutureUtil.waitForAll(putFutures).join();


        assertEquals(store.getChildren(key1).join().size(), 10);
    }


    @Test(dataProvider = "impl")
    public void testBigBatchSize(String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStore store = MetadataStoreFactory.create(urlSupplier.get(), MetadataStoreConfig.builder()
                .batchingEnabled(true)
                .batchingMaxDelayMillis(1_000)
                .build());

        String key1 = newKey();

        // Create 20 MB of data and try to read it out
        int dataSize = 500 * 1024;
        byte[] payload = new byte[dataSize];
        int N = 40;

        List<CompletableFuture<Stat>> putFutures = new ArrayList<>();

        for (int i = 0; i < N; i++) {
            putFutures.add(store.put(key1 + "/" + i, payload, Optional.empty()));
        }

        FutureUtil.waitForAll(putFutures).join();


        List<CompletableFuture<Optional<GetResult>>> getFutures = new ArrayList<>();

        for (int i = 0; i < N; i++) {
            getFutures.add(store.get(key1 + "/" + i));
        }

        FutureUtil.waitForAll(getFutures).join();
    }
}
