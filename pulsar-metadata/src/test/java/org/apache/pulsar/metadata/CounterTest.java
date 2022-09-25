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

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.fail;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import lombok.Cleanup;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.api.coordination.CoordinationService;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.coordination.impl.CoordinationServiceImpl;
import org.testng.annotations.Test;

public class CounterTest extends BaseMetadataStoreTest {

    @Test(dataProvider = "impl")
    public void basicTest(String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStoreExtended store = MetadataStoreExtended.create(urlSupplier.get(),
                MetadataStoreConfig.builder().build());

        @Cleanup
        CoordinationService cs1 = new CoordinationServiceImpl(store);

        long l1 = cs1.getNextCounterValue("/my/path").join();
        long l2 = cs1.getNextCounterValue("/my/path").join();
        long l3 = cs1.getNextCounterValue("/my/path").join();

        assertNotEquals(l1, l2);
        assertNotEquals(l2, l3);

        @Cleanup
        CoordinationService cs2 = new CoordinationServiceImpl(store);

        long l4 = cs2.getNextCounterValue("/my/path").join();
        assertNotEquals(l3, l4);
    }

    @Test(dataProvider = "impl")
    public void testCounterDoesNotAutoReset(String provider, Supplier<String> urlSupplier) throws Exception {
        if (provider.equals("Memory")) {
            // Test doesn't make sense for local memory since we're testing across different instances
            return;
        }
        String metadataUrl = urlSupplier.get();
        MetadataStoreExtended store1 = MetadataStoreExtended.create(metadataUrl, MetadataStoreConfig.builder().build());

        CoordinationService cs1 = new CoordinationServiceImpl(store1);

        long l1 = cs1.getNextCounterValue("/my/path").join();
        long l2 = cs1.getNextCounterValue("/my/path").join();
        long l3 = cs1.getNextCounterValue("/my/path").join();

        assertNotEquals(l1, l2);
        assertNotEquals(l2, l3);

        cs1.close();
        store1.close();

        // Delete all the empty container nodes
        zks.checkContainers();

        MetadataStoreExtended store2 = MetadataStoreExtended.create(metadataUrl, MetadataStoreConfig.builder().build());
        @Cleanup
        CoordinationService cs2 = new CoordinationServiceImpl(store2);

        long l4 = cs2.getNextCounterValue("/my/path").join();
        assertNotEquals(l1, l4);
        assertNotEquals(l2, l4);
        assertNotEquals(l3, l4);
    }

    @Test(dataProvider = "impl")
    public void testGetNextCounterRetry(String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStoreExtended store = MetadataStoreExtended.create(urlSupplier.get(),
                MetadataStoreConfig.builder().build());

        MetadataStoreExtended spy = spy(store);

        @Cleanup
        CoordinationService cs1 = new CoordinationServiceImpl(spy);

        AtomicInteger count = new AtomicInteger(0);
        CompletableFuture<Stat> future = new CompletableFuture<>();
        future.completeExceptionally(new MetadataStoreException.BadVersionException(""));
        when(spy.put(eq("/my/path"), eq(new byte[0]), eq(Optional.empty())))
                .thenAnswer(__ -> {
                    // Retry three times, then it will return success.
                    if (count.incrementAndGet() <= 3) {
                        return future;
                    }
                    reset(spy);
                    return CompletableFuture.completedFuture(null);
                });

        long l1 = cs1.getNextCounterValue("/my/path").join();
        long l2 = cs1.getNextCounterValue("/my/path").join();
        long l3 = cs1.getNextCounterValue("/my/path").join();

        assertNotEquals(l1, l2);
        assertNotEquals(l2, l3);

        when(spy.put(eq("/my/path1"), eq(new byte[0]), eq(Optional.empty())))
                .thenReturn(future);

        try {
            cs1.getNextCounterValue("/my/path1").join();
            fail("Should fail with MetadataStoreException.");
        } catch (Exception ex) {
            assertEquals(ex.getCause().getMessage(), "The number of retries has exhausted");
        }

        reset(spy);

        @Cleanup
        CoordinationService cs2 = new CoordinationServiceImpl(store);

        long l4 = cs2.getNextCounterValue("/my/path").join();
        assertNotEquals(l3, l4);
    }
}
