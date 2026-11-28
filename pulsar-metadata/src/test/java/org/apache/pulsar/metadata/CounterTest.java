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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotEquals;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import lombok.Cleanup;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.MetadataStoreException.BadVersionException;
import org.apache.pulsar.metadata.api.MetadataStoreException.InvalidPathException;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.api.coordination.CoordinationService;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.coordination.impl.CoordinationServiceImpl;
import org.testng.annotations.Test;

public class CounterTest extends BaseMetadataStoreTest {

    @Test(dataProvider = "impl")
    public void getNextCounterValueReportsInvalidPath(String provider, Supplier<String> urlSupplier) throws Exception {
        try (MetadataStoreExtended store = MetadataStoreExtended.create(urlSupplier.get(),
                MetadataStoreConfig.builder().build());
                CoordinationService cs = new CoordinationServiceImpl(store)) {
            assertThatThrownBy(() -> cs.getNextCounterValue("relative/path").get(5, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(InvalidPathException.class);
        }
    }

    @Test(dataProvider = "impl")
    public void basicTest(String provider, Supplier<String> urlSupplier) throws Exception {
        @Cleanup
        MetadataStoreExtended store = MetadataStoreExtended.create(urlSupplier.get(),
                MetadataStoreConfig.builder().fsyncEnable(false).build());

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
        @Cleanup
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
        maybeTriggerDeletingEmptyContainers(provider);

        @Cleanup
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
                MetadataStoreConfig.builder().fsyncEnable(false).build());

        MetadataStoreExtended spy = spy(store);

        @Cleanup
        CoordinationService cs1 = new CoordinationServiceImpl(spy);

        String path = newKey();
        BadVersionException badVersion = new BadVersionException("Version conflict");
        CompletableFuture<Stat> failedPut = CompletableFuture.failedFuture(badVersion);
        // The counter must retry both wrapped and directly reported version conflicts.
        when(spy.put(eq(path), eq(new byte[0]), eq(Optional.empty())))
                .thenReturn(CompletableFuture.failedFuture(new ExecutionException(badVersion)))
                .thenReturn(failedPut)
                .thenReturn(failedPut)
                .thenCallRealMethod();

        long l1 = cs1.getNextCounterValue(path).join();
        long l2 = cs1.getNextCounterValue(path).join();
        long l3 = cs1.getNextCounterValue(path).join();

        String exhaustedPath = newKey();
        when(spy.put(eq(exhaustedPath), eq(new byte[0]), eq(Optional.empty())))
                .thenReturn(failedPut);

        assertThatThrownBy(() -> cs1.getNextCounterValue(exhaustedPath).join())
                .isInstanceOf(CompletionException.class)
                .cause().isInstanceOf(MetadataStoreException.class)
                .hasMessage("The number of retries has exhausted");

        reset(spy);

        @Cleanup
        CoordinationService cs2 = new CoordinationServiceImpl(store);

        long l4 = cs2.getNextCounterValue(path).join();
        assertThat(new long[] {l1, l2, l3, l4}).doesNotHaveDuplicates();
    }
}
