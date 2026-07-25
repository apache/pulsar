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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import io.opentelemetry.api.OpenTelemetry;
import io.prometheus.client.CollectorRegistry;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;
import lombok.Cleanup;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataNodeSizeStats;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.api.extended.CreateOption;
import org.testng.annotations.Test;

/**
 * Verifies that {@link AbstractMetadataStore#get(String)} records operation stats on the
 * correct completion branch.
 */
public class AbstractMetadataStoreGetStatsTest {

    private static class TestMetadataStore extends AbstractMetadataStore {
        private volatile Function<String, CompletableFuture<Optional<GetResult>>> getImpl;

        TestMetadataStore(String name, MetadataNodeSizeStats nodeSizeStats) {
            super(name, OpenTelemetry.noop(), nodeSizeStats, 1);
        }

        @Override
        protected CompletableFuture<Optional<GetResult>> storeGet(String path) {
            return getImpl.apply(path);
        }

        @Override
        public CompletableFuture<List<String>> getChildrenFromStore(String path) {
            return CompletableFuture.completedFuture(List.of());
        }

        @Override
        protected CompletableFuture<Boolean> existsFromStore(String path) {
            return CompletableFuture.completedFuture(false);
        }

        @Override
        protected CompletableFuture<Void> storeDelete(String path, Optional<Long> expectedVersion) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        protected CompletableFuture<Stat> storePut(String path, byte[] data, Optional<Long> optExpectedVersion,
                                                   EnumSet<CreateOption> options) {
            return FutureUtil.failedFuture(new UnsupportedOperationException());
        }
    }

    private static double getOpsCount(String storeName, String status) {
        Double value = CollectorRegistry.defaultRegistry.getSampleValue(
                "pulsar_metadata_store_ops_latency_ms_count",
                new String[]{"name", "type", "status"},
                new String[]{storeName, "get", status});
        return value == null ? 0.0 : value;
    }

    @Test
    public void testGetSuccessRecordsNodeSizeStats() throws Exception {
        MetadataNodeSizeStats nodeSizeStats = mock(MetadataNodeSizeStats.class);
        @Cleanup
        TestMetadataStore store = new TestMetadataStore("get-stats-success", nodeSizeStats);
        GetResult result = new GetResult(new byte[]{1, 2, 3}, new Stat("/a", 0, 0, 0, false, false));
        store.getImpl = path -> CompletableFuture.completedFuture(Optional.of(result));

        assertSame(store.get("/a").join().orElse(null), result);

        verify(nodeSizeStats).recordGetRes("/a", result);
        assertEquals(getOpsCount("get-stats-success", "success"), 1.0);
    }

    @Test
    public void testGetFailureRecordsFailedOpAndPropagatesOriginalException() throws Exception {
        String storeName = "get-stats-failure";
        @Cleanup
        TestMetadataStore store = new TestMetadataStore(storeName, mock(MetadataNodeSizeStats.class));
        MetadataStoreException injected = new MetadataStoreException("injected failure");
        store.getImpl = path -> FutureUtil.failedFuture(injected);

        CompletionException ex = expectThrows(CompletionException.class, () -> store.get("/a").join());

        assertSame(ex.getCause(), injected);
        assertTrue(Arrays.stream(injected.getSuppressed()).noneMatch(s -> s instanceof NullPointerException),
                "stats callback must not throw NPE on the failure path");
        assertEquals(getOpsCount(storeName, "fail"), 1.0);
    }
}
