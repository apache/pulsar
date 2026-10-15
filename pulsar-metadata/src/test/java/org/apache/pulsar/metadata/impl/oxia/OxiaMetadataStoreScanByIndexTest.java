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
package org.apache.pulsar.metadata.impl.oxia;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import static org.testng.Assert.fail;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.RangeScanConsumer;
import io.oxia.client.api.Version;
import io.oxia.client.api.options.RangeScanOption;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Cleanup;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.Option;
import org.apache.pulsar.metadata.api.ScanConsumer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Covers how {@link OxiaMetadataStore#scanByIndex} handles what the Oxia client returns, which a real Oxia doesn't
 * produce on demand.
 */
public class OxiaMetadataStoreScanByIndexTest {

    private AsyncOxiaClient client;
    private OxiaMetadataStore store;

    @BeforeMethod
    public void setup() {
        client = mock(AsyncOxiaClient.class);
        store = new OxiaMetadataStore(client, "test-identity");
    }

    @AfterMethod(alwaysRun = true)
    public void teardown() throws Exception {
        if (store != null) {
            store.close();
            store = null;
        }
    }

    @Test
    public void scanByIndexScansTheIndexOnTheShardOfThePartitionKey() throws Exception {
        scanReturns(record("/p/a"));

        List<GetResult> results = new ArrayList<>();
        store.scanByIndex("/p", "idx", "k", "k", r -> true, ScanConsumer.collectInto(results),
                Set.of(new Option.PartitionKey("pk"))).get();

        assertEquals(paths(results), List.of("/p/a"));
        verify(client).rangeScan(anyString(), anyString(), any(RangeScanConsumer.class),
                eq(Set.of(RangeScanOption.UseIndex("idx"), RangeScanOption.PartitionKey("pk"))));
    }

    @Test
    public void scanByIndexSkipsRecordsDeletedDuringTheScan() throws Exception {
        // Oxia returns a record deleted after the scan read its index entry as not found: without a
        // version or a value
        io.oxia.client.api.GetResult deleted = new io.oxia.client.api.GetResult("/p/b", new byte[0],
                new Version(0, 0, 0, 0, Optional.empty(), Optional.empty()));
        scanReturns(record("/p/a"), deleted, record("/p/c"));

        List<GetResult> results = new ArrayList<>();
        store.scanByIndex("/p", "idx", "k", "k", r -> true, ScanConsumer.collectInto(results)).get();

        assertEquals(paths(results), List.of("/p/a", "/p/c"));
    }

    @Test
    public void scanByIndexFailsWithTheErrorOfItsConsumer() {
        scanReturns(record("/p/a"), record("/p/b"));
        RuntimeException error = new RuntimeException("bad record");
        List<GetResult> received = new ArrayList<>();
        AtomicReference<Throwable> reported = new AtomicReference<>();

        CompletableFuture<Void> scan = store.scanByIndex("/p", "idx", "k", "k", r -> true, new ScanConsumer() {
            @Override
            public void onNext(GetResult result) {
                received.add(result);
                throw error;
            }

            @Override
            public void onError(Throwable throwable) {
                reported.set(throwable);
            }

            @Override
            public void onCompleted() {
                fail("the scan should fail");
            }
        });

        ExecutionException e = expectThrows(ExecutionException.class, scan::get);
        assertSame(e.getCause(), error);
        assertSame(reported.get(), error);
        // The scan stops at the record the consumer failed on
        assertEquals(paths(received), List.of("/p/a"));
    }

    @Test
    public void scanByIndexFailsOnceWhenItDoesNotEndWithinTheRequestTimeout() throws Exception {
        // The client keeps retrying the scan of a shard that doesn't answer, and reports nothing meanwhile
        AtomicReference<RangeScanConsumer> scanConsumer = new AtomicReference<>();
        doAnswer(invocation -> {
            scanConsumer.set(invocation.getArgument(2));
            return null;
        }).when(client).rangeScan(anyString(), anyString(), any(RangeScanConsumer.class), anySet());
        @Cleanup
        OxiaMetadataStore storeWithShortTimeout =
                new OxiaMetadataStore(client, "test-identity", null, Duration.ofMillis(100));
        List<GetResult> received = new ArrayList<>();
        AtomicInteger errors = new AtomicInteger();
        AtomicInteger completions = new AtomicInteger();

        CompletableFuture<Void> scan = storeWithShortTimeout.scanByIndex("/p", "idx", "k", "k", r -> true,
                new ScanConsumer() {
                    @Override
                    public void onNext(GetResult result) {
                        received.add(result);
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        errors.incrementAndGet();
                    }

                    @Override
                    public void onCompleted() {
                        completions.incrementAndGet();
                    }
                });

        ExecutionException e = expectThrows(ExecutionException.class, () -> scan.get(10, TimeUnit.SECONDS));
        assertTrue(e.getCause() instanceof TimeoutException, "unexpected failure: " + e.getCause());
        // When the shard answers at last, the scan stops it and passes nothing more on
        assertFalse(scanConsumer.get().onNext(record("/p/a")));
        scanConsumer.get().onError(new RuntimeException("late error"));
        scanConsumer.get().onCompleted();
        assertEquals(errors.get(), 1);
        assertEquals(completions.get(), 0);
        assertEquals(received, List.of());
    }

    private void scanReturns(io.oxia.client.api.GetResult... records) {
        doAnswer(invocation -> {
            RangeScanConsumer consumer = invocation.getArgument(2);
            // As the client does: stop at the first record the consumer doesn't take, then complete
            for (io.oxia.client.api.GetResult record : records) {
                if (!consumer.onNext(record)) {
                    break;
                }
            }
            consumer.onCompleted();
            return null;
        }).when(client).rangeScan(anyString(), anyString(), any(RangeScanConsumer.class), anySet());
    }

    private static io.oxia.client.api.GetResult record(String key) {
        return new io.oxia.client.api.GetResult(key, key.getBytes(StandardCharsets.UTF_8),
                new Version(1, 1000, 1000, 0, Optional.empty(), Optional.empty()));
    }

    private static List<String> paths(List<GetResult> results) {
        return results.stream().map(r -> r.getStat().getPath()).toList();
    }
}
