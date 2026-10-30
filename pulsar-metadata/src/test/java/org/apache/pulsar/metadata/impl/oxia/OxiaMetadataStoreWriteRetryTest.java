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

import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.OxiaClientBuilder;
import io.oxia.client.api.exceptions.ResourceUnavailableException;
import io.oxia.testcontainers.OxiaContainer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Cleanup;
import org.apache.pulsar.metadata.OxiaTestServer;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.Option;
import org.mockito.stubbing.Answer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Writes that fail with {@link ResourceUnavailableException}, as the Oxia client fails the writes in flight when a
 * shard leader changes, are retried by the store for up to its request timeout.
 */
public class OxiaMetadataStoreWriteRetryTest {

    private OxiaContainer oxiaServer;

    @BeforeClass
    public void setup() {
        oxiaServer = OxiaTestServer.start(1);
    }

    @AfterClass(alwaysRun = true)
    public void cleanup() {
        if (oxiaServer != null) {
            oxiaServer.close();
        }
    }

    @Test
    public void putIsRetriedWhileTheShardIsUnavailable() throws Exception {
        AsyncOxiaClient oxia = OxiaClientBuilder.create(oxiaServer.getServiceAddress()).asyncClient().get();
        AsyncOxiaClient client = mock(AsyncOxiaClient.class, delegatesTo(oxia));
        AtomicInteger failures = new AtomicInteger(2);
        doAnswer(failFirst(oxia, failures)).when(client).put(anyString(), any(), anySet());
        @Cleanup
        OxiaMetadataStore store = new OxiaMetadataStore(client, "test-identity");
        String path = "/write-retry-put-" + System.nanoTime() + "/key";

        store.put(path, "value".getBytes(StandardCharsets.UTF_8), Optional.of(-1L)).get(10, TimeUnit.SECONDS);

        assertEquals(failures.get(), 0);
        assertEquals(new String(store.get(path).get(10, TimeUnit.SECONDS).orElseThrow().getValue(),
                StandardCharsets.UTF_8), "value");
    }

    @Test
    public void deleteIsRetriedWhileTheShardIsUnavailable() throws Exception {
        AsyncOxiaClient oxia = OxiaClientBuilder.create(oxiaServer.getServiceAddress()).asyncClient().get();
        AsyncOxiaClient client = mock(AsyncOxiaClient.class, delegatesTo(oxia));
        AtomicInteger failures = new AtomicInteger(2);
        doAnswer(failFirst(oxia, failures)).when(client).delete(anyString(), anySet());
        @Cleanup
        OxiaMetadataStore store = new OxiaMetadataStore(client, "test-identity");
        String path = "/write-retry-delete-" + System.nanoTime() + "/key";
        store.put(path, new byte[0], Optional.of(-1L)).get(10, TimeUnit.SECONDS);

        store.delete(path, Optional.empty()).get(10, TimeUnit.SECONDS);

        assertEquals(failures.get(), 0);
        assertFalse(store.exists(path).get(10, TimeUnit.SECONDS));
    }

    @Test
    public void writeFailsWhenTheShardStaysUnavailableForTheRequestTimeout() throws Exception {
        AsyncOxiaClient oxia = OxiaClientBuilder.create(oxiaServer.getServiceAddress()).asyncClient().get();
        AsyncOxiaClient client = mock(AsyncOxiaClient.class, delegatesTo(oxia));
        doAnswer(failFirst(oxia, new AtomicInteger(Integer.MAX_VALUE)))
                .when(client).put(anyString(), any(), anySet());
        @Cleanup
        OxiaMetadataStore store = new OxiaMetadataStore(client, "test-identity", null, Duration.ofSeconds(1));

        ExecutionException e = expectThrows(ExecutionException.class,
                () -> store.put("/write-retry-timeout-" + System.nanoTime(), new byte[0], Optional.empty())
                        .get(10, TimeUnit.SECONDS));

        assertTrue(e.getCause() instanceof MetadataStoreException, "unexpected failure: " + e.getCause());
        assertTrue(e.getCause().getCause() instanceof ResourceUnavailableException,
                "unexpected failure: " + e.getCause());
        verify(client, atLeast(2)).put(anyString(), any(), anySet());
    }

    @Test
    public void putWithSequenceKeysDeltasIsNotRetried() throws Exception {
        AsyncOxiaClient oxia = OxiaClientBuilder.create(oxiaServer.getServiceAddress()).asyncClient().get();
        AsyncOxiaClient client = mock(AsyncOxiaClient.class, delegatesTo(oxia));
        doAnswer(failFirst(oxia, new AtomicInteger(1))).when(client).put(anyString(), any(), anySet());
        @Cleanup
        OxiaMetadataStore store = new OxiaMetadataStore(client, "test-identity");

        // A key without a parent, so that the put is the only write
        ExecutionException e = expectThrows(ExecutionException.class,
                () -> store.put("/write-retry-seq-" + System.nanoTime(), new byte[0], Optional.empty(),
                                Set.of(new Option.SequenceKeysDeltas(List.of(1L))))
                        .get(10, TimeUnit.SECONDS));

        assertTrue(e.getCause().getCause() instanceof ResourceUnavailableException,
                "unexpected failure: " + e.getCause());
        verify(client, times(1)).put(anyString(), any(), anySet());
    }

    private static Answer<CompletableFuture<?>> failFirst(AsyncOxiaClient oxia, AtomicInteger failures) {
        return invocation -> {
            if (failures.getAndUpdate(n -> Math.max(n - 1, 0)) > 0) {
                return CompletableFuture.failedFuture(
                        new ResourceUnavailableException("Write stream failed: context canceled", null));
            }
            return (CompletableFuture<?>) invocation.getMethod().invoke(oxia, invocation.getArguments());
        };
    }
}
