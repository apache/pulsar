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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.expectThrows;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.Version;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import lombok.Cleanup;
import org.apache.pulsar.metadata.api.Option;
import org.apache.pulsar.metadata.api.ScanConsumer;
import org.testng.annotations.Test;

public class OxiaMetadataStoreScanByIndexTest {

    @Test
    public void resolverFailureDoesNotCallOnNextAfterError() throws Exception {
        AsyncOxiaClient client = mock(AsyncOxiaClient.class);
        CompletableFuture<io.oxia.client.api.GetResult> firstGet = new CompletableFuture<>();
        when(client.list(anyString(), anyString(), anySet()))
                .thenReturn(CompletableFuture.completedFuture(List.of("/first", "/second")));
        when(client.get(eq("/first"), anySet())).thenReturn(firstGet);
        @Cleanup
        OxiaMetadataStore store = new OxiaMetadataStore(client, "test");

        List<String> callbacks = new ArrayList<>();
        CompletableFuture<Void> scan = store.storeScanByIndex("/", "idx", "match", "match",
                __ -> true, recordingConsumer(callbacks), Set.of(new Option.PartitionKeyResolver(path -> {
                    if (path.equals("/second")) {
                        throw new IllegalStateException("resolver failure");
                    }
                    return "first-route";
                })));

        assertFalse(scan.isDone(), "the next resolver must wait for the preceding read");
        firstGet.complete(getResult("/first"));

        CompletionException error = expectThrows(CompletionException.class, scan::join);
        assertThat(error).hasCauseInstanceOf(IllegalStateException.class);
        assertEquals(callbacks, List.of("next:/first", "error"));
        verify(client, never()).get(eq("/second"), anySet());
    }

    @Test
    public void unresolvedPartitionKeyFailsIndexScan() throws Exception {
        AsyncOxiaClient client = mock(AsyncOxiaClient.class);
        when(client.list(anyString(), anyString(), anySet()))
                .thenReturn(CompletableFuture.completedFuture(List.of("/unroutable")));
        @Cleanup
        OxiaMetadataStore store = new OxiaMetadataStore(client, "test");

        List<String> callbacks = new ArrayList<>();
        CompletableFuture<Void> scan = store.storeScanByIndex("/", "idx", "match", "match",
                __ -> true, recordingConsumer(callbacks),
                Set.of(new Option.PartitionKeyResolver(__ -> null)));

        CompletionException error = expectThrows(CompletionException.class, scan::join);
        assertThat(error).hasCauseInstanceOf(IllegalStateException.class);
        assertEquals(callbacks, List.of("error"));
        verify(client, never()).get(anyString(), anySet());
    }

    private static ScanConsumer recordingConsumer(List<String> callbacks) {
        return new ScanConsumer() {
            @Override
            public void onNext(org.apache.pulsar.metadata.api.GetResult result) {
                callbacks.add("next:" + result.getStat().getPath());
            }

            @Override
            public void onError(Throwable throwable) {
                callbacks.add("error");
            }

            @Override
            public void onCompleted() {
                callbacks.add("completed");
            }
        };
    }

    private static io.oxia.client.api.GetResult getResult(String path) {
        return new io.oxia.client.api.GetResult(path, new byte[0],
                new Version(1, 0, 0, 1, Optional.empty(), Optional.empty()));
    }
}
