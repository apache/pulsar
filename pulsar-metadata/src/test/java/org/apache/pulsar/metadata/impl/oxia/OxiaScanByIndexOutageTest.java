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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import io.oxia.client.api.OxiaClientBuilder;
import io.oxia.testcontainers.OxiaContainer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Cleanup;
import org.apache.pulsar.metadata.OxiaTestServer;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.Option;
import org.apache.pulsar.metadata.api.ScanConsumer;
import org.testng.annotations.Test;

/**
 * An index scan while a 3-shard Oxia is down. The Oxia client retries the scan of a shard it can't reach with no
 * deadline and reports none of the failed attempts, so the store has to end the scan after its request timeout.
 */
public class OxiaScanByIndexOutageTest {

    @Test
    public void scanByIndexFailsAfterTheRequestTimeoutWhileOxiaIsDown() throws Exception {
        OxiaContainer oxiaServer = OxiaTestServer.start(3);
        try {
            // The client gets a longer request timeout than the store, so that only the store can end the scan
            // in time: an attempt that hangs until the client's timeout isn't retried, unlike one that fails fast
            @Cleanup
            OxiaMetadataStore store = new OxiaMetadataStore(
                    OxiaClientBuilder.create(oxiaServer.getServiceAddress())
                            .requestTimeout(Duration.ofSeconds(10))
                            .connectionBackoff(Duration.ofMillis(10), Duration.ofMillis(50))
                            .asyncClient().get(),
                    "test-identity", null, Duration.ofSeconds(1));
            String prefix = "/index-scan-outage-" + System.nanoTime();
            for (int i = 0; i < 10; i++) {
                store.put(prefix + "/r-" + i, ("v-" + i).getBytes(StandardCharsets.UTF_8), Optional.of(-1L),
                        Set.of(new Option.PartitionKey("group-" + i),
                                new Option.SecondaryIndex("by-tag", "tag"))).get();
            }

            oxiaServer.stop();

            AtomicInteger errors = new AtomicInteger();
            AtomicInteger completions = new AtomicInteger();
            CompletableFuture<Void> scan = store.scanByIndex(prefix, "by-tag", "tag", "tag", r -> true,
                    new ScanConsumer() {
                        @Override
                        public void onNext(GetResult result) {
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

            ExecutionException e = expectThrows(ExecutionException.class, () -> scan.get(5, TimeUnit.SECONDS));
            assertTrue(e.getCause() instanceof TimeoutException, "unexpected failure: " + e.getCause());
            assertEquals(errors.get(), 1);
            assertEquals(completions.get(), 0);
        } finally {
            oxiaServer.close();
        }
    }
}
