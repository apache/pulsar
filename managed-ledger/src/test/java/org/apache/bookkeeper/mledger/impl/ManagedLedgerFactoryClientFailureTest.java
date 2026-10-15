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
package org.apache.bookkeeper.mledger.impl;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.PulsarMockBookKeeper;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OpenLedgerCallback;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ManagedLedgerFactoryClientFailureTest {

    @DataProvider
    public Object[][] clientFailureTiming() {
        return new Object[][] {{true, 1}, {false, 1}, {false, 2}};
    }

    @Test(dataProvider = "clientFailureTiming")
    public void testRetryFromClientFailureCallback(boolean immediateFailure, int waitingCallers) throws Exception {
        OrderedExecutor executor = OrderedExecutor.newBuilder().numThreads(1)
                .name("test-client-failure-bk").build();
        try (MetadataStoreExtended store = MetadataStoreExtended.create(
                "memory:client-failure-" + immediateFailure + "-" + waitingCallers,
                MetadataStoreConfig.builder().build());
             PulsarMockBookKeeper client = new PulsarMockBookKeeper(executor)) {
            RuntimeException clientFailure = new RuntimeException("temporary client initialization failure");
            CompletableFuture<BookKeeper> firstClient = new CompletableFuture<>();
            if (immediateFailure) {
                firstClient.completeExceptionally(clientFailure);
            }
            AtomicInteger clientRequests = new AtomicInteger();
            ManagedLedgerFactoryImpl factory = new ManagedLedgerFactoryImpl(store,
                    policy -> clientRequests.getAndIncrement() == 0
                            ? firstClient : CompletableFuture.completedFuture(client),
                    new ManagedLedgerFactoryConfig());
            try {
                String ledgerName = "retry-after-client-failure";
                List<CompletableFuture<ManagedLedgerException>> firstFailures = new ArrayList<>();
                List<CompletableFuture<ManagedLedger>> retries = new ArrayList<>();
                for (int i = 0; i < waitingCallers; i++) {
                    CompletableFuture<ManagedLedgerException> firstFailure = new CompletableFuture<>();
                    CompletableFuture<ManagedLedger> retry = new CompletableFuture<>();
                    firstFailures.add(firstFailure);
                    retries.add(retry);
                    factory.asyncOpen(ledgerName, new ManagedLedgerConfig(), new OpenLedgerCallback() {
                        @Override
                        public void openLedgerComplete(ManagedLedger ledger, Object ctx) {
                            firstFailure.completeExceptionally(new AssertionError("The first client should fail"));
                        }

                        @Override
                        public void openLedgerFailed(ManagedLedgerException exception, Object ctx) {
                            firstFailure.complete(exception);
                            // Retrying from the callback requires eviction before reporting the original failure.
                            factory.asyncOpen(ledgerName, new ManagedLedgerConfig(), openCallback(retry), null, null);
                        }
                    }, null, null);
                }

                if (!immediateFailure) {
                    firstFailures.forEach(failure -> assertThat(failure).isNotDone());
                    retries.forEach(retry -> assertThat(retry).isNotDone());
                    firstClient.completeExceptionally(clientFailure);
                }

                for (CompletableFuture<ManagedLedgerException> failure : firstFailures) {
                    assertThat(failure.get(5, TimeUnit.SECONDS).getCause()).isSameAs(clientFailure);
                }
                ManagedLedger recovered = retries.get(0).get(5, TimeUnit.SECONDS);
                for (CompletableFuture<ManagedLedger> retry : retries) {
                    assertThat(retry.get(5, TimeUnit.SECONDS)).isSameAs(recovered);
                }
                // A later failed waiter must not evict the future installed by an earlier retry.
                assertThat(clientRequests.get()).isEqualTo(2);
                assertThat(recovered.addEntry(new byte[] {1}).getEntryId()).isZero();
                recovered.close();
            } finally {
                factory.shutdownAsync().get(5, TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdownNow();
        }
    }

    private static OpenLedgerCallback openCallback(CompletableFuture<ManagedLedger> result) {
        return new OpenLedgerCallback() {
            @Override
            public void openLedgerComplete(ManagedLedger ledger, Object ctx) {
                result.complete(ledger);
            }

            @Override
            public void openLedgerFailed(ManagedLedgerException exception, Object ctx) {
                result.completeExceptionally(exception);
            }
        };
    }
}
