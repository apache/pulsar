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

import static org.apache.bookkeeper.mledger.util.ManagedLedgerTestUtil.defaultConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.mledger.AsyncCallbacks.UpdatePropertiesCallback;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.intercept.ManagedLedgerInterceptor;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.impl.FaultInjectionMetadataStore;
import org.testng.annotations.Test;

public class ManagedLedgerPropertiesInterceptorTest extends MockedBookKeeperTestCase {

    @Test(timeOut = 20000)
    @SuppressWarnings("unchecked")
    public void testInterceptorChangesPublishWithSuccessfulPropertiesUpdate() throws Exception {
        String ledgerName = "properties-interceptor-publication";
        String ledgerPath = "/managed-ledgers/" + ledgerName;
        AtomicReference<String> interceptorValue = new AtomicReference<>("old-index");
        ManagedLedgerInterceptor interceptor = mock(ManagedLedgerInterceptor.class);
        doAnswer(invocation -> {
            Map<String, String> properties = invocation.getArgument(0);
            properties.put("interceptor-index", interceptorValue.get());
            return null;
        }).when(interceptor).onUpdateManagedLedgerInfo(anyMap());

        CompletableFuture<Void> failedWriteStarted = new CompletableFuture<>();
        CompletableFuture<Stat> failedWriteResult = new CompletableFuture<>();
        CompletableFuture<Stat> retryStored = new CompletableFuture<>();
        CompletableFuture<Stat> retryResult = new CompletableFuture<>();
        AtomicBoolean failNextPut = new AtomicBoolean();
        AtomicBoolean pauseNextPut = new AtomicBoolean();
        FaultInjectionMetadataStore spyStore = spy(metadataStore);
        doAnswer(invocation -> {
            if (failNextPut.compareAndSet(true, false)) {
                failedWriteStarted.complete(null);
                return failedWriteResult;
            }
            if (pauseNextPut.compareAndSet(true, false)) {
                CompletableFuture<Stat> stored = (CompletableFuture<Stat>) invocation.callRealMethod();
                stored.whenComplete((stat, error) -> {
                    if (error == null) {
                        retryStored.complete(stat);
                    } else {
                        retryStored.completeExceptionally(error);
                    }
                });
                return retryResult;
            }
            return invocation.callRealMethod();
        }).when(spyStore).put(eq(ledgerPath), any(byte[].class), any());

        ManagedLedgerFactoryImpl gatedFactory = new ManagedLedgerFactoryImpl(spyStore, bkc);
        try {
            ManagedLedgerConfig config = initManagedLedgerConfig(defaultConfig());
            config.setManagedLedgerInterceptor(interceptor);
            ManagedLedger ledger = gatedFactory.open(ledgerName, config);
            ledger.setProperty("user-key", "old-value");
            Map<String, String> oldProperties = Map.of("user-key", "old-value", "interceptor-index", "old-index");
            Map<String, String> newProperties = Map.of("user-key", "new-value", "interceptor-index", "new-index");
            assertThat(ledger.getProperties()).containsExactlyInAnyOrderEntriesOf(oldProperties);

            interceptorValue.set("new-index");
            failNextPut.set(true);
            CompletableFuture<Map<String, String>> failedUpdate = updateProperty(ledger);
            failedWriteStarted.get(5, TimeUnit.SECONDS);
            assertThat(failedUpdate).isNotDone();
            assertThat(ledger.getProperties()).containsExactlyInAnyOrderEntriesOf(oldProperties);

            failedWriteResult.completeExceptionally(new MetadataStoreException("injected property write failure"));
            assertThatThrownBy(() -> failedUpdate.get(5, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(ManagedLedgerException.class);
            assertThat(ledger.getProperties()).containsExactlyInAnyOrderEntriesOf(oldProperties);
            assertThat(gatedFactory.getMetaStore().getManagedLedgerPropertiesAsync(ledgerName).get(5, TimeUnit.SECONDS))
                    .containsExactlyInAnyOrderEntriesOf(oldProperties);

            // Hold the successful response after storage, so the interceptor's staged value must still be hidden.
            pauseNextPut.set(true);
            CompletableFuture<Map<String, String>> retry = updateProperty(ledger);
            Stat storedVersion = retryStored.get(5, TimeUnit.SECONDS);
            assertThat(retry).isNotDone();
            assertThat(ledger.getProperties()).containsExactlyInAnyOrderEntriesOf(oldProperties);
            assertThat(gatedFactory.getMetaStore().getManagedLedgerPropertiesAsync(ledgerName).get(5, TimeUnit.SECONDS))
                    .containsExactlyInAnyOrderEntriesOf(newProperties);

            retryResult.complete(storedVersion);
            Map<String, String> callbackProperties = retry.get(5, TimeUnit.SECONDS);
            assertThat(callbackProperties).containsExactlyInAnyOrderEntriesOf(newProperties);
            assertThat(ledger.getProperties()).containsExactlyInAnyOrderEntriesOf(newProperties);
            callbackProperties.put("interceptor-index", "callback-change");
            assertThat(ledger.getProperties()).containsExactlyInAnyOrderEntriesOf(newProperties);

            ledger.close();
            // Reopen without the interceptor, so recovery verifies the properties persisted by the update itself.
            ManagedLedger reopenedLedger = gatedFactory.open(ledgerName);
            assertThat(reopenedLedger.getProperties()).containsExactlyInAnyOrderEntriesOf(newProperties);
        } finally {
            failedWriteResult.completeExceptionally(new MetadataStoreException("test cleanup"));
            retryResult.completeExceptionally(new MetadataStoreException("test cleanup"));
            gatedFactory.shutdown();
        }
    }

    private static CompletableFuture<Map<String, String>> updateProperty(ManagedLedger ledger) {
        CompletableFuture<Map<String, String>> result = new CompletableFuture<>();
        ledger.asyncSetProperty("user-key", "new-value", new UpdatePropertiesCallback() {
            @Override
            public void updatePropertiesComplete(Map<String, String> properties, Object ctx) {
                result.complete(properties);
            }

            @Override
            public void updatePropertiesFailed(ManagedLedgerException exception, Object ctx) {
                result.completeExceptionally(exception);
            }
        }, null);
        return result;
    }
}
