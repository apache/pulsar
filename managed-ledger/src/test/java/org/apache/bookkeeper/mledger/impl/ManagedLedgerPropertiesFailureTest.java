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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.bookkeeper.mledger.util.ManagedLedgerTestUtil.defaultConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.bookkeeper.mledger.AsyncCallbacks.UpdatePropertiesCallback;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.ManagedLedgerAlreadyClosedException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.ManagedLedgerFencedException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.impl.FaultInjectionMetadataStore;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ManagedLedgerPropertiesFailureTest extends MockedBookKeeperTestCase {

    @Test(timeOut = 20000)
    public void testPendingPropertiesUpdatesFailWhenRolloverFencesLedger() throws Exception {
        String ledgerName = "properties-fenced-during-rollover";
        String ledgerPath = "/managed-ledgers/" + ledgerName;
        CompletableFuture<Stat> rolloverUpdate = new CompletableFuture<>();
        CountDownLatch rolloverUpdateStarted = new CountDownLatch(1);
        AtomicBoolean interceptNextPut = new AtomicBoolean();
        FaultInjectionMetadataStore spyStore = spy(metadataStore);
        doAnswer(invocation -> {
            if (interceptNextPut.compareAndSet(true, false)) {
                rolloverUpdateStarted.countDown();
                return rolloverUpdate;
            }
            return invocation.callRealMethod();
        }).when(spyStore).put(eq(ledgerPath), any(byte[].class), any());

        ManagedLedgerFactoryImpl gatedFactory = new ManagedLedgerFactoryImpl(spyStore, bkc);
        try {
            ManagedLedgerConfig config = initManagedLedgerConfig(defaultConfig()).setMaxEntriesPerLedger(1);
            ManagedLedgerImpl ledger = (ManagedLedgerImpl) gatedFactory.open(ledgerName, config);
            ledger.setProperty("existing", "value");

            // Filling the ledger starts an actual rollover whose metadata update holds the mutex.
            interceptNextPut.set(true);
            ledger.addEntry("entry".getBytes(UTF_8));
            assertThat(rolloverUpdateStarted.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(ledger.getState()).isEqualTo(ManagedLedgerImpl.State.CreatingLedger);

            CompletableFuture<Position> migration = ledger.asyncMigrate();
            CompletableFuture<Position> coalescedMigration = ledger.asyncMigrate();
            CompletableFuture<Map<String, String>> propertyUpdate = updatePropertiesAsync(
                    callback -> ledger.asyncSetProperty("pending", "value", callback, null));
            assertThat(migration).isNotDone();
            assertThat(coalescedMigration).isNotDone();
            assertThat(propertyUpdate).isNotDone();

            // A rollover version conflict fences the ledger and intentionally leaves the metadata mutex locked.
            rolloverUpdate.completeExceptionally(new MetadataStoreException.BadVersionException("injected failure"));
            assertFailedWith(migration, ManagedLedgerFencedException.class);
            assertFailedWith(coalescedMigration, ManagedLedgerFencedException.class);
            assertFailedWith(propertyUpdate, ManagedLedgerFencedException.class);
            assertThat(ledger.getState().isFenced()).isTrue();
            assertThat(ledger.getProperties()).containsExactlyEntriesOf(Map.of("existing", "value"));
            assertThat(ledger.isMigrated()).isFalse();
        } finally {
            rolloverUpdate.completeExceptionally(new MetadataStoreException("test cleanup"));
            gatedFactory.shutdown();
        }
    }

    @Test(timeOut = 20000)
    public void testClosedLedgerRejectsPropertiesUpdatesAndMigration() throws Exception {
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("properties-after-close");
        ledger.setProperty("existing", "value");
        ledger.close();

        assertFailedWith(updatePropertiesAsync(
                callback -> ledger.asyncSetProperty("new", "value", callback, null)),
                ManagedLedgerAlreadyClosedException.class);
        assertFailedWith(updatePropertiesAsync(
                callback -> ledger.asyncSetProperties(Map.of("new", "value"), callback, null)),
                ManagedLedgerAlreadyClosedException.class);
        assertFailedWith(updatePropertiesAsync(
                callback -> ledger.asyncDeleteProperty("existing", callback, null)),
                ManagedLedgerAlreadyClosedException.class);
        assertFailedWith(ledger.asyncMigrate(), ManagedLedgerAlreadyClosedException.class);
        assertThat(ledger.getProperties()).containsExactlyEntriesOf(Map.of("existing", "value"));

        ManagedLedgerImpl reopenedLedger = (ManagedLedgerImpl) factory.open("properties-after-close");
        assertThat(reopenedLedger.getProperties()).containsExactlyEntriesOf(Map.of("existing", "value"));
        assertThat(reopenedLedger.isMigrated()).isFalse();
    }

    @DataProvider(name = "migrationOrPropertyUpdate")
    public Object[][] migrationOrPropertyUpdate() {
        return new Object[][] {{true}, {false}};
    }

    @Test(timeOut = 20000, dataProvider = "migrationOrPropertyUpdate")
    public void testSynchronousPropertiesFailureReleasesMetadataMutex(boolean migration) throws Exception {
        String ledgerName = "properties-synchronous-failure";
        String ledgerPath = "/managed-ledgers/" + ledgerName;
        AtomicBoolean throwNextPut = new AtomicBoolean();
        IllegalStateException storageFailure = new IllegalStateException("injected synchronous metadata failure");
        FaultInjectionMetadataStore spyStore = spy(metadataStore);
        doAnswer(invocation -> {
            if (throwNextPut.compareAndSet(true, false)) {
                throw storageFailure;
            }
            return invocation.callRealMethod();
        }).when(spyStore).put(eq(ledgerPath), any(byte[].class), any());

        ManagedLedgerFactoryImpl throwingFactory = new ManagedLedgerFactoryImpl(spyStore, bkc);
        try {
            ManagedLedgerImpl ledger = (ManagedLedgerImpl) throwingFactory.open(ledgerName);
            ledger.setProperty("existing", "value");
            Position lastPosition = ledger.addEntry("entry".getBytes(UTF_8));

            throwNextPut.set(true);
            CompletableFuture<?> firstUpdate;
            AtomicInteger failureCallbacks = new AtomicInteger();
            if (migration) {
                firstUpdate = ledger.asyncMigrate();
            } else {
                CompletableFuture<Map<String, String>> propertyUpdate = new CompletableFuture<>();
                firstUpdate = propertyUpdate;
                // Neither the storage failure nor a throwing failure callback may escape this asynchronous API.
                ledger.asyncSetProperty("failed", "value", new UpdatePropertiesCallback() {
                    @Override
                    public void updatePropertiesComplete(Map<String, String> properties, Object ctx) {
                        propertyUpdate.complete(properties);
                    }

                    @Override
                    public void updatePropertiesFailed(ManagedLedgerException exception, Object ctx) {
                        failureCallbacks.incrementAndGet();
                        propertyUpdate.completeExceptionally(exception);
                        throw new IllegalStateException("injected failure callback exception");
                    }
                }, null);
            }
            assertThatThrownBy(() -> firstUpdate.get(5, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(ManagedLedgerException.class)
                    .hasRootCause(storageFailure);
            if (!migration) {
                assertThat(failureCallbacks.get()).isEqualTo(1);
            }

            boolean mutexReleased = ledger.metadataMutex.tryLock();
            if (mutexReleased) {
                try {
                    boolean duplicatePermit = ledger.metadataMutex.tryLock();
                    if (duplicatePermit) {
                        ledger.metadataMutex.unlock();
                    }
                    assertThat(duplicatePermit).as("The metadata mutex must be released exactly once").isFalse();
                } finally {
                    ledger.metadataMutex.unlock();
                }
            }
            assertThat(mutexReleased)
                    .as("A synchronous properties failure must release the metadata mutex")
                    .isTrue();
            assertThat(ledger.getProperties()).containsExactlyEntriesOf(Map.of("existing", "value"));

            assertThat(updatePropertiesAsync(
                    callback -> ledger.asyncSetProperty("after-failure", "value", callback, null))
                    .get(5, TimeUnit.SECONDS))
                    .containsExactlyInAnyOrderEntriesOf(Map.of("existing", "value", "after-failure", "value"));
            assertThat(ledger.asyncMigrate().get(5, TimeUnit.SECONDS)).isEqualTo(lastPosition);
        } finally {
            throwingFactory.shutdown();
        }
    }

    private static CompletableFuture<Map<String, String>> updatePropertiesAsync(
            Consumer<UpdatePropertiesCallback> update) {
        CompletableFuture<Map<String, String>> result = new CompletableFuture<>();
        update.accept(new UpdatePropertiesCallback() {
            @Override
            public void updatePropertiesComplete(Map<String, String> properties, Object ctx) {
                result.complete(properties);
            }

            @Override
            public void updatePropertiesFailed(ManagedLedgerException exception, Object ctx) {
                result.completeExceptionally(exception);
            }
        });
        return result;
    }

    private static void assertFailedWith(CompletableFuture<?> future, Class<? extends Throwable> cause) {
        assertThatThrownBy(() -> future.get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(cause);
    }
}
