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

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import io.netty.buffer.ByteBuf;
import java.nio.charset.Charset;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups = "broker")
@Slf4j
public class DuplicateManagedLedgerTest extends MockedBookKeeperTestCase {

    private void triggerLedgerRollover(ManagedLedger ledger, int maxEntriesPerLedger) {
        new Thread(() -> {
            int writeLedgerCount = 2;
            for (int i = 0; i < writeLedgerCount; i++) {
                for (int j = 0; j < maxEntriesPerLedger; j++) {
                    byte[] data = String.format("%s_%s", i, j).getBytes(Charset.defaultCharset());
                    Object ctx = "";
                    ledger.asyncAddEntry(data, new AsyncCallbacks.AddEntryCallback() {
                        @Override
                        public void addComplete(Position position, ByteBuf entryData, Object ctx) {

                        }

                        @Override
                        public void addFailed(ManagedLedgerException exception, Object ctx) {

                        }
                    }, ctx);
                }
            }
        }).start();
    }

    @Test
    public void testConcurrentCloseLedgerAndSwitchLedgerForReproduceIssue() throws Exception {
        String managedLedgerName = "lg_" + UUID.randomUUID().toString().replaceAll("-", "_");
        int maxEntriesPerLedger = 5;
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setThrottleMarkDelete(1);
        config.setMaximumRolloverTime(Integer.MAX_VALUE, TimeUnit.SECONDS);
        config.setMaxEntriesPerLedger(5);
        final ProcessCoordinator processCoordinator = new ProcessCoordinator();

        // call "switch ledger" and "managedLedger.close" concurrently.
        final ManagedLedgerImpl managedLedger1 = (ManagedLedgerImpl) factory.open(managedLedgerName, config);
        waitManagedLedgerStateEquals(managedLedger1, ManagedLedgerImpl.State.LedgerOpened);
        processCoordinator.on();
        final ManagedLedgerImpl sequentiallyManagedLedger1 =
                makeManagedLedgerWorksWithStrictlySequentially(managedLedger1, processCoordinator);
        triggerLedgerRollover(sequentiallyManagedLedger1, maxEntriesPerLedger);
        sequentiallyManagedLedger1.close();
        waitManagedLedgerStateNotEquals(managedLedger1, ManagedLedgerImpl.State.Closed);

        // create managedLedger2.
        final ManagedLedgerImpl managedLedger2 = (ManagedLedgerImpl) factory.open(managedLedgerName, config);
        Assert.assertEquals(factory.ledgers.size(), 1);
        Assert.assertNotEquals(managedLedger1, managedLedger2);
        waitManagedLedgerInFactoryEquals(managedLedger2);
        processCoordinator.off();
        managedLedger1.close();
        waitManagedLedgerStateEquals(managedLedger1, ManagedLedgerImpl.State.Closed);
        Assert.assertFalse(factory.ledgers.isEmpty());
        Assert.assertEquals(factory.ledgers.get(managedLedger2.getName()).join(), managedLedger2);

        // cleanup.
        managedLedger2.close();
    }

    private ManagedLedgerImpl makeManagedLedgerWorksWithStrictlySequentially(ManagedLedgerImpl originalManagedLedger,
                                                                             ProcessCoordinator processCoordinator)
            throws Exception {
        ManagedLedgerImpl sequentiallyManagedLedger = spy(originalManagedLedger);
        // step-1.
        doAnswer(invocation -> {
            synchronized (originalManagedLedger) {
                // step-3.
                // Wait for `managedLedger.close`, then do task: "asyncCreateLedger()".
                // Because the thread selector in "managedLedger.executor" is random logic, so it is possible to fail.
                // Adding 1000 tasks to stuck the executor gives a high chance of success.
                for (int i = 0; i < 1000; i++) {
                    originalManagedLedger.getExecutor().execute(() -> {
                        processCoordinator.waitPreviousAndSetStep(3);
                    });
                }
                LedgerHandle lh = (LedgerHandle) invocation.getArguments()[0];
                processCoordinator.waitPreviousAndSetStep(1);
                originalManagedLedger.ledgerClosed(lh);
            }
            return null;
        }).when(sequentiallyManagedLedger).ledgerClosed(any(LedgerHandle.class));
        // step-2.
        doAnswer(invocation -> {
            processCoordinator.waitPreviousAndSetStep(2);
            originalManagedLedger.close();
            return null;
        }).when(sequentiallyManagedLedger).close();
        return sequentiallyManagedLedger;
    }

    private void waitManagedLedgerInFactoryEquals(ManagedLedgerImpl managedLedger){
        Awaitility.await().until(() -> {
            CompletableFuture<ManagedLedgerImpl> managedLedgerFuture = factory.ledgers.get(managedLedger.getName());
            return managedLedgerFuture.join() == managedLedger;
        });
    }

    private void waitManagedLedgerStateEquals(ManagedLedgerImpl managedLedger, ManagedLedgerImpl.State expectedStat){
        Awaitility.await().untilAsserted(() ->
                Assert.assertTrue(managedLedger.getState() == expectedStat));
    }

    private void waitManagedLedgerStateNotEquals(ManagedLedgerImpl managedLedger, ManagedLedgerImpl.State expectedStat){
        Awaitility.await().untilAsserted(() ->
                Assert.assertTrue(managedLedger.getState() != expectedStat));
    }

    private static class ProcessCoordinator {

        private AtomicBoolean latch = new AtomicBoolean(true);

        private AtomicInteger step = new AtomicInteger();

        public boolean waitPreviousAndSetStep(int currentStep){
            if (!latch.get()){
                return false;
            }
            int previousStep = currentStep - 1;
            while (true){
                if (step.compareAndSet(previousStep, currentStep)){
                    return true;
                }
                if (step.get() >= currentStep){
                    return false;
                }
                try {
                    Thread.sleep(20);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        public void on() {
            latch.set(true);
        }

        public void off() {
            latch.set(false);
        }
    }
}
