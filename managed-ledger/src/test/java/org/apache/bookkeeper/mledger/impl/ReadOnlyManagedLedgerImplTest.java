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


import static org.testng.Assert.assertEquals;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ReadOnlyManagedLedger;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.testng.annotations.Test;

public class ReadOnlyManagedLedgerImplTest extends MockedBookKeeperTestCase {
    private static final String MANAGED_LEDGER_NAME_NON_PROPERTIES = "ml-non-properties";
    private static final String MANAGED_LEDGER_NAME_ATTACHED_PROPERTIES = "ml-attached-properties";


    @Test
    public void testReadOnlyManagedLedgerImplAttachProperties()
            throws ManagedLedgerException, InterruptedException, ExecutionException, TimeoutException {
        final ManagedLedger ledger = factory.open(MANAGED_LEDGER_NAME_ATTACHED_PROPERTIES,
                new ManagedLedgerConfig().setRetentionTime(1, TimeUnit.HOURS));
        final String propertiesKey = "test-key";
        final String propertiesValue = "test-value";

        ledger.setConfig(new ManagedLedgerConfig());
        ledger.addEntry("entry-0".getBytes());
        Map<String, String> properties = new HashMap<>();
        properties.put(propertiesKey, propertiesValue);
        ledger.setProperties(Collections.unmodifiableMap(properties));
        CompletableFuture<Void> future = new CompletableFuture<>();
        factory.asyncOpenReadOnlyManagedLedger(MANAGED_LEDGER_NAME_ATTACHED_PROPERTIES,
                new AsyncCallbacks.OpenReadOnlyManagedLedgerCallback() {
                    @Override
                    public void openReadOnlyManagedLedgerComplete(ReadOnlyManagedLedger managedLedger,
                                                                  Object ctx) {
                        managedLedger.getProperties().forEach((key, value) -> {
                            assertEquals(key, propertiesKey);
                            assertEquals(value, propertiesValue);
                        });
                        future.complete(null);
                    }

                    @Override
                    public void openReadOnlyManagedLedgerFailed(ManagedLedgerException exception, Object ctx) {
                        future.completeExceptionally(exception);
                    }
                }, new ManagedLedgerConfig(), null);

        future.get(60, TimeUnit.SECONDS);
    }

    @Test
    public void testReadOnlyManagedLedgerImplNoProperties()
            throws ManagedLedgerException, InterruptedException, ExecutionException, TimeoutException {
        final ManagedLedger ledger = factory.open(MANAGED_LEDGER_NAME_NON_PROPERTIES,
                new ManagedLedgerConfig().setRetentionTime(1, TimeUnit.HOURS));
        ledger.setConfig(new ManagedLedgerConfig());
        ledger.addEntry("entry-0".getBytes());
        CompletableFuture<Void> future = new CompletableFuture<>();
        factory.asyncOpenReadOnlyManagedLedger(MANAGED_LEDGER_NAME_NON_PROPERTIES,
                new AsyncCallbacks.OpenReadOnlyManagedLedgerCallback() {
                    @Override
                    public void openReadOnlyManagedLedgerComplete(ReadOnlyManagedLedger managedLedger,
                                                                  Object ctx) {
                        assertEquals(managedLedger.getProperties().size(), 0);
                        future.complete(null);
                    }

                    @Override
                    public void openReadOnlyManagedLedgerFailed(ManagedLedgerException exception, Object ctx) {
                        future.completeExceptionally(exception);
                    }
                }, new ManagedLedgerConfig(), null);

        future.get(60, TimeUnit.SECONDS);
    }

}