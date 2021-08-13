/**
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
import static org.testng.Assert.assertTrue;

import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.testng.annotations.Test;

public class ManagedLedgerMBeanTest extends MockedBookKeeperTestCase {

    private void waitForRefresh(ManagedLedgerMBeanImpl mbean) {
        try {
            Thread.sleep(100);
        } catch (Exception e) {
            // do nothing
        }
    }

    @Test
    public void simple() throws Exception {
        ManagedLedgerFactoryConfig config = new ManagedLedgerFactoryConfig();
        config.setMaxCacheSize(0);
        ManagedLedgerFactoryImpl factory = new ManagedLedgerFactoryImpl(metadataStore, bkc, config);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("my_test_ledger");
        ManagedCursor cursor = ledger.openCursor("c1");
        ManagedLedgerMBeanImpl mbean = ledger.mbean;

        assertEquals(mbean.getName(), "my_test_ledger");

        assertEquals(mbean.getStoredMessagesSize(), 0);
        assertEquals(mbean.getNumberOfMessagesInBacklog(), 0);

        waitForRefresh(mbean);

        mbean.addAddEntryLatencySample(1, TimeUnit.MILLISECONDS);
        mbean.addAddEntryLatencySample(10, TimeUnit.MILLISECONDS);
        mbean.addAddEntryLatencySample(1, TimeUnit.SECONDS);
        
        mbean.addLedgerAddEntryLatencySample(1, TimeUnit.MILLISECONDS);
        mbean.addLedgerAddEntryLatencySample(10, TimeUnit.MILLISECONDS);
        mbean.addLedgerAddEntryLatencySample(1, TimeUnit.SECONDS);

        mbean.addLedgerSwitchLatencySample(1, TimeUnit.MILLISECONDS);
        mbean.addLedgerSwitchLatencySample(10, TimeUnit.MILLISECONDS);
        mbean.addLedgerSwitchLatencySample(1, TimeUnit.SECONDS);

        // Simulate stats getting update from different thread
        factory.scheduledExecutor.submit(() -> {
            mbean.refreshStats(1, TimeUnit.SECONDS);
        }).get();

        assertEquals(mbean.getAddEntryBytesRate(), 0.0);
        assertEquals(mbean.getAddEntryWithReplicasBytesRate(), 0.0);
        assertEquals(mbean.getAddEntryMessagesRate(), 0.0);
        assertEquals(mbean.getAddEntrySucceed(), 0);
        assertEquals(mbean.getAddEntryErrors(), 0);
        assertEquals(mbean.getReadEntriesBytesRate(), 0.0);
        assertEquals(mbean.getReadEntriesRate(), 0.0);
        assertEquals(mbean.getReadEntriesSucceeded(), 0);
        assertEquals(mbean.getReadEntriesErrors(), 0);
        assertEquals(mbean.getMarkDeleteRate(), 0.0);

        assertEquals(mbean.getAddEntryLatencyBuckets(), new long[] { 0, 1, 0, 1, 0, 0, 0, 0, 1, 0 });
        assertEquals(mbean.getAddEntryLatencyAverageUsec(), 337_000.0);
        assertEquals(mbean.getLedgerAddEntryLatencyBuckets(), new long[] { 0, 1, 0, 1, 0, 0, 0, 0, 1, 0 });
        assertEquals(mbean.getLedgerAddEntryLatencyAverageUsec(), 337_000.0);
        assertEquals(mbean.getEntrySizeBuckets(), new long[] { 0, 0, 0, 0, 0, 0, 0, 0, 0 });

        assertEquals(mbean.getLedgerSwitchLatencyBuckets(), new long[] { 0, 1, 0, 1, 0, 0, 0, 0, 1, 0 });
        assertEquals(mbean.getLedgerSwitchLatencyAverageUsec(), 337_000.0);

        Position p1 = ledger.addEntry(new byte[200]);
        ledger.addEntry(new byte[600]);
        cursor.markDelete(p1);

        factory.scheduledExecutor.submit(() -> {
            mbean.refreshStats(1, TimeUnit.SECONDS);
        }).get();

        assertEquals(mbean.getAddEntryBytesRate(), 800.0);
        assertEquals(mbean.getAddEntryWithReplicasBytesRate(), 1600.0);
        assertEquals(mbean.getAddEntryMessagesRate(), 2.0);
        assertEquals(mbean.getAddEntrySucceed(), 2);
        assertEquals(mbean.getAddEntryErrors(), 0);
        assertEquals(mbean.getReadEntriesBytesRate(), 0.0);
        assertEquals(mbean.getReadEntriesRate(), 0.0);
        assertEquals(mbean.getReadEntriesSucceeded(), 0);
        assertEquals(mbean.getReadEntriesErrors(), 0);
        assertTrue(mbean.getMarkDeleteRate() > 0.0);

        assertEquals(mbean.getEntrySizeBuckets(), new long[] { 0, 1, 1, 0, 0, 0, 0, 0, 0 });

        mbean.recordAddEntryError();
        mbean.recordReadEntriesError();

        factory.scheduledExecutor.submit(() -> {
            mbean.refreshStats(1, TimeUnit.SECONDS);
        }).get();

        assertEquals(mbean.getAddEntryErrors(), 1);
        assertEquals(mbean.getReadEntriesErrors(), 1);

        List<Entry> entries = cursor.readEntries(100);
        assertEquals(entries.size(), 1);

        factory.scheduledExecutor.submit(() -> {
            mbean.refreshStats(1, TimeUnit.SECONDS);
        }).get();

        assertEquals(mbean.getReadEntriesBytesRate(), 600.0);
        assertEquals(mbean.getReadEntriesRate(), 1.0);
        assertEquals(mbean.getReadEntriesSucceeded(), 1);
        assertEquals(mbean.getReadEntriesErrors(), 0);
        assertEquals(mbean.getNumberOfMessagesInBacklog(), 1);
        assertEquals(mbean.getMarkDeleteRate(), 0.0);

        factory.shutdown();
    }

}
