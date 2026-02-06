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
package org.apache.bookkeeper.replication;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;
import java.util.Enumeration;
import java.util.List;
import java.util.Map.Entry;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.util.BookKeeperConstants;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Test auto recovery.
 */
public class TestAutoRecoveryAlongWithBookieServers extends
        BookKeeperClusterTestCase {

    private String basePath = "";

    public TestAutoRecoveryAlongWithBookieServers() throws Exception {
        super(3);
        setAutoRecoveryEnabled(true);
        Class.forName("org.apache.pulsar.metadata.bookkeeper.PulsarMetadataClientDriver");
        Class.forName("org.apache.pulsar.metadata.bookkeeper.PulsarMetadataBookieDriver");
    }

    @BeforeMethod
    @Override
    public void setUp() throws Exception {
        super.setUp();
        basePath = BookKeeperConstants.DEFAULT_ZK_LEDGERS_ROOT_PATH + '/'
                + BookKeeperConstants.UNDER_REPLICATION_NODE
                + BookKeeperConstants.DEFAULT_ZK_LEDGERS_ROOT_PATH;
    }

    @AfterMethod
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    @Override
    protected void startBKCluster(String metadataServiceUri) throws Exception {
        super.startBKCluster(metadataServiceUri.replaceAll("zk://", "metadata-store:").replaceAll("/ledgers", ""));
    }

    /**
     * Tests that the auto recovery service along with Bookie servers itself.
     */
    @Test
    public void testAutoRecoveryAlongWithBookieServers() throws Exception {
        LedgerHandle lh = bkc.createLedger(3, 3, BookKeeper.DigestType.CRC32,
                "testpasswd".getBytes());
        byte[] testData = "testBuiltAutoRecovery".getBytes();

        for (int i = 0; i < 10; i++) {
            lh.addEntry(testData);
        }
        lh.close();
        BookieId replicaToKill = lh.getLedgerMetadata().getAllEnsembles().get(0L).get(0);

        killBookie(replicaToKill);

        BookieId newBkAddr = startNewBookieAndReturnBookieId();

        while (ReplicationTestUtil.isLedgerInUnderReplication(zkc, lh.getId(),
                basePath)) {
            Thread.sleep(100);
        }

        // Killing all bookies except newly replicated bookie
        for (Entry<Long, ? extends List<BookieId>> entry :
                lh.getLedgerMetadata().getAllEnsembles().entrySet()) {
            List<BookieId> bookies = entry.getValue();
            for (BookieId bookie : bookies) {
                if (bookie.equals(newBkAddr)) {
                    continue;
                }
                killBookie(bookie);
            }
        }

        // Should be able to read the entries from 0-9
        LedgerHandle lhs = bkc.openLedgerNoRecovery(lh.getId(),
                BookKeeper.DigestType.CRC32, "testpasswd".getBytes());
        Enumeration<LedgerEntry> entries = lhs.readEntries(0, 9);
        assertTrue("Should have the elements", entries.hasMoreElements());
        while (entries.hasMoreElements()) {
            LedgerEntry entry = entries.nextElement();
            assertEquals("testBuiltAutoRecovery", new String(entry.getEntry()));
        }
    }
}
