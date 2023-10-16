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
package org.apache.bookkeeper.replication;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotSame;
import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.TestBookieImpl;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.apache.bookkeeper.meta.MetadataBookieDriver;
import org.apache.bookkeeper.meta.MetadataDrivers;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * This test verifies that the period check on the auditor
 * will pick up on missing data in the client.
 */
public class AuditorPeriodicCheckTest extends BookKeeperClusterTestCase {
    private static final Logger LOG = LoggerFactory
            .getLogger(AuditorPeriodicCheckTest.class);

    private MetadataBookieDriver driver;
    private HashMap<String, AuditorElector> auditorElectors = new HashMap<String, AuditorElector>();

    private static final int CHECK_INTERVAL = 1; // run every second

    public AuditorPeriodicCheckTest() throws Exception {
        super(3);
        baseConf.setPageLimit(1); // to make it easy to push ledger out of cache
        Class.forName("org.apache.pulsar.metadata.bookkeeper.PulsarMetadataClientDriver");
        Class.forName("org.apache.pulsar.metadata.bookkeeper.PulsarMetadataBookieDriver");
    }

    @BeforeMethod
    @Override
    public void setUp() throws Exception {
        super.setUp();

        for (int i = 0; i < numBookies; i++) {
            ServerConfiguration conf = new ServerConfiguration(confByIndex(i));
            conf.setAuditorPeriodicCheckInterval(CHECK_INTERVAL);
            conf.setMetadataServiceUri(
                    zkUtil.getMetadataServiceUri().replaceAll("zk://", "metadata-store:").replaceAll("/ledgers", ""));

            String addr = addressByIndex(i).toString();

            AuditorElector auditorElector = new AuditorElector(addr, conf);
            auditorElectors.put(addr, auditorElector);
            auditorElector.start();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Starting Auditor Elector");
            }
        }

        URI uri = URI.create(confByIndex(0).getMetadataServiceUri().replaceAll("zk://", "metadata-store:")
                .replaceAll("/ledgers", ""));
        driver = MetadataDrivers.getBookieDriver(uri);
        ServerConfiguration serverConfiguration = new ServerConfiguration(confByIndex(0));
        serverConfiguration.setMetadataServiceUri(
                serverConfiguration.getMetadataServiceUri().replaceAll("zk://", "metadata-store:")
                        .replaceAll("/ledgers", ""));
        driver.initialize(serverConfiguration, NullStatsLogger.INSTANCE);
    }

    @AfterMethod
    @Override
    public void tearDown() throws Exception {
        if (null != driver) {
            driver.close();
        }

        for (AuditorElector e : auditorElectors.values()) {
            e.shutdown();
        }
        super.tearDown();
    }

    private BookieId replaceBookieWithWriteFailingBookie(LedgerHandle lh) throws Exception {
        int bookieIdx = -1;
        Long entryId = lh.getLedgerMetadata().getAllEnsembles().firstKey();
        List<BookieId> curEnsemble = lh.getLedgerMetadata().getAllEnsembles().get(entryId);

        // Identify a bookie in the current ledger ensemble to be replaced
        BookieId replacedBookie = null;
        for (int i = 0; i < numBookies; i++) {
            if (curEnsemble.contains(addressByIndex(i))) {
                bookieIdx = i;
                replacedBookie = addressByIndex(i);
                break;
            }
        }
        assertNotSame("Couldn't find ensemble bookie in bookie list", -1, bookieIdx);

        LOG.info("Killing bookie " + addressByIndex(bookieIdx));
        ServerConfiguration conf = killBookie(bookieIdx);
        Bookie writeFailingBookie = new TestBookieImpl(conf) {
            @Override
            public void addEntry(ByteBuf entry, boolean ackBeforeSync, WriteCallback cb,
                                 Object ctx, byte[] masterKey)
                    throws IOException, BookieException {
                try {
                    LOG.info("Failing write to entry ");
                    // sleep a bit so that writes to other bookies succeed before
                    // the client hears about the failure on this bookie. If the
                    // client gets ack-quorum number of acks first, it won't care
                    // about any failures and won't reform the ensemble.
                    Thread.sleep(100);
                    throw new IOException();
                } catch (InterruptedException ie) {
                    // ignore, only interrupted if shutting down,
                    // and an exception would spam the logs
                    Thread.currentThread().interrupt();
                }
            }
        };
        startAndAddBookie(conf, writeFailingBookie);
        return replacedBookie;
    }

    /*
     * Validates that the periodic ledger check will fix entries with a failed write.
     */
    @Test
    public void testFailedWriteRecovery() throws Exception {
        LedgerManagerFactory mFactory = driver.getLedgerManagerFactory();
        LedgerUnderreplicationManager underReplicationManager = mFactory.newLedgerUnderreplicationManager();
        underReplicationManager.disableLedgerReplication();

        LedgerHandle lh = bkc.createLedger(2, 2, 1, DigestType.CRC32, "passwd".getBytes());

        // kill one of the bookies and replace it with one that rejects write;
        // This way we get into the under replication state
        BookieId replacedBookie = replaceBookieWithWriteFailingBookie(lh);

        // Write a few entries; this should cause under replication
        byte[] data = "foobar".getBytes();
        data = "foobar".getBytes();
        lh.addEntry(data);
        lh.addEntry(data);
        lh.addEntry(data);

        lh.close();

        // enable under replication detection and wait for it to report
        // under replicated ledger
        underReplicationManager.enableLedgerReplication();
        long underReplicatedLedger = -1;
        for (int i = 0; i < 5; i++) {
            underReplicatedLedger = underReplicationManager.pollLedgerToRereplicate();
            if (underReplicatedLedger != -1) {
                break;
            }
            Thread.sleep(CHECK_INTERVAL * 1000);
        }
        assertEquals("Ledger should be under replicated", lh.getId(), underReplicatedLedger);

        // now start the replication workers
        List<ReplicationWorker> l = new ArrayList<ReplicationWorker>();
        for (int i = 0; i < numBookies; i++) {
            ReplicationWorker rw = new ReplicationWorker(confByIndex(i), NullStatsLogger.INSTANCE);
            rw.start();
            l.add(rw);
        }
        underReplicationManager.close();

        // Wait for ensemble to change after replication
        Thread.sleep(3000);
        for (ReplicationWorker rw : l) {
            rw.shutdown();
        }

        // check that ensemble has changed and the bookie that rejected writes has
        // been replaced in the ensemble
        LedgerHandle newLh = bkc.openLedger(lh.getId(), DigestType.CRC32, "passwd".getBytes());
        for (Map.Entry<Long, ? extends List<BookieId>> e :
                newLh.getLedgerMetadata().getAllEnsembles().entrySet()) {
            List<BookieId> ensemble = e.getValue();
            assertFalse("Ensemble hasn't been updated", ensemble.contains(replacedBookie));
        }
        newLh.close();
    }
}
