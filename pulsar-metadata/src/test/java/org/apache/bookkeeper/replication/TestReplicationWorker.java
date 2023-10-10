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

import static org.apache.bookkeeper.replication.ReplicationStats.AUDITOR_SCOPE;
import static org.apache.bookkeeper.replication.ReplicationStats.NUM_ENTRIES_UNABLE_TO_READ_FOR_REPLICATION;
import static org.apache.bookkeeper.replication.ReplicationStats.REPLICATION_SCOPE;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;
import io.netty.util.HashedWheelTimer;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.Enumeration;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.TimerTask;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import lombok.Cleanup;
import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeperTestClient;
import org.apache.bookkeeper.client.ClientUtil;
import org.apache.bookkeeper.client.EnsemblePlacementPolicy;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.RackawareEnsemblePlacementPolicy;
import org.apache.bookkeeper.client.ZoneawareEnsemblePlacementPolicy;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.feature.FeatureProvider;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.apache.bookkeeper.meta.MetadataBookieDriver;
import org.apache.bookkeeper.meta.MetadataClientDriver;
import org.apache.bookkeeper.meta.MetadataDrivers;
import org.apache.bookkeeper.meta.ZkLedgerUnderreplicationManager;
import org.apache.bookkeeper.meta.exceptions.MetadataException;
import org.apache.bookkeeper.meta.zk.ZKMetadataDriverBase;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.DNSToSwitchMapping;
import org.apache.bookkeeper.proto.BookieAddressResolver;
import org.apache.bookkeeper.replication.ReplicationException.CompatibilityException;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.test.TestStatsProvider;
import org.apache.bookkeeper.test.TestStatsProvider.TestStatsLogger;
import org.apache.bookkeeper.util.BookKeeperConstants;
import org.apache.bookkeeper.util.StaticDNSResolver;
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.pulsar.metadata.bookkeeper.PulsarLedgerManagerFactory;
import org.apache.pulsar.metadata.bookkeeper.PulsarMetadataClientDriver;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.awaitility.Awaitility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Test the ReplicationWroker, where it has to replicate the fragments from
 * failed Bookies to given target Bookie.
 */
public class TestReplicationWorker extends BookKeeperClusterTestCase {

    private static final byte[] TESTPASSWD = "testpasswd".getBytes();
    private static final Logger LOG = LoggerFactory
            .getLogger(TestReplicationWorker.class);
    private String basePath = "";
    private String baseLockPath = "";
    private MetadataBookieDriver driver;
    private LedgerManagerFactory mFactory;
    private LedgerUnderreplicationManager underReplicationManager;
    private LedgerManager ledgerManager;
    private static byte[] data = "TestReplicationWorker".getBytes();
    private OrderedScheduler scheduler;
    private String zkLedgersRootPath;

    public TestReplicationWorker() throws Exception {
        this("org.apache.pulsar.metadata.bookkeeper.PulsarLedgerManagerFactory");
    }

    TestReplicationWorker(String ledgerManagerFactory) throws Exception {
        super(3, 300);
        LOG.info("Running test case using ledger manager : "
                + ledgerManagerFactory);
        Class.forName("org.apache.pulsar.metadata.bookkeeper.PulsarMetadataClientDriver");
        Class.forName("org.apache.pulsar.metadata.bookkeeper.PulsarMetadataBookieDriver");
        // set ledger manager name
        baseConf.setLedgerManagerFactoryClassName(ledgerManagerFactory);
        baseClientConf.setLedgerManagerFactoryClassName(ledgerManagerFactory);
        baseConf.setRereplicationEntryBatchSize(3);
        baseConf.setZkTimeout(7000);
        baseConf.setZkRetryBackoffMaxMs(500);
        baseConf.setZkRetryBackoffStartMs(10);
    }

    @BeforeMethod
    @Override
    public void setUp() throws Exception {
        super.setUp();
        zkLedgersRootPath = ZKMetadataDriverBase.resolveZkLedgersRootPath(baseClientConf);
        basePath = zkLedgersRootPath + '/'
                + BookKeeperConstants.UNDER_REPLICATION_NODE
                + BookKeeperConstants.DEFAULT_ZK_LEDGERS_ROOT_PATH;
        baseLockPath = zkLedgersRootPath + '/'
                + BookKeeperConstants.UNDER_REPLICATION_NODE
                + "/locks";
        baseClientConf.setMetadataServiceUri(
                zkUtil.getMetadataServiceUri().replaceAll("zk://", "metadata-store:").replaceAll("/ledgers", ""));
        baseConf.setMetadataServiceUri(
                zkUtil.getMetadataServiceUri().replaceAll("zk://", "metadata-store:").replaceAll("/ledgers", ""));
        this.scheduler = OrderedScheduler.newSchedulerBuilder()
                .name("test-scheduler")
                .numThreads(1)
                .build();

        this.driver = MetadataDrivers.getBookieDriver(
                URI.create(baseConf.getMetadataServiceUri()));
        this.driver.initialize(
                baseConf,
                NullStatsLogger.INSTANCE);
        // initialize urReplicationManager
        mFactory = driver.getLedgerManagerFactory();
        ledgerManager = mFactory.newLedgerManager();
        underReplicationManager = mFactory.newLedgerUnderreplicationManager();
    }

    @AfterMethod
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        if (null != ledgerManager) {
            ledgerManager.close();
            ledgerManager = null;
        }
        if (null != underReplicationManager) {
            underReplicationManager.close();
            underReplicationManager = null;
        }
        if (null != driver) {
            driver.close();
        }
        if (null != scheduler) {
            scheduler.shutdown();
            scheduler = null;
        }
        if (null != mFactory) {
            mFactory.close();
        }
    }

    /**
     * Tests that replication worker should replicate the failed bookie
     * fragments to target bookie given to the worker.
     */
    @Test
    public void testRWShouldReplicateFragmentsToTargetBookie() throws Exception {
        LedgerHandle lh = bkc.createLedger(3, 3, BookKeeper.DigestType.CRC32,
                TESTPASSWD);

        for (int i = 0; i < 10; i++) {
            lh.addEntry(data);
        }
        BookieId replicaToKill = lh.getLedgerMetadata().getAllEnsembles().get(0L).get(0);

        LOG.info("Killing Bookie : {}", replicaToKill);
        killBookie(replicaToKill);

        BookieId newBkAddr = startNewBookieAndReturnBookieId();
        LOG.info("New Bookie addr : {}", newBkAddr);

        for (int i = 0; i < 10; i++) {
            lh.addEntry(data);
        }

        ReplicationWorker rw = new ReplicationWorker(baseConf);

        rw.start();
        try {

            underReplicationManager.markLedgerUnderreplicated(lh.getId(),
                    replicaToKill.toString());

            while (ReplicationTestUtil.isLedgerInUnderReplication(zkc, lh
                    .getId(), basePath)) {
                Thread.sleep(100);
            }

            killAllBookies(lh, newBkAddr);

            // Should be able to read the entries from 0-9
            verifyRecoveredLedgers(lh, 0, 9);
        } finally {
            rw.shutdown();
        }
    }

    /**
     * Tests that replication worker should retry for replication until enough
     * bookies available for replication.
     */
    @Test
    public void testRWShouldRetryUntilThereAreEnoughBksAvailableForReplication()
            throws Exception {
        LedgerHandle lh = bkc.createLedger(1, 1, BookKeeper.DigestType.CRC32,
                TESTPASSWD);

        for (int i = 0; i < 10; i++) {
            lh.addEntry(data);
        }
        lh.close();
        BookieId replicaToKill = lh.getLedgerMetadata().getAllEnsembles().get(0L).get(0);
        LOG.info("Killing Bookie : {}", replicaToKill);
        ServerConfiguration killedBookieConfig = killBookie(replicaToKill);

        BookieId newBkAddr = startNewBookieAndReturnBookieId();
        LOG.info("New Bookie addr :" + newBkAddr);

        killAllBookies(lh, newBkAddr);
        ReplicationWorker rw = new ReplicationWorker(baseConf);

        rw.start();
        try {
            underReplicationManager.markLedgerUnderreplicated(lh.getId(),
                    replicaToKill.toString());
            int counter = 30;
            while (counter-- > 0) {
                assertTrue("Expecting that replication should not complete",
                        ReplicationTestUtil.isLedgerInUnderReplication(zkc, lh
                                .getId(), basePath));
                Thread.sleep(100);
            }
            // restart killed bookie
            startAndAddBookie(killedBookieConfig);
            while (ReplicationTestUtil.isLedgerInUnderReplication(zkc, lh
                    .getId(), basePath)) {
                Thread.sleep(100);
            }
            // Should be able to read the entries from 0-9
            verifyRecoveredLedgers(lh, 0, 9);
        } finally {
            rw.shutdown();
        }
    }

    /**
     * Tests that replication worker1 should take one fragment replication and
     * other replication worker also should compete for the replication.
     */
    @Test
    public void test2RWsShouldCompeteForReplicationOf2FragmentsAndCompleteReplication()
            throws Exception {
        LedgerHandle lh = bkc.createLedger(2, 2, BookKeeper.DigestType.CRC32,
                TESTPASSWD);

        for (int i = 0; i < 10; i++) {
            lh.addEntry(data);
        }
        lh.close();
        BookieId replicaToKill = lh.getLedgerMetadata().getAllEnsembles().get(0L).get(0);
        LOG.info("Killing Bookie : {}", replicaToKill);
        ServerConfiguration killedBookieConfig = killBookie(replicaToKill);

        killAllBookies(lh, null);
        // Starte RW1
        BookieId newBkAddr1 = startNewBookieAndReturnBookieId();
        LOG.info("New Bookie addr : {}", newBkAddr1);
        ReplicationWorker rw1 = new ReplicationWorker(baseConf);

        // Starte RW2
        BookieId newBkAddr2 = startNewBookieAndReturnBookieId();
        LOG.info("New Bookie addr : {}", newBkAddr2);
        ReplicationWorker rw2 = new ReplicationWorker(baseConf);
        rw1.start();
        rw2.start();

        try {
            underReplicationManager.markLedgerUnderreplicated(lh.getId(),
                    replicaToKill.toString());
            int counter = 10;
            while (counter-- > 0) {
                assertTrue("Expecting that replication should not complete",
                        ReplicationTestUtil.isLedgerInUnderReplication(zkc, lh
                                .getId(), basePath));
                Thread.sleep(100);
            }
            // restart killed bookie
            startAndAddBookie(killedBookieConfig);
            while (ReplicationTestUtil.isLedgerInUnderReplication(zkc, lh
                    .getId(), basePath)) {
                Thread.sleep(100);
            }
            // Should be able to read the entries from 0-9
            verifyRecoveredLedgers(lh, 0, 9);
        } finally {
            rw1.shutdown();
            rw2.shutdown();
        }
    }

    /**
     * Tests that Replication worker should clean the leadger under replication
     * node of the ledger already deleted.
     */
    @Test
    public void testRWShouldCleanTheLedgerFromUnderReplicationIfLedgerAlreadyDeleted()
            throws Exception {
        LedgerHandle lh = bkc.createLedger(2, 2, BookKeeper.DigestType.CRC32,
                TESTPASSWD);

        for (int i = 0; i < 10; i++) {
            lh.addEntry(data);
        }
        lh.close();
        BookieId replicaToKill = lh.getLedgerMetadata().getAllEnsembles().get(0L).get(0);
        LOG.info("Killing Bookie : {}", replicaToKill);
        killBookie(replicaToKill);

        BookieId newBkAddr = startNewBookieAndReturnBookieId();
        LOG.info("New Bookie addr : {}", newBkAddr);
        ReplicationWorker rw = new ReplicationWorker(baseConf);
        rw.start();

        try {
            bkc.deleteLedger(lh.getId()); // Deleting the ledger
            // Also mark ledger as in UnderReplication
            underReplicationManager.markLedgerUnderreplicated(lh.getId(),
                    replicaToKill.toString());
            while (ReplicationTestUtil.isLedgerInUnderReplication(zkc, lh
                    .getId(), basePath)) {
                Thread.sleep(100);
            }
        } finally {
            rw.shutdown();
        }

    }

    @Test
    public void testMultipleLedgerReplicationWithReplicationWorker()
            throws Exception {
        // Ledger1
        LedgerHandle lh1 = bkc.createLedger(3, 3, BookKeeper.DigestType.CRC32,
                TESTPASSWD);

        for (int i = 0; i < 10; i++) {
            lh1.addEntry(data);
        }
        BookieId replicaToKillFromFirstLedger = lh1.getLedgerMetadata().getAllEnsembles().get(0L).get(0);

        LOG.info("Killing Bookie : {}", replicaToKillFromFirstLedger);

        // Ledger2
        LedgerHandle lh2 = bkc.createLedger(3, 3, BookKeeper.DigestType.CRC32,
                TESTPASSWD);

        for (int i = 0; i < 10; i++) {
            lh2.addEntry(data);
        }
        BookieId replicaToKillFromSecondLedger = lh2.getLedgerMetadata().getAllEnsembles().get(0L).get(0);

        LOG.info("Killing Bookie : {}", replicaToKillFromSecondLedger);

        // Kill ledger1
        killBookie(replicaToKillFromFirstLedger);
        lh1.close();
        // Kill ledger2
        killBookie(replicaToKillFromFirstLedger);
        lh2.close();

        BookieId newBkAddr = startNewBookieAndReturnBookieId();
        LOG.info("New Bookie addr : {}", newBkAddr);

        ReplicationWorker rw = new ReplicationWorker(baseConf);

        rw.start();
        try {

            // Mark ledger1 and 2 as underreplicated
            underReplicationManager.markLedgerUnderreplicated(lh1.getId(),
                    replicaToKillFromFirstLedger.toString());
            underReplicationManager.markLedgerUnderreplicated(lh2.getId(),
                    replicaToKillFromSecondLedger.toString());

            while (ReplicationTestUtil.isLedgerInUnderReplication(zkc, lh1
                    .getId(), basePath)) {
                Thread.sleep(100);
            }

            while (ReplicationTestUtil.isLedgerInUnderReplication(zkc, lh2
                    .getId(), basePath)) {
                Thread.sleep(100);
            }

            killAllBookies(lh1, newBkAddr);

            // Should be able to read the entries from 0-9
            verifyRecoveredLedgers(lh1, 0, 9);
            verifyRecoveredLedgers(lh2, 0, 9);
        } finally {
            rw.shutdown();
        }

    }

    /**
     * Tests that ReplicationWorker should fence the ledger and release ledger
     * lock after timeout. Then replication should happen normally.
     */
    @Test
    public void testRWShouldReplicateTheLedgersAfterTimeoutIfLastFragmentIsUR()
            throws Exception {
        LedgerHandle lh = bkc.createLedger(3, 3, BookKeeper.DigestType.CRC32,
                TESTPASSWD);

        for (int i = 0; i < 10; i++) {
            lh.addEntry(data);
        }
        BookieId replicaToKill = lh.getLedgerMetadata().getAllEnsembles().get(0L).get(0);

        LOG.info("Killing Bookie : {}", replicaToKill);
        killBookie(replicaToKill);

        BookieId newBkAddr = startNewBookieAndReturnBookieId();
        LOG.info("New Bookie addr : {}", newBkAddr);

        // set to 3s instead of default 30s
        baseConf.setOpenLedgerRereplicationGracePeriod("3000");
        ReplicationWorker rw = new ReplicationWorker(baseConf);

        @Cleanup MetadataClientDriver clientDriver = MetadataDrivers.getClientDriver(
                URI.create(baseClientConf.getMetadataServiceUri()));
        clientDriver.initialize(baseClientConf, scheduler, NullStatsLogger.INSTANCE, Optional.empty());

        LedgerManagerFactory mFactory = clientDriver.getLedgerManagerFactory();

        LedgerUnderreplicationManager underReplicationManager = mFactory
                .newLedgerUnderreplicationManager();
        rw.start();
        try {

            underReplicationManager.markLedgerUnderreplicated(lh.getId(),
                    replicaToKill.toString());
            while (ReplicationTestUtil.isLedgerInUnderReplication(zkc, lh
                    .getId(), basePath)) {
                Thread.sleep(100);
            }
            killAllBookies(lh, newBkAddr);
            // Should be able to read the entries from 0-9
            verifyRecoveredLedgers(lh, 0, 9);
            lh = bkc.openLedgerNoRecovery(lh.getId(),
                    BookKeeper.DigestType.CRC32, TESTPASSWD);
            assertFalse("Ledger must have been closed by RW", ClientUtil
                    .isLedgerOpen(lh));
        } finally {
            rw.shutdown();
            underReplicationManager.close();
        }

    }

    @Test
    public void testBookiesNotAvailableScenarioForReplicationWorker() throws Exception {
        int ensembleSize = 3;
        LedgerHandle lh = bkc.createLedger(ensembleSize, ensembleSize, BookKeeper.DigestType.CRC32, TESTPASSWD);

        int numOfEntries = 7;
        for (int i = 0; i < numOfEntries; i++) {
            lh.addEntry(data);
        }
        lh.close();

        BookieId[] bookiesKilled = new BookieId[ensembleSize];
        ServerConfiguration[] killedBookiesConfig = new ServerConfiguration[ensembleSize];

        // kill all bookies
        for (int i = 0; i < ensembleSize; i++) {
            bookiesKilled[i] = lh.getLedgerMetadata().getAllEnsembles().get(0L).get(i);
            killedBookiesConfig[i] = getBkConf(bookiesKilled[i]);
            LOG.info("Killing Bookie : {}", bookiesKilled[i]);
            killBookie(bookiesKilled[i]);
        }

        // start new bookiesToKill number of bookies
        for (int i = 0; i < ensembleSize; i++) {
            BookieId newBkAddr = startNewBookieAndReturnBookieId();
        }

        // create couple of replicationworkers
        ServerConfiguration newRWConf = new ServerConfiguration(baseConf);
        newRWConf.setLockReleaseOfFailedLedgerGracePeriod("64");
        ReplicationWorker rw1 = new ReplicationWorker(newRWConf);
        ReplicationWorker rw2 = new ReplicationWorker(newRWConf);

        @Cleanup
        MetadataClientDriver clientDriver = MetadataDrivers
                .getClientDriver(URI.create(baseClientConf.getMetadataServiceUri()));
        clientDriver.initialize(baseClientConf, scheduler, NullStatsLogger.INSTANCE, Optional.empty());

        LedgerManagerFactory mFactory = clientDriver.getLedgerManagerFactory();

        LedgerUnderreplicationManager underReplicationManager = mFactory.newLedgerUnderreplicationManager();
        try {
            //mark ledger underreplicated
            for (int i = 0; i < bookiesKilled.length; i++) {
                underReplicationManager.markLedgerUnderreplicated(lh.getId(), bookiesKilled[i].toString());
            }
            while (!ReplicationTestUtil.isLedgerInUnderReplication(zkc, lh.getId(), basePath)) {
                Thread.sleep(100);
            }
            rw1.start();
            rw2.start();

            AtomicBoolean isBookieRestarted = new AtomicBoolean(false);

            (new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        Thread.sleep(3000);
                        isBookieRestarted.set(true);
                        /*
                         * after sleeping for 3000 msecs, restart one of the
                         * bookie, so that replication can succeed.
                         */
                        startBookie(killedBookiesConfig[0]);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            })).start();

            int rw1PrevFailedAttemptsCount = 0;
            int rw2PrevFailedAttemptsCount = 0;
            while (!isBookieRestarted.get()) {
                /*
                 * since all the bookies containing the ledger entries are down
                 * replication wouldnt have succeeded.
                 */
                assertTrue("Ledger: " + lh.getId() + " should be underreplicated",
                        ReplicationTestUtil.isLedgerInUnderReplication(zkc, lh.getId(), basePath));

                // the number of failed attempts should have increased.
                int rw1CurFailedAttemptsCount = rw1.replicationFailedLedgers.get(lh.getId()).get();
                assertTrue(
                        "The current number of failed attempts: " + rw1CurFailedAttemptsCount
                                + " should be greater than or equal to previous value: " + rw1PrevFailedAttemptsCount,
                        rw1CurFailedAttemptsCount >= rw1PrevFailedAttemptsCount);
                rw1PrevFailedAttemptsCount = rw1CurFailedAttemptsCount;

                int rw2CurFailedAttemptsCount = rw2.replicationFailedLedgers.get(lh.getId()).get();
                assertTrue(
                        "The current number of failed attempts: " + rw2CurFailedAttemptsCount
                                + " should be greater than or equal to previous value: " + rw2PrevFailedAttemptsCount,
                        rw2CurFailedAttemptsCount >= rw2PrevFailedAttemptsCount);
                rw2PrevFailedAttemptsCount = rw2CurFailedAttemptsCount;

                Thread.sleep(50);
            }

            /**
             * since one of the killed bookie is restarted, replicationworker
             * should succeed in replicating this under replicated ledger and it
             * shouldn't be under replicated anymore.
             */
            int timeToWaitForReplicationToComplete = 20000;
            int timeWaited = 0;
            while (ReplicationTestUtil.isLedgerInUnderReplication(zkc, lh.getId(), basePath)) {
                Thread.sleep(100);
                timeWaited += 100;
                if (timeWaited == timeToWaitForReplicationToComplete) {
                    fail("Ledger should be replicated by now");
                }
            }

            rw1PrevFailedAttemptsCount = rw1.replicationFailedLedgers.get(lh.getId()).get();
            rw2PrevFailedAttemptsCount = rw2.replicationFailedLedgers.get(lh.getId()).get();
            Thread.sleep(2000);
            // now since the ledger is replicated, number of failed attempts
            // counter shouldn't be increased even after sleeping for sometime.
            assertEquals("rw1 failedattempts", rw1PrevFailedAttemptsCount,
                    rw1.replicationFailedLedgers.get(lh.getId()).get());
            assertEquals("rw2 failed attempts ", rw2PrevFailedAttemptsCount,
                    rw2.replicationFailedLedgers.get(lh.getId()).get());

            /*
             * Since these entries are eventually available, and replication has
             * eventually succeeded, in one of the RW
             * unableToReadEntriesForReplication should be 0.
             */
            int rw1UnableToReadEntriesForReplication = rw1.unableToReadEntriesForReplication.get(lh.getId()).size();
            int rw2UnableToReadEntriesForReplication = rw2.unableToReadEntriesForReplication.get(lh.getId()).size();
            assertTrue(
                    "unableToReadEntriesForReplication in RW1: " + rw1UnableToReadEntriesForReplication
                            + " in RW2: "
                            + rw2UnableToReadEntriesForReplication,
                    (rw1UnableToReadEntriesForReplication == 0)
                            || (rw2UnableToReadEntriesForReplication == 0));
        } finally {
            rw1.shutdown();
            rw2.shutdown();
            underReplicationManager.close();
        }
    }

    class InjectedReplicationWorker extends ReplicationWorker {
        CopyOnWriteArrayList<Long> delayReplicationPeriods;

        public InjectedReplicationWorker(ServerConfiguration conf, StatsLogger statsLogger,
                                         CopyOnWriteArrayList<Long> delayReplicationPeriods)
                throws CompatibilityException, ReplicationException.UnavailableException,
                InterruptedException, IOException {
            super(conf, statsLogger);
            this.delayReplicationPeriods = delayReplicationPeriods;
        }

        @Override
        protected void scheduleTaskWithDelay(TimerTask timerTask, long delayPeriod) {
            delayReplicationPeriods.add(delayPeriod);
            super.scheduleTaskWithDelay(timerTask, delayPeriod);
        }
    }

    @Test
    public void testDeferLedgerLockReleaseForReplicationWorker() throws Exception {
        int ensembleSize = 3;
        LedgerHandle lh = bkc.createLedger(ensembleSize, ensembleSize, BookKeeper.DigestType.CRC32, TESTPASSWD);
        int numOfEntries = 7;
        for (int i = 0; i < numOfEntries; i++) {
            lh.addEntry(data);
        }
        lh.close();

        BookieId[] bookiesKilled = new BookieId[ensembleSize];
        ServerConfiguration[] killedBookiesConfig = new ServerConfiguration[ensembleSize];

        // kill all bookies
        for (int i = 0; i < ensembleSize; i++) {
            bookiesKilled[i] = lh.getLedgerMetadata().getAllEnsembles().get(0L).get(i);
            killedBookiesConfig[i] = getBkConf(bookiesKilled[i]);
            LOG.info("Killing Bookie : {}", bookiesKilled[i]);
            killBookie(bookiesKilled[i]);
        }

        // start new bookiesToKill number of bookies
        for (int i = 0; i < ensembleSize; i++) {
            startNewBookieAndReturnBookieId();
        }

        // create couple of replicationworkers
        long lockReleaseOfFailedLedgerGracePeriod = 64L;
        long baseBackoffForLockReleaseOfFailedLedger = lockReleaseOfFailedLedgerGracePeriod
                / (int) Math.pow(2, ReplicationWorker.NUM_OF_EXPONENTIAL_BACKOFF_RETRIALS);
        ServerConfiguration newRWConf = new ServerConfiguration(baseConf);
        newRWConf.setLockReleaseOfFailedLedgerGracePeriod(Long.toString(lockReleaseOfFailedLedgerGracePeriod));
        newRWConf.setRereplicationEntryBatchSize(1000);
        CopyOnWriteArrayList<Long> rw1DelayReplicationPeriods = new CopyOnWriteArrayList<Long>();
        CopyOnWriteArrayList<Long> rw2DelayReplicationPeriods = new CopyOnWriteArrayList<Long>();
        TestStatsProvider statsProvider = new TestStatsProvider();
        TestStatsLogger statsLogger1 = statsProvider.getStatsLogger("rw1");
        TestStatsLogger statsLogger2 = statsProvider.getStatsLogger("rw2");
        ReplicationWorker rw1 = new InjectedReplicationWorker(newRWConf, statsLogger1, rw1DelayReplicationPeriods);
        ReplicationWorker rw2 = new InjectedReplicationWorker(newRWConf, statsLogger2, rw2DelayReplicationPeriods);

        Counter numEntriesUnableToReadForReplication1 = statsLogger1
                .getCounter(NUM_ENTRIES_UNABLE_TO_READ_FOR_REPLICATION);
        Counter numEntriesUnableToReadForReplication2 = statsLogger2
                .getCounter(NUM_ENTRIES_UNABLE_TO_READ_FOR_REPLICATION);
        @Cleanup
        MetadataClientDriver clientDriver = MetadataDrivers
                .getClientDriver(URI.create(baseClientConf.getMetadataServiceUri()));
        clientDriver.initialize(baseClientConf, scheduler, NullStatsLogger.INSTANCE, Optional.empty());

        LedgerManagerFactory mFactory = clientDriver.getLedgerManagerFactory();

        LedgerUnderreplicationManager underReplicationManager = mFactory.newLedgerUnderreplicationManager();
        try {
            // mark ledger underreplicated
            for (int i = 0; i < bookiesKilled.length; i++) {
                underReplicationManager.markLedgerUnderreplicated(lh.getId(), bookiesKilled[i].toString());
            }
            while (!ReplicationTestUtil.isLedgerInUnderReplication(zkc, lh.getId(), basePath)) {
                Thread.sleep(100);
            }
            rw1.start();
            rw2.start();

            // wait for RWs to complete 'numOfAttemptsToWaitFor' failed attempts
            int numOfAttemptsToWaitFor = 10;
            while ((rw1.replicationFailedLedgers.get(lh.getId()).get() < numOfAttemptsToWaitFor)
                    || rw2.replicationFailedLedgers.get(lh.getId()).get() < numOfAttemptsToWaitFor) {
                Thread.sleep(500);
            }

            /*
             * since all the bookies containing the ledger entries are down
             * replication wouldn't have succeeded.
             */
            assertTrue("Ledger: " + lh.getId() + " should be underreplicated",
                    ReplicationTestUtil.isLedgerInUnderReplication(zkc, lh.getId(), basePath));

            /*
             * since RW failed 'numOfAttemptsToWaitFor' number of times, we
             * should have atleast (numOfAttemptsToWaitFor - 1)
             * delayReplicationPeriods and their value should be
             * (lockReleaseOfFailedLedgerGracePeriod/16) , 2 * previous value,..
             * with max : lockReleaseOfFailedLedgerGracePeriod
             */
            for (int i = 0; i < ((numOfAttemptsToWaitFor - 1)); i++) {
                long expectedDelayValue = Math.min(lockReleaseOfFailedLedgerGracePeriod,
                        baseBackoffForLockReleaseOfFailedLedger * (1 << i));
                assertEquals("RW1 delayperiod", (Long) expectedDelayValue, rw1DelayReplicationPeriods.get(i));
                assertEquals("RW2 delayperiod", (Long) expectedDelayValue, rw2DelayReplicationPeriods.get(i));
            }

            /*
             * RW wont try to replicate until and unless RW succeed in reading
             * those failed entries before proceeding with replication of under
             * replicated fragment, so the numEntriesUnableToReadForReplication
             * should be just 'numOfEntries', though RW failed to replicate
             * multiple times.
             */
            assertEquals("numEntriesUnableToReadForReplication for RW1", Long.valueOf((long) numOfEntries),
                    numEntriesUnableToReadForReplication1.get());
            assertEquals("numEntriesUnableToReadForReplication for RW2", Long.valueOf((long) numOfEntries),
                    numEntriesUnableToReadForReplication2.get());

            /*
             * Since these entries are unavailable,
             * unableToReadEntriesForReplication should be of size numOfEntries.
             */
            assertEquals("RW1 unabletoreadentries", numOfEntries,
                    rw1.unableToReadEntriesForReplication.get(lh.getId()).size());
            assertEquals("RW2 unabletoreadentries", numOfEntries,
                    rw2.unableToReadEntriesForReplication.get(lh.getId()).size());
        } finally {
            rw1.shutdown();
            rw2.shutdown();
            underReplicationManager.close();
        }
    }

    /**
     * Tests that ReplicationWorker should not have identified for postponing
     * the replication if ledger is in open state and lastFragment is not in
     * underReplication state. Note that RW should not fence such ledgers.
     */
    @Test
    public void testRWShouldReplicateTheLedgersAfterTimeoutIfLastFragmentIsNotUR()
            throws Exception {
        LedgerHandle lh = bkc.createLedger(3, 3, BookKeeper.DigestType.CRC32,
                TESTPASSWD);

        for (int i = 0; i < 10; i++) {
            lh.addEntry(data);
        }
        BookieId replicaToKill = lh.getLedgerMetadata().getAllEnsembles().get(0L).get(0);

        LOG.info("Killing Bookie : {}", replicaToKill);
        killBookie(replicaToKill);

        BookieId newBkAddr = startNewBookieAndReturnBookieId();
        LOG.info("New Bookie addr : {}", newBkAddr);

        // Reform ensemble...Making sure that last fragment is not in
        // under-replication
        for (int i = 0; i < 10; i++) {
            lh.addEntry(data);
        }

        ReplicationWorker rw = new ReplicationWorker(baseConf);

        baseClientConf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());

        @Cleanup MetadataClientDriver driver = MetadataDrivers.getClientDriver(
                URI.create(baseClientConf.getMetadataServiceUri()));
        driver.initialize(baseClientConf, scheduler, NullStatsLogger.INSTANCE, Optional.empty());

        LedgerManagerFactory mFactory = driver.getLedgerManagerFactory();

        LedgerUnderreplicationManager underReplicationManager = mFactory
                .newLedgerUnderreplicationManager();

        rw.start();
        try {

            underReplicationManager.markLedgerUnderreplicated(lh.getId(),
                    replicaToKill.toString());
            while (ReplicationTestUtil.isLedgerInUnderReplication(zkc, lh
                    .getId(), basePath)) {
                Thread.sleep(100);
            }

            killAllBookies(lh, newBkAddr);

            // Should be able to read the entries from 0-9
            verifyRecoveredLedgers(lh, 0, 9);
            lh = bkc.openLedgerNoRecovery(lh.getId(),
                    BookKeeper.DigestType.CRC32, TESTPASSWD);

            // Ledger should be still in open state
            assertTrue("Ledger must have been closed by RW", ClientUtil
                    .isLedgerOpen(lh));
        } finally {
            rw.shutdown();
            underReplicationManager.close();
        }

    }

    /**
     * Test that the replication worker will not shutdown on a simple ZK disconnection.
     */
    @Test
    public void testRWZKConnectionLost() throws Exception {
        try (ZooKeeperClient zk = ZooKeeperClient.newBuilder()
                .connectString(zkUtil.getZooKeeperConnectString())
                .sessionTimeoutMs(10000)
                .build()) {

            ReplicationWorker rw = new ReplicationWorker(baseConf);
            rw.start();
            for (int i = 0; i < 10; i++) {
                if (rw.isRunning()) {
                    break;
                }
                Thread.sleep(1000);
            }
            assertTrue("Replication worker should be running", rw.isRunning());

            stopZKCluster();
            // ZK is down for shorter period than reconnect timeout
            Thread.sleep(1000);
            startZKCluster();

            assertTrue("Replication worker should not shutdown", rw.isRunning());
        }
    }

    /**
     * Test that the replication worker shuts down on non-recoverable ZK connection loss.
     */
    @Test
    public void testRWZKConnectionLostOnNonRecoverableZkError() throws Exception {
        for (int j = 0; j < 3; j++) {
            LedgerHandle lh = bkc.createLedger(1, 1, 1,
                    BookKeeper.DigestType.CRC32, TESTPASSWD,
                    null);
            final long createdLedgerId = lh.getId();
            for (int i = 0; i < 10; i++) {
                lh.addEntry(data);
            }
            lh.close();
        }

        killBookie(2);
        killBookie(1);
        startNewBookie();
        startNewBookie();

        servers.get(0).getConfiguration().setRwRereplicateBackoffMs(100);
        servers.get(0).startAutoRecovery();

        Auditor auditor = getAuditor(10, TimeUnit.SECONDS);
        ReplicationWorker rw = servers.get(0).getReplicationWorker();

        ZkLedgerUnderreplicationManager ledgerUnderreplicationManager =
                (ZkLedgerUnderreplicationManager) FieldUtils.readField(auditor,
                        "ledgerUnderreplicationManager", true);

        ZooKeeper zkc = (ZooKeeper) FieldUtils.readField(ledgerUnderreplicationManager, "zkc", true);
        auditor.submitAuditTask().get();

        assertTrue(zkc.getState().isConnected());
        zkc.close();
        assertFalse(zkc.getState().isConnected());

        auditor.submitAuditTask();
        rw.run();

        for (int i = 0; i < 10; i++) {
            if (!rw.isRunning() && !auditor.isRunning()) {
                break;
            }
            Thread.sleep(1000);
        }
        assertFalse("Replication worker should NOT be running", rw.isRunning());
        assertFalse("Auditor should NOT be running", auditor.isRunning());
    }

    private void killAllBookies(LedgerHandle lh, BookieId excludeBK)
            throws Exception {
        // Killing all bookies except newly replicated bookie
        for (Entry<Long, ? extends List<BookieId>> entry :
                lh.getLedgerMetadata().getAllEnsembles().entrySet()) {
            List<BookieId> bookies = entry.getValue();
            for (BookieId bookie : bookies) {
                if (bookie.equals(excludeBK)) {
                    continue;
                }
                killBookie(bookie);
            }
        }
    }

    private void verifyRecoveredLedgers(LedgerHandle lh, long startEntryId,
                                        long endEntryId) throws BKException, InterruptedException {
        LedgerHandle lhs = bkc.openLedgerNoRecovery(lh.getId(),
                BookKeeper.DigestType.CRC32, TESTPASSWD);
        Enumeration<LedgerEntry> entries = lhs.readEntries(startEntryId,
                endEntryId);
        assertTrue("Should have the elements", entries.hasMoreElements());
        while (entries.hasMoreElements()) {
            LedgerEntry entry = entries.nextElement();
            assertEquals("TestReplicationWorker", new String(entry.getEntry()));
        }
    }

    @Test
    public void testReplicateEmptyOpenStateLedger() throws Exception {
        LedgerHandle lh = bkc.createLedger(3, 3, 2, BookKeeper.DigestType.CRC32, TESTPASSWD);
        assertFalse(lh.getLedgerMetadata().isClosed());

        List<BookieId> firstEnsemble = lh.getLedgerMetadata().getAllEnsembles().firstEntry().getValue();
        List<BookieId> ensemble = lh.getLedgerMetadata().getAllEnsembles().entrySet().iterator().next().getValue();
        killBookie(ensemble.get(1));

        startNewBookie();
        baseConf.setOpenLedgerRereplicationGracePeriod(String.valueOf(30));
        ReplicationWorker replicationWorker = new ReplicationWorker(baseConf);
        replicationWorker.start();

        try {
            underReplicationManager.markLedgerUnderreplicated(lh.getId(), ensemble.get(1).toString());
            Awaitility.waitAtMost(60, TimeUnit.SECONDS).untilAsserted(() ->
                    assertFalse(ReplicationTestUtil.isLedgerInUnderReplication(zkc, lh.getId(), basePath))
            );

            LedgerHandle lh1 = bkc.openLedgerNoRecovery(lh.getId(), BookKeeper.DigestType.CRC32, TESTPASSWD);
            assertTrue(lh1.getLedgerMetadata().isClosed());
        } finally {
            replicationWorker.shutdown();
        }
    }

    @Test
    public void testRepairedNotAdheringPlacementPolicyLedgerFragmentsOnRack() throws Exception {
        testRepairedNotAdheringPlacementPolicyLedgerFragments(RackawareEnsemblePlacementPolicy.class, null);
    }

    @Test
    public void testReplicationStats() throws Exception {
        BiConsumer<Boolean, ReplicationWorker> checkReplicationStats = (first, rw) -> {
            try {
                final Method rereplicate = rw.getClass().getDeclaredMethod("rereplicate");
                rereplicate.setAccessible(true);
                final Object result = rereplicate.invoke(rw);
                final Field statsLoggerField = rw.getClass().getDeclaredField("statsLogger");
                statsLoggerField.setAccessible(true);
                final TestStatsLogger statsLogger = (TestStatsLogger) statsLoggerField.get(rw);

                final Counter numDeferLedgerLockReleaseOfFailedLedgerCounter =
                        statsLogger.getCounter(ReplicationStats.NUM_DEFER_LEDGER_LOCK_RELEASE_OF_FAILED_LEDGER);
                final Counter numLedgersReplicatedCounter =
                        statsLogger.getCounter(ReplicationStats.NUM_FULL_OR_PARTIAL_LEDGERS_REPLICATED);
                final Counter numNotAdheringPlacementLedgersCounter = statsLogger
                        .getCounter(ReplicationStats.NUM_NOT_ADHERING_PLACEMENT_LEDGERS_REPLICATED);

                assertEquals("NUM_DEFER_LEDGER_LOCK_RELEASE_OF_FAILED_LEDGER",
                        1, numDeferLedgerLockReleaseOfFailedLedgerCounter.get().longValue());

                if (first) {
                    assertFalse((boolean) result);
                    assertEquals("NUM_FULL_OR_PARTIAL_LEDGERS_REPLICATED",
                            0, numLedgersReplicatedCounter.get().longValue());
                    assertEquals("NUM_NOT_ADHERING_PLACEMENT_LEDGERS_REPLICATED",
                            0, numNotAdheringPlacementLedgersCounter.get().longValue());

                } else {
                    assertTrue((boolean) result);
                    assertEquals("NUM_FULL_OR_PARTIAL_LEDGERS_REPLICATED",
                            1, numLedgersReplicatedCounter.get().longValue());
                    assertEquals("NUM_NOT_ADHERING_PLACEMENT_LEDGERS_REPLICATED",
                            1, numNotAdheringPlacementLedgersCounter.get().longValue());
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
        testRepairedNotAdheringPlacementPolicyLedgerFragments(
                RackawareEnsemblePlacementPolicy.class, checkReplicationStats);
    }

    private void testRepairedNotAdheringPlacementPolicyLedgerFragments(
            Class<? extends EnsemblePlacementPolicy> placementPolicyClass,
            BiConsumer<Boolean, ReplicationWorker> checkReplicationStats) throws Exception {
        List<BookieId> firstThreeBookies = servers.stream().map(ele -> {
            try {
                return ele.getServer().getBookieId();
            } catch (UnknownHostException e) {
                return null;
            }
        }).filter(Objects::nonNull).collect(Collectors.toList());

        baseClientConf.setProperty("reppDnsResolverClass", StaticDNSResolver.class.getName());
        baseClientConf.setProperty("enforceStrictZoneawarePlacement", false);
        bkc.close();
        bkc = new BookKeeperTestClient(baseClientConf) {
            @Override
            protected EnsemblePlacementPolicy initializeEnsemblePlacementPolicy(ClientConfiguration conf,
                                                                                DNSToSwitchMapping dnsResolver,
                                                                                HashedWheelTimer timer,
                                                                                FeatureProvider featureProvider,
                                                                                StatsLogger statsLogger,
                                                                                BookieAddressResolver bookieAddressResolver)
                    throws IOException {
                EnsemblePlacementPolicy ensemblePlacementPolicy = null;
                if (ZoneawareEnsemblePlacementPolicy.class == placementPolicyClass) {
                    ensemblePlacementPolicy = buildZoneAwareEnsemblePlacementPolicy(firstThreeBookies);
                } else if (RackawareEnsemblePlacementPolicy.class == placementPolicyClass) {
                    ensemblePlacementPolicy = buildRackAwareEnsemblePlacementPolicy(firstThreeBookies);
                }
                ensemblePlacementPolicy.initialize(conf, Optional.ofNullable(dnsResolver), timer,
                        featureProvider, statsLogger, bookieAddressResolver);
                return ensemblePlacementPolicy;
            }
        };

        //This ledger not adhering placement policy, the combine(0,1,2) rack is 1.
        LedgerHandle lh = bkc.createLedger(3, 3, 3, BookKeeper.DigestType.CRC32, TESTPASSWD);

        int entrySize = 10;
        for (int i = 0; i < entrySize; i++) {
            lh.addEntry(data);
        }
        lh.close();

        int minNumRacksPerWriteQuorumConfValue = 2;

        ServerConfiguration servConf = new ServerConfiguration(confByIndex(0));
        servConf.setMinNumRacksPerWriteQuorum(minNumRacksPerWriteQuorumConfValue);
        servConf.setProperty("reppDnsResolverClass", StaticDNSResolver.class.getName());
        servConf.setAuditorPeriodicPlacementPolicyCheckInterval(1000);
        servConf.setRepairedPlacementPolicyNotAdheringBookieEnable(true);

        MutableObject<Auditor> auditorRef = new MutableObject<Auditor>();
        try {
            TestStatsLogger statsLogger = startAuditorAndWaitForPlacementPolicyCheck(servConf, auditorRef);
            Gauge<? extends Number> ledgersNotAdheringToPlacementPolicyGuage = statsLogger
                    .getGauge(ReplicationStats.NUM_LEDGERS_NOT_ADHERING_TO_PLACEMENT_POLICY);
            assertEquals("NUM_LEDGERS_NOT_ADHERING_TO_PLACEMENT_POLICY guage value",
                    1, ledgersNotAdheringToPlacementPolicyGuage.getSample());
            Gauge<? extends Number> ledgersSoftlyAdheringToPlacementPolicyGuage = statsLogger
                    .getGauge(ReplicationStats.NUM_LEDGERS_SOFTLY_ADHERING_TO_PLACEMENT_POLICY);
            assertEquals("NUM_LEDGERS_SOFTLY_ADHERING_TO_PLACEMENT_POLICY guage value",
                    0, ledgersSoftlyAdheringToPlacementPolicyGuage.getSample());
        } finally {
            Auditor auditor = auditorRef.getValue();
            if (auditor != null) {
                auditor.close();
            }
        }

        ZooKeeper zk = getZk((PulsarMetadataClientDriver) bkc.getMetadataClientDriver());


        Stat stat = zk.exists("/ledgers/underreplication/ledgers/0000/0000/0000/0000/urL0000000000", false);
        assertNotNull(stat);

        baseConf.setRepairedPlacementPolicyNotAdheringBookieEnable(true);
        BookKeeper bookKeeper = new BookKeeperTestClient(baseClientConf) {
            @Override
            protected EnsemblePlacementPolicy initializeEnsemblePlacementPolicy(ClientConfiguration conf,
                                                                                DNSToSwitchMapping dnsResolver,
                                                                                HashedWheelTimer timer,
                                                                                FeatureProvider featureProvider,
                                                                                StatsLogger statsLogger,
                                                                                BookieAddressResolver bookieAddressResolver)
                    throws IOException {
                EnsemblePlacementPolicy ensemblePlacementPolicy = null;
                if (ZoneawareEnsemblePlacementPolicy.class == placementPolicyClass) {
                    ensemblePlacementPolicy = buildZoneAwareEnsemblePlacementPolicy(firstThreeBookies);
                } else if (RackawareEnsemblePlacementPolicy.class == placementPolicyClass) {
                    ensemblePlacementPolicy = buildRackAwareEnsemblePlacementPolicy(firstThreeBookies);
                }
                ensemblePlacementPolicy.initialize(conf, Optional.ofNullable(dnsResolver), timer,
                        featureProvider, statsLogger, bookieAddressResolver);
                return ensemblePlacementPolicy;
            }
        };
        TestStatsProvider statsProvider = new TestStatsProvider();
        TestStatsLogger statsLogger = statsProvider.getStatsLogger(REPLICATION_SCOPE);
        ReplicationWorker rw = new ReplicationWorker(baseConf, bookKeeper, false, statsLogger);

        if (checkReplicationStats != null) {
            checkReplicationStats.accept(true, rw);
        } else {
            rw.start();
        }

        //start new bookie, the rack is /rack2
        BookieId newBookieId = startNewBookieAndReturnBookieId();

        if (checkReplicationStats != null) {
            checkReplicationStats.accept(false, rw);
        }

        Awaitility.await().untilAsserted(() -> {
            LedgerMetadata metadata = bkc.getLedgerManager().readLedgerMetadata(lh.getId()).get().getValue();
            List<BookieId> newBookies = metadata.getAllEnsembles().get(0L);
            assertTrue(newBookies.contains(newBookieId));
        });

        Awaitility.await().untilAsserted(() -> {
            Stat stat1 = zk.exists("/ledgers/underreplication/ledgers/0000/0000/0000/0000/urL0000000000", false);
            assertNull(stat1);
        });

        for (BookieId rack1Book : firstThreeBookies) {
            killBookie(rack1Book);
        }

        verifyRecoveredLedgers(lh, 0, entrySize - 1);

        if (checkReplicationStats == null) {
            rw.shutdown();
        }
        baseConf.setRepairedPlacementPolicyNotAdheringBookieEnable(false);
        bookKeeper.close();
    }

    private EnsemblePlacementPolicy buildRackAwareEnsemblePlacementPolicy(List<BookieId> bookieIds) {
        return new RackawareEnsemblePlacementPolicy() {
            @Override
            public String resolveNetworkLocation(BookieId addr) {
                if (bookieIds.contains(addr)) {
                    return "/rack1";
                }
                //The other bookie is /rack2
                return "/rack2";
            }
        };
    }

    private EnsemblePlacementPolicy buildZoneAwareEnsemblePlacementPolicy(List<BookieId> firstThreeBookies) {
        return new ZoneawareEnsemblePlacementPolicy() {
            @Override
            protected String resolveNetworkLocation(BookieId addr) {
                //The first three bookie 1 is /zone1/ud1
                //The first three bookie 2,3 is /zone1/ud2
                if (firstThreeBookies.get(0).equals(addr)) {
                    return "/zone1/ud1";
                } else if (firstThreeBookies.contains(addr)) {
                    return "/zone1/ud2";
                }
                //The other bookie is /zone2/ud1
                return "/zone2/ud1";
            }
        };
    }

    private TestStatsLogger startAuditorAndWaitForPlacementPolicyCheck(ServerConfiguration servConf,
                                                                       MutableObject<Auditor> auditorRef)
            throws MetadataException, CompatibilityException, KeeperException,
            InterruptedException, ReplicationException.UnavailableException, UnknownHostException {
        LedgerManagerFactory mFactory = driver.getLedgerManagerFactory();
        LedgerUnderreplicationManager urm = mFactory.newLedgerUnderreplicationManager();
        TestStatsProvider statsProvider = new TestStatsProvider();
        TestStatsLogger statsLogger = statsProvider.getStatsLogger(AUDITOR_SCOPE);
        TestStatsProvider.TestOpStatsLogger placementPolicyCheckStatsLogger =
                (TestStatsProvider.TestOpStatsLogger) statsLogger
                        .getOpStatsLogger(ReplicationStats.PLACEMENT_POLICY_CHECK_TIME);

        final AuditorPeriodicCheckTest.TestAuditor auditor = new AuditorPeriodicCheckTest.TestAuditor(
                BookieImpl.getBookieId(servConf).toString(), servConf, bkc, false, statsLogger, null);
        auditorRef.setValue(auditor);
        CountDownLatch latch = auditor.getLatch();
        assertEquals("PLACEMENT_POLICY_CHECK_TIME SuccessCount", 0,
                placementPolicyCheckStatsLogger.getSuccessCount());
        urm.setPlacementPolicyCheckCTime(-1);
        auditor.start();
        /*
         * since placementPolicyCheckCTime is set to -1, placementPolicyCheck should be
         * scheduled to run with no initialdelay
         */
        assertTrue("placementPolicyCheck should have executed", latch.await(20, TimeUnit.SECONDS));
        for (int i = 0; i < 20; i++) {
            Thread.sleep(100);
            if (placementPolicyCheckStatsLogger.getSuccessCount() >= 1) {
                break;
            }
        }
        assertEquals("PLACEMENT_POLICY_CHECK_TIME SuccessCount", 1,
                placementPolicyCheckStatsLogger.getSuccessCount());
        return statsLogger;
    }

    private ZooKeeper getZk(PulsarMetadataClientDriver pulsarMetadataClientDriver) throws Exception {
        PulsarLedgerManagerFactory pulsarLedgerManagerFactory =
                (PulsarLedgerManagerFactory) pulsarMetadataClientDriver.getLedgerManagerFactory();
        Field field = pulsarLedgerManagerFactory.getClass().getDeclaredField("store");
        field.setAccessible(true);
        ZKMetadataStore zkMetadataStore = (ZKMetadataStore) field.get(pulsarLedgerManagerFactory);
        return zkMetadataStore.getZkClient();
    }
}
