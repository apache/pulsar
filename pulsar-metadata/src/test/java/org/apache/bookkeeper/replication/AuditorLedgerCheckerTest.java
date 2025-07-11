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
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNotSame;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import lombok.Cleanup;
import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.LedgerMetadataBuilder;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LayoutManager;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.apache.bookkeeper.meta.MetadataClientDriver;
import org.apache.bookkeeper.meta.MetadataDrivers;
import org.apache.bookkeeper.meta.UnderreplicatedLedger;
import org.apache.bookkeeper.meta.ZkLedgerUnderreplicationManager;
import org.apache.bookkeeper.meta.zk.ZKMetadataDriverBase;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.replication.ReplicationException.UnavailableException;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.bookkeeper.PulsarLayoutManager;
import org.apache.pulsar.metadata.bookkeeper.PulsarLedgerAuditorManager;
import org.apache.pulsar.metadata.bookkeeper.PulsarLedgerManagerFactory;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests publishing of under replicated ledgers by the Auditor bookie node when
 * corresponding bookies identifes as not running.
 */
public class AuditorLedgerCheckerTest extends BookKeeperClusterTestCase {

    // Depending on the taste, select the amount of logging
    // by decommenting one of the two lines below
    // private static final Logger LOG = Logger.getRootLogger();
    private static final Logger LOG = LoggerFactory
            .getLogger(AuditorLedgerCheckerTest.class);

    private static final byte[] ledgerPassword = "aaa".getBytes();
    private Random rng; // Random Number Generator

    private DigestType digestType;

    private String underreplicatedPath;
    private Map<String, AuditorElector> auditorElectors = new ConcurrentHashMap<>();
    private LedgerUnderreplicationManager urLedgerMgr;

    private Set<Long> urLedgerList;
    private String electionPath;

    private List<Long> ledgerList;

    public AuditorLedgerCheckerTest()
            throws Exception {
        this("org.apache.pulsar.metadata.bookkeeper.PulsarLedgerManagerFactory");
    }

    AuditorLedgerCheckerTest(String ledgerManagerFactoryClass)
            throws Exception {
        super(3);
        LOG.info("Running test case using ledger manager : "
                + ledgerManagerFactoryClass);
        this.digestType = DigestType.CRC32;
        Class.forName("org.apache.pulsar.metadata.bookkeeper.PulsarMetadataClientDriver");
        Class.forName("org.apache.pulsar.metadata.bookkeeper.PulsarMetadataBookieDriver");        // set ledger manager name
        baseConf.setLedgerManagerFactoryClassName(ledgerManagerFactoryClass);
        baseClientConf
                .setLedgerManagerFactoryClassName(ledgerManagerFactoryClass);
    }

    @BeforeMethod
    public void setUp() throws Exception {
        super.setUp();
        underreplicatedPath = ZKMetadataDriverBase.resolveZkLedgersRootPath(baseClientConf)
                + "/underreplication/ledgers";
        electionPath = ZKMetadataDriverBase.resolveZkLedgersRootPath(baseConf)
                + "/underreplication/" + PulsarLedgerAuditorManager.ELECTION_PATH;

        String ledgersRoot = "/ledgers";
        String storeUri = metadataServiceUri.replaceAll("zk://", "").replaceAll("/ledgers", "");
        MetadataStoreExtended store = registerCloseable(MetadataStoreExtended.create(storeUri,
                MetadataStoreConfig.builder().fsyncEnable(false).build()));
        LayoutManager layoutManager = new PulsarLayoutManager(store, ledgersRoot);
        PulsarLedgerManagerFactory ledgerManagerFactory = registerCloseable(new PulsarLedgerManagerFactory());
        ClientConfiguration conf = new ClientConfiguration();
        conf.setZkLedgersRootPath(ledgersRoot);
        ledgerManagerFactory.initialize(conf, layoutManager, 1);
        urLedgerMgr = registerCloseable(ledgerManagerFactory.newLedgerUnderreplicationManager());
        urLedgerMgr.setCheckAllLedgersCTime(System.currentTimeMillis());

        baseClientConf.setMetadataServiceUri(
                zkUtil.getMetadataServiceUri().replaceAll("zk://", "metadata-store:").replaceAll("/ledgers", ""));
        baseConf.setMetadataServiceUri(
                zkUtil.getMetadataServiceUri().replaceAll("zk://", "metadata-store:").replaceAll("/ledgers", ""));
        startAuditorElectors();
        rng = new Random(System.currentTimeMillis()); // Initialize the Random
        urLedgerList = new HashSet<Long>();
        ledgerList = new ArrayList<Long>(2);
    }

    @AfterMethod
    @Override
    public void tearDown() throws Exception {
        stopAuditorElectors();
        super.tearDown();
    }

    private void startAuditorElectors() throws Exception {
        for (String addr : bookieAddresses().stream().map(Object::toString)
                .collect(Collectors.toList())) {
            AuditorElector auditorElector = new AuditorElector(addr, baseConf);
            auditorElectors.put(addr, auditorElector);
            auditorElector.start();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Starting Auditor Elector");
            }
        }
    }

    private void stopAuditorElectors() throws Exception {
        for (AuditorElector auditorElector : auditorElectors.values()) {
            auditorElector.shutdown();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Stopping Auditor Elector!");
            }
        }
    }

    /**
     * Test publishing of under replicated ledgers by the auditor bookie.
     */
    @Test
    public void testSimpleLedger() throws Exception {
        LedgerHandle lh1 = createAndAddEntriesToLedger();
        Long ledgerId = lh1.getId();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Created ledger : " + ledgerId);
        }
        ledgerList.add(ledgerId);
        lh1.close();

        final CountDownLatch underReplicaLatch = registerUrLedgerWatcher(ledgerList
                .size());

        int bkShutdownIndex = lastBookieIndex();
        String shutdownBookie = shutdownBookie(bkShutdownIndex);

        // grace period for publishing the bk-ledger
        if (LOG.isDebugEnabled()) {
            LOG.debug("Waiting for ledgers to be marked as under replicated");
        }
        waitForAuditToComplete();
        underReplicaLatch.await(5, TimeUnit.SECONDS);
        Map<Long, String> urLedgerData = getUrLedgerData(urLedgerList);
        assertEquals("Missed identifying under replicated ledgers", 1,
                urLedgerList.size());

        /*
         * Sample data format present in the under replicated ledger path
         *
         * {4=replica: "10.18.89.153:5002"}
         */
        assertTrue("Ledger is not marked as underreplicated:" + ledgerId,
                urLedgerList.contains(ledgerId));
        String data = urLedgerData.get(ledgerId);
        assertTrue("Bookie " + shutdownBookie
                        + "is not listed in the ledger as missing replica :" + data,
                data.contains(shutdownBookie));
    }

    /**
     * Test once published under replicated ledger should exists even after
     * restarting respective bookie.
     */
    @Test
    public void testRestartBookie() throws Exception {
        LedgerHandle lh1 = createAndAddEntriesToLedger();
        LedgerHandle lh2 = createAndAddEntriesToLedger();

        if (LOG.isDebugEnabled()) {
            LOG.debug("Created following ledgers : {}, {}", lh1, lh2);
        }

        int bkShutdownIndex = lastBookieIndex();
        ServerConfiguration bookieConf1 = confByIndex(bkShutdownIndex);
        String shutdownBookie = shutdownBookie(bkShutdownIndex);

        // restart the failed bookie
        startAndAddBookie(bookieConf1);

        waitForLedgerMissingReplicas(lh1.getId(), 10, shutdownBookie);
        waitForLedgerMissingReplicas(lh2.getId(), 10, shutdownBookie);
    }

    /**
     * Test publishing of under replicated ledgers when multiple bookie failures
     * one after another.
     */
    @Test
    public void testMultipleBookieFailures() throws Exception {
        LedgerHandle lh1 = createAndAddEntriesToLedger();

        // failing first bookie
        shutdownBookie(lastBookieIndex());

        // simulate re-replication
        doLedgerRereplication(lh1.getId());

        // failing another bookie
        String shutdownBookie = shutdownBookie(lastBookieIndex());

        // grace period for publishing the bk-ledger
        if (LOG.isDebugEnabled()) {
            LOG.debug("Waiting for ledgers to be marked as under replicated");
        }
        assertTrue("Ledger should be missing second replica",
                waitForLedgerMissingReplicas(lh1.getId(), 10, shutdownBookie));
    }

    @Test
    public void testToggleLedgerReplication() throws Exception {
        LedgerHandle lh1 = createAndAddEntriesToLedger();
        ledgerList.add(lh1.getId());
        if (LOG.isDebugEnabled()) {
            LOG.debug("Created following ledgers : " + ledgerList);
        }

        // failing another bookie
        CountDownLatch urReplicaLatch = registerUrLedgerWatcher(ledgerList
                .size());

        // disabling ledger replication
        urLedgerMgr.disableLedgerReplication();
        ArrayList<String> shutdownBookieList = new ArrayList<String>();
        shutdownBookieList.add(shutdownBookie(lastBookieIndex()));
        shutdownBookieList.add(shutdownBookie(lastBookieIndex()));

        assertFalse("Ledger replication is not disabled!", urReplicaLatch
                .await(1, TimeUnit.SECONDS));

        // enabling ledger replication
        urLedgerMgr.enableLedgerReplication();
        assertTrue("Ledger replication is not enabled!", urReplicaLatch.await(
                5, TimeUnit.SECONDS));
    }

    @Test
    public void testDuplicateEnDisableAutoRecovery() throws Exception {
        urLedgerMgr.disableLedgerReplication();
        try {
            urLedgerMgr.disableLedgerReplication();
            fail("Must throw exception, since AutoRecovery is already disabled");
        } catch (UnavailableException e) {
            assertTrue("AutoRecovery is not disabled previously!",
                    e.getCause().getCause() instanceof MetadataStoreException.BadVersionException);
        }
        urLedgerMgr.enableLedgerReplication();
        try {
            urLedgerMgr.enableLedgerReplication();
            fail("Must throw exception, since AutoRecovery is already enabled");
        } catch (UnavailableException e) {
            assertTrue("AutoRecovery is not enabled previously!",
                    e.getCause().getCause() instanceof MetadataStoreException.NotFoundException);
        }
    }

    /**
     * Test Auditor should consider Readonly bookie as available bookie. Should not publish ur ledgers for
     * readonly bookies.
     */
    @Test
    public void testReadOnlyBookieExclusionFromURLedgersCheck() throws Exception {
        LedgerHandle lh = createAndAddEntriesToLedger();
        ledgerList.add(lh.getId());
        if (LOG.isDebugEnabled()) {
            LOG.debug("Created following ledgers : " + ledgerList);
        }

        int count = ledgerList.size();
        final CountDownLatch underReplicaLatch = registerUrLedgerWatcher(count);

        final int bkIndex = 2;
        ServerConfiguration bookieConf = confByIndex(bkIndex);
        BookieServer bk = serverByIndex(bkIndex);
        bookieConf.setReadOnlyModeEnabled(true);

        ((BookieImpl) bk.getBookie()).getStateManager().doTransitionToReadOnlyMode();
        bkc.waitForReadOnlyBookie(BookieImpl.getBookieId(confByIndex(bkIndex)))
                .get(30, TimeUnit.SECONDS);

        // grace period for publishing the bk-ledger
        if (LOG.isDebugEnabled()) {
            LOG.debug("Waiting for Auditor to finish ledger check.");
        }
        waitForAuditToComplete();
        assertFalse("latch should not have completed", underReplicaLatch.await(5, TimeUnit.SECONDS));
    }

    /**
     * Test Auditor should consider Readonly bookie fail and publish ur ledgers for readonly bookies.
     */
    @Test
    public void testReadOnlyBookieShutdown() throws Exception {
        LedgerHandle lh = createAndAddEntriesToLedger();
        long ledgerId = lh.getId();
        ledgerList.add(ledgerId);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Created following ledgers : " + ledgerList);
        }

        int count = ledgerList.size();
        final CountDownLatch underReplicaLatch = registerUrLedgerWatcher(count);

        int bkIndex = lastBookieIndex();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Moving bookie {} {} to read only...", bkIndex, serverByIndex(bkIndex));
        }
        ServerConfiguration bookieConf = confByIndex(bkIndex);
        BookieServer bk = serverByIndex(bkIndex);
        bookieConf.setReadOnlyModeEnabled(true);

        ((BookieImpl) bk.getBookie()).getStateManager().doTransitionToReadOnlyMode();
        bkc.waitForReadOnlyBookie(BookieImpl.getBookieId(confByIndex(bkIndex)))
                .get(30, TimeUnit.SECONDS);

        // grace period for publishing the bk-ledger
        if (LOG.isDebugEnabled()) {
            LOG.debug("Waiting for Auditor to finish ledger check.");
        }
        waitForAuditToComplete();
        assertFalse("latch should not have completed", underReplicaLatch.await(1, TimeUnit.SECONDS));

        String shutdownBookie = shutdownBookie(bkIndex);

        // grace period for publishing the bk-ledger
        if (LOG.isDebugEnabled()) {
            LOG.debug("Waiting for ledgers to be marked as under replicated");
        }
        waitForAuditToComplete();
        underReplicaLatch.await(5, TimeUnit.SECONDS);
        Map<Long, String> urLedgerData = getUrLedgerData(urLedgerList);
        assertEquals("Missed identifying under replicated ledgers", 1, urLedgerList.size());

        /*
         * Sample data format present in the under replicated ledger path
         *
         * {4=replica: "10.18.89.153:5002"}
         */
        assertTrue("Ledger is not marked as underreplicated:" + ledgerId, urLedgerList.contains(ledgerId));
        String data = urLedgerData.get(ledgerId);
        assertTrue("Bookie " + shutdownBookie + "is not listed in the ledger as missing replica :" + data,
                data.contains(shutdownBookie));
    }

    public void testInnerDelayedAuditOfLostBookies() throws Exception {
        LedgerHandle lh1 = createAndAddEntriesToLedger();
        Long ledgerId = lh1.getId();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Created ledger : " + ledgerId);
        }
        ledgerList.add(ledgerId);
        lh1.close();

        final CountDownLatch underReplicaLatch = registerUrLedgerWatcher(ledgerList
                .size());

        // wait for 5 seconds before starting the recovery work when a bookie fails
        urLedgerMgr.setLostBookieRecoveryDelay(5);

        AtomicReference<String> shutdownBookieRef = new AtomicReference<>();
        CountDownLatch shutdownLatch = new CountDownLatch(1);
        new Thread(() -> {
            try {
                String shutdownBookie = shutDownNonAuditorBookie();
                shutdownBookieRef.set(shutdownBookie);
                shutdownLatch.countDown();
            } catch (Exception ignore) {
            }
        }).start();

        if (LOG.isDebugEnabled()) {
            LOG.debug("Waiting for ledgers to be marked as under replicated");
        }
        assertFalse("audit of lost bookie isn't delayed", underReplicaLatch.await(4, TimeUnit.SECONDS));
        assertEquals("under replicated ledgers identified when it was not expected", 0,
                urLedgerList.size());

        // wait for another 5 seconds for the ledger to get reported as under replicated
        assertTrue("audit of lost bookie isn't delayed", underReplicaLatch.await(2, TimeUnit.SECONDS));

        assertTrue("Ledger is not marked as underreplicated:" + ledgerId,
                urLedgerList.contains(ledgerId));
        Map<Long, String> urLedgerData = getUrLedgerData(urLedgerList);
        String data = urLedgerData.get(ledgerId);
        shutdownLatch.await();
        assertTrue("Bookie " + shutdownBookieRef.get()
                        + "is not listed in the ledger as missing replica :" + data,
                data.contains(shutdownBookieRef.get()));
    }

    /**
     * Test publishing of under replicated ledgers by the auditor
     * bookie is delayed if LostBookieRecoveryDelay option is set.
     */
    @Test
    public void testDelayedAuditOfLostBookies() throws Exception {
        // wait for a second so that the initial periodic check finishes
        Thread.sleep(1000);

        testInnerDelayedAuditOfLostBookies();
    }

    /**
     * Test publishing of under replicated ledgers by the auditor
     * bookie is delayed if LostBookieRecoveryDelay option is set
     * and it continues to be delayed even when periodic bookie check
     * is set to run every 2 secs. I.e. periodic bookie check doesn't
     * override the delay
     */
    @Test
    public void testDelayedAuditWithPeriodicBookieCheck() throws Exception {
        // enable periodic bookie check on a cadence of every 2 seconds.
        // this requires us to stop the auditor/auditorElectors, set the
        // periodic check interval and restart the auditorElectors
        stopAuditorElectors();
        baseConf.setAuditorPeriodicBookieCheckInterval(2);
        startAuditorElectors();

        // wait for a second so that the initial periodic check finishes
        Thread.sleep(1000);

        // the delaying of audit should just work despite the fact
        // we have enabled periodic bookie check
        testInnerDelayedAuditOfLostBookies();
    }

    @Test
    public void testRescheduleOfDelayedAuditOfLostBookiesToStartImmediately() throws Exception {
        // wait for a second so that the initial periodic check finishes
        Thread.sleep(1000);

        LedgerHandle lh1 = createAndAddEntriesToLedger();
        Long ledgerId = lh1.getId();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Created ledger : " + ledgerId);
        }
        ledgerList.add(ledgerId);
        lh1.close();

        final CountDownLatch underReplicaLatch = registerUrLedgerWatcher(ledgerList
                .size());

        // wait for 50 seconds before starting the recovery work when a bookie fails
        urLedgerMgr.setLostBookieRecoveryDelay(50);

        // shutdown a non auditor bookie; choosing non-auditor to avoid another election
        AtomicReference<String> shutdownBookieRef = new AtomicReference<>();
        CountDownLatch shutdownLatch = new CountDownLatch(1);
        new Thread(() -> {
            try {
                String shutdownBookie = shutDownNonAuditorBookie();
                shutdownBookieRef.set(shutdownBookie);
                shutdownLatch.countDown();
            } catch (Exception ignore) {
            }
        }).start();

        if (LOG.isDebugEnabled()) {
            LOG.debug("Waiting for ledgers to be marked as under replicated");
        }
        assertFalse("audit of lost bookie isn't delayed", underReplicaLatch.await(4, TimeUnit.SECONDS));
        assertEquals("under replicated ledgers identified when it was not expected", 0,
                urLedgerList.size());

        // set lostBookieRecoveryDelay to 0, so that it triggers AuditTask immediately
        urLedgerMgr.setLostBookieRecoveryDelay(0);

        // wait for 1 second for the ledger to get reported as under replicated
        assertTrue("audit of lost bookie isn't delayed", underReplicaLatch.await(1, TimeUnit.SECONDS));

        assertTrue("Ledger is not marked as underreplicated:" + ledgerId,
                urLedgerList.contains(ledgerId));
        Map<Long, String> urLedgerData = getUrLedgerData(urLedgerList);
        String data = urLedgerData.get(ledgerId);
        shutdownLatch.await();
        assertTrue("Bookie " + shutdownBookieRef.get()
                        + "is not listed in the ledger as missing replica :" + data,
                data.contains(shutdownBookieRef.get()));
    }

    @Test
    public void testRescheduleOfDelayedAuditOfLostBookiesToStartLater() throws Exception {
        // wait for a second so that the initial periodic check finishes
        Thread.sleep(1000);

        LedgerHandle lh1 = createAndAddEntriesToLedger();
        Long ledgerId = lh1.getId();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Created ledger : " + ledgerId);
        }
        ledgerList.add(ledgerId);
        lh1.close();

        final CountDownLatch underReplicaLatch = registerUrLedgerWatcher(ledgerList
                .size());

        // wait for 3 seconds before starting the recovery work when a bookie fails
        urLedgerMgr.setLostBookieRecoveryDelay(3);

        // shutdown a non auditor bookie; choosing non-auditor to avoid another election
        AtomicReference<String> shutdownBookieRef = new AtomicReference<>();
        CountDownLatch shutdownLatch = new CountDownLatch(1);
        new Thread(() -> {
            try {
                String shutdownBookie = shutDownNonAuditorBookie();
                shutdownBookieRef.set(shutdownBookie);
                shutdownLatch.countDown();
            } catch (Exception ignore) {
            }
        }).start();

        if (LOG.isDebugEnabled()) {
            LOG.debug("Waiting for ledgers to be marked as under replicated");
        }
        assertFalse("audit of lost bookie isn't delayed", underReplicaLatch.await(2, TimeUnit.SECONDS));
        assertEquals("under replicated ledgers identified when it was not expected", 0,
                urLedgerList.size());

        // set lostBookieRecoveryDelay to 4, so the pending AuditTask is resheduled
        urLedgerMgr.setLostBookieRecoveryDelay(4);

        // since we changed the BookieRecoveryDelay period to 4, the audittask shouldn't have been executed
        if (LOG.isDebugEnabled()) {
            LOG.debug("Waiting for ledgers to be marked as under replicated");
        }
        assertFalse("audit of lost bookie isn't delayed", underReplicaLatch.await(2, TimeUnit.SECONDS));
        assertEquals("under replicated ledgers identified when it was not expected", 0,
                urLedgerList.size());

        // wait for 3 seconds (since we already waited for 2 secs) for the ledger to get reported as under replicated
        assertTrue("audit of lost bookie isn't delayed", underReplicaLatch.await(3, TimeUnit.SECONDS));
        assertTrue("Ledger is not marked as underreplicated:" + ledgerId,
                urLedgerList.contains(ledgerId));
        Map<Long, String> urLedgerData = getUrLedgerData(urLedgerList);
        String data = urLedgerData.get(ledgerId);
        shutdownLatch.await();
        assertTrue("Bookie " + shutdownBookieRef.get()
                        + "is not listed in the ledger as missing replica :" + data,
                data.contains(shutdownBookieRef.get()));
    }

    @Test
    public void testTriggerAuditorWithNoPendingAuditTask() throws Exception {
        // wait for a second so that the initial periodic check finishes
        Thread.sleep(1000);
        int lostBookieRecoveryDelayConfValue = baseConf.getLostBookieRecoveryDelay();
        Auditor auditorBookiesAuditor = getAuditorBookiesAuditor();
        Future<?> auditTask = auditorBookiesAuditor.getAuditTask();
        int lostBookieRecoveryDelayBeforeChange = auditorBookiesAuditor.getLostBookieRecoveryDelayBeforeChange();
        assertEquals("auditTask is supposed to be null", null, auditTask);
        assertEquals(
                "lostBookieRecoveryDelayBeforeChange of Auditor should be equal to BaseConf's lostBookieRecoveryDelay",
                lostBookieRecoveryDelayConfValue, lostBookieRecoveryDelayBeforeChange);

        @Cleanup("shutdown") OrderedScheduler scheduler = OrderedScheduler.newSchedulerBuilder()
                .name("test-scheduler")
                .numThreads(1)
                .build();
        @Cleanup MetadataClientDriver driver =
                MetadataDrivers.getClientDriver(URI.create(baseClientConf.getMetadataServiceUri()));
        driver.initialize(baseClientConf, scheduler, NullStatsLogger.INSTANCE, Optional.of(zkc));

        // there is no easy way to validate if the Auditor has executed Audit process (Auditor.startAudit),
        // without shuttingdown Bookie. To test if by resetting LostBookieRecoveryDelay it does Auditing
        // even when there is no pending AuditTask, following approach is needed.

        // Here we are creating few ledgers ledgermetadata with non-existing bookies as its ensemble.
        // When Auditor does audit it recognizes these ledgers as underreplicated and mark them as
        // under-replicated, since these bookies are not available.
        int numofledgers = 5;
        Random rand = new Random();
        for (int i = 0; i < numofledgers; i++) {
            ArrayList<BookieId> ensemble = new ArrayList<BookieId>();
            ensemble.add(new BookieSocketAddress("99.99.99.99:9999").toBookieId());
            ensemble.add(new BookieSocketAddress("11.11.11.11:1111").toBookieId());
            ensemble.add(new BookieSocketAddress("88.88.88.88:8888").toBookieId());

            long ledgerId = (Math.abs(rand.nextLong())) % 100000000;

            LedgerMetadata metadata = LedgerMetadataBuilder.create()
                    .withId(ledgerId)
                    .withEnsembleSize(3).withWriteQuorumSize(2).withAckQuorumSize(2)
                    .withPassword("passwd".getBytes())
                    .withDigestType(DigestType.CRC32.toApiDigestType())
                    .newEnsembleEntry(0L, ensemble).build();

            try (LedgerManager lm = driver.getLedgerManagerFactory().newLedgerManager()) {
                lm.createLedgerMetadata(ledgerId, metadata).get(2000, TimeUnit.MILLISECONDS);
            }
            ledgerList.add(ledgerId);
        }

        final CountDownLatch underReplicaLatch = registerUrLedgerWatcher(ledgerList.size());
        urLedgerMgr.setLostBookieRecoveryDelay(lostBookieRecoveryDelayBeforeChange);
        assertTrue("Audit should be triggered and created ledgers should be marked as underreplicated",
                underReplicaLatch.await(2, TimeUnit.SECONDS));
        assertEquals("All the ledgers should be marked as underreplicated", ledgerList.size(), urLedgerList.size());

        auditTask = auditorBookiesAuditor.getAuditTask();
        assertEquals("auditTask is supposed to be null", null, auditTask);
        assertEquals(
                "lostBookieRecoveryDelayBeforeChange of Auditor should be equal to BaseConf's lostBookieRecoveryDelay",
                lostBookieRecoveryDelayBeforeChange, auditorBookiesAuditor.getLostBookieRecoveryDelayBeforeChange());
    }

    @Test
    public void testTriggerAuditorWithPendingAuditTask() throws Exception {
        // wait for a second so that the initial periodic check finishes
        Thread.sleep(1000);

        Auditor auditorBookiesAuditor = getAuditorBookiesAuditor();
        LedgerHandle lh1 = createAndAddEntriesToLedger();
        Long ledgerId = lh1.getId();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Created ledger : " + ledgerId);
        }
        ledgerList.add(ledgerId);
        lh1.close();

        final CountDownLatch underReplicaLatch = registerUrLedgerWatcher(ledgerList
                .size());

        int lostBookieRecoveryDelay = 5;
        // wait for 5 seconds before starting the recovery work when a bookie fails
        urLedgerMgr.setLostBookieRecoveryDelay(lostBookieRecoveryDelay);

        // shutdown a non auditor bookie; choosing non-auditor to avoid another election
        new Thread(() -> {
            try {
                shutDownNonAuditorBookie();
            } catch (Exception ignore) {
            }
        }).start();

        if (LOG.isDebugEnabled()) {
            LOG.debug("Waiting for ledgers to be marked as under replicated");
        }
        assertFalse("audit of lost bookie isn't delayed", underReplicaLatch.await(2, TimeUnit.SECONDS));
        assertEquals("under replicated ledgers identified when it was not expected", 0,
                urLedgerList.size());

        Future<?> auditTask = auditorBookiesAuditor.getAuditTask();
        assertNotSame("auditTask is not supposed to be null", null, auditTask);
        assertEquals(
                "lostBookieRecoveryDelayBeforeChange of Auditor should be equal to what we set",
                lostBookieRecoveryDelay, auditorBookiesAuditor.getLostBookieRecoveryDelayBeforeChange());

        // set lostBookieRecoveryDelay to 5 (previous value), so that Auditor is triggered immediately
        urLedgerMgr.setLostBookieRecoveryDelay(lostBookieRecoveryDelay);
        assertTrue("audit of lost bookie shouldn't be delayed", underReplicaLatch.await(2, TimeUnit.SECONDS));
        assertEquals("all under replicated ledgers should be identified", ledgerList.size(),
                urLedgerList.size());

        Thread.sleep(100);
        auditTask = auditorBookiesAuditor.getAuditTask();
        assertEquals("auditTask is supposed to be null", null, auditTask);
        assertEquals(
                "lostBookieRecoveryDelayBeforeChange of Auditor should be equal to previously set value",
                lostBookieRecoveryDelay, auditorBookiesAuditor.getLostBookieRecoveryDelayBeforeChange());
    }

    @Test
    public void testTriggerAuditorBySettingDelayToZeroWithPendingAuditTask() throws Exception {
        // wait for a second so that the initial periodic check finishes
        Thread.sleep(1000);

        Auditor auditorBookiesAuditor = getAuditorBookiesAuditor();
        LedgerHandle lh1 = createAndAddEntriesToLedger();
        Long ledgerId = lh1.getId();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Created ledger : " + ledgerId);
        }
        ledgerList.add(ledgerId);
        lh1.close();

        final CountDownLatch underReplicaLatch = registerUrLedgerWatcher(ledgerList
                .size());

        int lostBookieRecoveryDelay = 5;
        // wait for 5 seconds before starting the recovery work when a bookie fails
        urLedgerMgr.setLostBookieRecoveryDelay(lostBookieRecoveryDelay);

        // shutdown a non auditor bookie; choosing non-auditor to avoid another election
        new Thread(() -> {
            try {
                shutDownNonAuditorBookie();
            } catch (Exception ignore) {
            }
        }).start();

        if (LOG.isDebugEnabled()) {
            LOG.debug("Waiting for ledgers to be marked as under replicated");
        }
        assertFalse("audit of lost bookie isn't delayed", underReplicaLatch.await(2, TimeUnit.SECONDS));
        assertEquals("under replicated ledgers identified when it was not expected", 0,
                urLedgerList.size());

        Future<?> auditTask = auditorBookiesAuditor.getAuditTask();
        assertNotSame("auditTask is not supposed to be null", null, auditTask);
        assertEquals(
                "lostBookieRecoveryDelayBeforeChange of Auditor should be equal to what we set",
                lostBookieRecoveryDelay, auditorBookiesAuditor.getLostBookieRecoveryDelayBeforeChange());

        // set lostBookieRecoveryDelay to 0, so that Auditor is triggered immediately
        urLedgerMgr.setLostBookieRecoveryDelay(0);
        assertTrue("audit of lost bookie shouldn't be delayed", underReplicaLatch.await(1, TimeUnit.SECONDS));
        assertEquals("all under replicated ledgers should be identified", ledgerList.size(),
                urLedgerList.size());

        Thread.sleep(100);
        auditTask = auditorBookiesAuditor.getAuditTask();
        assertEquals("auditTask is supposed to be null", null, auditTask);
        assertEquals(
                "lostBookieRecoveryDelayBeforeChange of Auditor should be equal to previously set value",
                0, auditorBookiesAuditor.getLostBookieRecoveryDelayBeforeChange());
    }

    /**
     * Test audit of bookies is delayed when one bookie is down. But when
     * another one goes down, the audit is started immediately.
     */
    @Test
    public void testDelayedAuditWithMultipleBookieFailures() throws Exception {
        // wait for the periodic bookie check to finish
        Thread.sleep(1000);

        // create a ledger with a bunch of entries
        LedgerHandle lh1 = createAndAddEntriesToLedger();
        Long ledgerId = lh1.getId();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Created ledger : " + ledgerId);
        }
        ledgerList.add(ledgerId);
        lh1.close();

        CountDownLatch underReplicaLatch = registerUrLedgerWatcher(ledgerList.size());

        // wait for 10 seconds before starting the recovery work when a bookie fails
        urLedgerMgr.setLostBookieRecoveryDelay(10);

        // shutdown a non auditor bookie; choosing non-auditor to avoid another election
        AtomicReference<String> shutdownBookieRef1 = new AtomicReference<>();
        CountDownLatch shutdownLatch1 = new CountDownLatch(1);
        new Thread(() -> {
            try {
                String shutdownBookie1 = shutDownNonAuditorBookie();
                shutdownBookieRef1.set(shutdownBookie1);
                shutdownLatch1.countDown();
            } catch (Exception ignore) {
            }
        }).start();

        // wait for 3 seconds and there shouldn't be any under replicated ledgers
        // because we have delayed the start of audit by 10 seconds
        assertFalse("audit of lost bookie isn't delayed", underReplicaLatch.await(3, TimeUnit.SECONDS));
        assertEquals("under replicated ledgers identified when it was not expected", 0,
                urLedgerList.size());

        // Now shutdown the second non auditor bookie; We want to make sure that
        // the history about having delayed recovery remains. Hence we make sure
        // we bring down a non auditor bookie. This should cause the audit to take
        // place immediately and not wait for the remaining 7 seconds to elapse
        AtomicReference<String> shutdownBookieRef2 = new AtomicReference<>();
        CountDownLatch shutdownLatch2 = new CountDownLatch(1);
        new Thread(() -> {
            try {
                String shutdownBookie2 = shutDownNonAuditorBookie();
                shutdownBookieRef2.set(shutdownBookie2);
                shutdownLatch2.countDown();
            } catch (Exception ignore) {
            }
        }).start();

        // 2 second grace period for the ledgers to get reported as under replicated
        Thread.sleep(2000);

        // If the following checks pass, it means that audit happened
        // within 2 seconds of second bookie going down and it didn't
        // wait for 7 more seconds. Hence the second bookie failure doesn't
        // delay the audit
        assertTrue("Ledger is not marked as underreplicated:" + ledgerId,
                urLedgerList.contains(ledgerId));
        Map<Long, String> urLedgerData = getUrLedgerData(urLedgerList);
        String data = urLedgerData.get(ledgerId);
        shutdownLatch1.await();
        shutdownLatch2.await();
        assertTrue("Bookie " + shutdownBookieRef1.get() + shutdownBookieRef2.get()
                        + " are not listed in the ledger as missing replicas :" + data,
                data.contains(shutdownBookieRef1.get()) && data.contains(shutdownBookieRef2.get()));
    }

    /**
     * Test audit of bookies is delayed during rolling upgrade scenario:
     * a bookies goes down and comes up, the next bookie go down and up and so on.
     * At any time only one bookie is down.
     */
    @Test
    public void testDelayedAuditWithRollingUpgrade() throws Exception {
        // wait for the periodic bookie check to finish
        Thread.sleep(1000);

        // create a ledger with a bunch of entries
        LedgerHandle lh1 = createAndAddEntriesToLedger();
        Long ledgerId = lh1.getId();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Created ledger : " + ledgerId);
        }
        ledgerList.add(ledgerId);
        lh1.close();

        CountDownLatch underReplicaLatch = registerUrLedgerWatcher(ledgerList.size());

        // wait for 5 seconds before starting the recovery work when a bookie fails
        urLedgerMgr.setLostBookieRecoveryDelay(5);

        // shutdown a non auditor bookie to avoid an election
        int idx1 = getShutDownNonAuditorBookieIdx("");
        ServerConfiguration conf1 = confByIndex(idx1);
        AtomicReference<String> shutdownBookieRef1 = new AtomicReference<>();
        CountDownLatch shutdownLatch1 = new CountDownLatch(1);
        new Thread(() -> {
            try {
                String shutdownBookie1 = shutdownBookie(idx1);
                shutdownBookieRef1.set(shutdownBookie1);
                shutdownLatch1.countDown();
            } catch (Exception ignore) {
            }
        }).start();

        // wait for 2 seconds and there shouldn't be any under replicated ledgers
        // because we have delayed the start of audit by 5 seconds
        assertFalse("audit of lost bookie isn't delayed", underReplicaLatch.await(2, TimeUnit.SECONDS));
        assertEquals("under replicated ledgers identified when it was not expected", 0,
                urLedgerList.size());

        // restart the bookie we shut down above
        startAndAddBookie(conf1);

        // Now to simulate the rolling upgrade, bring down a bookie different from
        // the one we brought down/up above.
        // shutdown a non auditor bookie; choosing non-auditor to avoid another election
        AtomicReference<String> shutdownBookieRef2 = new AtomicReference<>();
        CountDownLatch shutdownLatch2 = new CountDownLatch(1);
        new Thread(() -> {
            try {
                String shutdownBookie2 = shutDownNonAuditorBookie();
                shutdownBookieRef2.set(shutdownBookie2);
                shutdownLatch2.countDown();
            } catch (Exception ignore) {
            }
        }).start();

        // since the first bookie that was brought down/up has come up, there is only
        // one bookie down at this time. Hence the lost bookie check shouldn't start
        // immediately; it will start 5 seconds after the second bookie went down
        assertFalse("audit of lost bookie isn't delayed", underReplicaLatch.await(2, TimeUnit.SECONDS));
        assertEquals("under replicated ledgers identified when it was not expected", 0,
                urLedgerList.size());

        // wait for a total of 6 seconds(2+4) for the ledgers to get reported as under replicated
        Thread.sleep(4000);

        // If the following checks pass, it means that auditing happened
        // after lostBookieRecoveryDelay during rolling upgrade as expected
        assertTrue("Ledger is not marked as underreplicated:" + ledgerId,
                urLedgerList.contains(ledgerId));
        Map<Long, String> urLedgerData = getUrLedgerData(urLedgerList);
        String data = urLedgerData.get(ledgerId);
        shutdownLatch1.await();
        shutdownLatch2.await();
        assertTrue("Bookie " + shutdownBookieRef1.get() + "wrongly listed as missing the ledger: " + data,
                !data.contains(shutdownBookieRef1.get()));
        assertTrue("Bookie " + shutdownBookieRef2.get()
                        + " is not listed in the ledger as missing replicas :" + data,
                data.contains(shutdownBookieRef2.get()));
        LOG.info("*****************Test Complete");
    }

    private void waitForAuditToComplete() throws Exception {
        long endTime = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < endTime) {
            Auditor auditor = getAuditorBookiesAuditor();
            if (auditor != null) {
                Future<?> task = auditor.submitAuditTask();
                task.get(5, TimeUnit.SECONDS);
                return;
            }
            Thread.sleep(100);
        }
        throw new TimeoutException("Could not find an audit within 5 seconds");
    }

    /**
     * Wait for ledger to be underreplicated, and to be missing all replicas specified.
     */
    private boolean waitForLedgerMissingReplicas(Long ledgerId, long secondsToWait, String... replicas)
            throws Exception {
        for (int i = 0; i < secondsToWait; i++) {
            try {
                UnderreplicatedLedger data = urLedgerMgr.getLedgerUnreplicationInfo(ledgerId);
                boolean all = true;
                for (String r : replicas) {
                    all = all && data.getReplicaList().contains(r);
                }
                if (all) {
                    return true;
                }
            } catch (Exception e) {
                // may not find node
            }
            Thread.sleep(1000);
        }
        return false;
    }

    private CountDownLatch registerUrLedgerWatcher(int count)
            throws KeeperException, InterruptedException {
        final CountDownLatch underReplicaLatch = new CountDownLatch(count);
        for (Long ledgerId : ledgerList) {
            Watcher urLedgerWatcher = new ChildWatcher(underReplicaLatch);
            String znode = ZkLedgerUnderreplicationManager.getUrLedgerZnode(underreplicatedPath,
                    ledgerId);
            zkc.exists(znode, urLedgerWatcher);
        }
        return underReplicaLatch;
    }

    private void doLedgerRereplication(Long... ledgerIds)
            throws UnavailableException {
        for (int i = 0; i < ledgerIds.length; i++) {
            long lid = urLedgerMgr.getLedgerToRereplicate();
            assertTrue("Received unexpected ledgerid", Arrays.asList(ledgerIds).contains(lid));
            urLedgerMgr.markLedgerReplicated(lid);
            urLedgerMgr.releaseUnderreplicatedLedger(lid);
        }
    }

    private String shutdownBookie(int bkShutdownIndex) throws Exception {
        BookieServer bkServer = serverByIndex(bkShutdownIndex);
        String bookieAddr = bkServer.getBookieId().toString();
        if (LOG.isInfoEnabled()) {
            LOG.info("Shutting down bookie:" + bookieAddr);
        }
        killBookie(bkShutdownIndex);
        auditorElectors.get(bookieAddr).shutdown();
        auditorElectors.remove(bookieAddr);
        return bookieAddr;
    }

    private LedgerHandle createAndAddEntriesToLedger() throws BKException,
            InterruptedException {
        int numEntriesToWrite = 100;
        // Create a ledger
        LedgerHandle lh = bkc.createLedger(digestType, ledgerPassword);
        LOG.info("Ledger ID: " + lh.getId());
        addEntry(numEntriesToWrite, lh);
        return lh;
    }

    private void addEntry(int numEntriesToWrite, LedgerHandle lh)
            throws InterruptedException, BKException {
        final CountDownLatch completeLatch = new CountDownLatch(numEntriesToWrite);
        final AtomicInteger rc = new AtomicInteger(BKException.Code.OK);

        for (int i = 0; i < numEntriesToWrite; i++) {
            ByteBuffer entry = ByteBuffer.allocate(4);
            entry.putInt(rng.nextInt(Integer.MAX_VALUE));
            entry.position(0);
            lh.asyncAddEntry(entry.array(), new AddCallback() {
                public void addComplete(int rc2, LedgerHandle lh, long entryId, Object ctx) {
                    rc.compareAndSet(BKException.Code.OK, rc2);
                    completeLatch.countDown();
                }
            }, null);
        }
        completeLatch.await();
        if (rc.get() != BKException.Code.OK) {
            throw BKException.create(rc.get());
        }

    }

    private Map<Long, String> getUrLedgerData(Set<Long> urLedgerList)
            throws KeeperException, InterruptedException {
        Map<Long, String> urLedgerData = new HashMap<Long, String>();
        for (Long ledgerId : urLedgerList) {
            String znode = ZkLedgerUnderreplicationManager.getUrLedgerZnode(underreplicatedPath,
                    ledgerId);
            byte[] data = zkc.getData(znode, false, null);
            urLedgerData.put(ledgerId, new String(data));
        }
        return urLedgerData;
    }

    private class ChildWatcher implements Watcher {
        private final CountDownLatch underReplicaLatch;

        public ChildWatcher(CountDownLatch underReplicaLatch) {
            this.underReplicaLatch = underReplicaLatch;
        }

        @Override
        public void process(WatchedEvent event) {
            LOG.info("Received notification for the ledger path : "
                    + event.getPath());
            for (Long ledgerId : ledgerList) {
                if (event.getPath().contains(ledgerId + "")) {
                    urLedgerList.add(ledgerId);
                }
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Count down and waiting for next notification");
            }
            // count down and waiting for next notification
            underReplicaLatch.countDown();
        }
    }

    private BookieServer getAuditorBookie() throws Exception {
        List<BookieServer> auditors = new LinkedList<BookieServer>();
        byte[] data = zkc.getData(electionPath, false, null);
        assertNotNull("Auditor election failed", data);
        for (int i = 0; i < bookieCount(); i++) {
            BookieId bookieId = addressByIndex(i);
            if (new String(data).contains(bookieId + "")) {
                auditors.add(serverByIndex(i));
            }
        }
        assertEquals("Multiple Bookies acting as Auditor!", 1, auditors
                .size());
        return auditors.get(0);
    }

    private Auditor getAuditorBookiesAuditor() throws Exception {
        BookieServer auditorBookieServer = getAuditorBookie();
        String bookieAddr = auditorBookieServer.getBookieId().toString();
        return auditorElectors.get(bookieAddr).auditor;
    }

    private String shutDownNonAuditorBookie() throws Exception {
        // shutdown bookie which is not an auditor
        int indexOf = indexOfServer(getAuditorBookie());
        int bkIndexDownBookie;
        if (indexOf < lastBookieIndex()) {
            bkIndexDownBookie = indexOf + 1;
        } else {
            bkIndexDownBookie = indexOf - 1;
        }
        return shutdownBookie(bkIndexDownBookie);
    }

    private int getShutDownNonAuditorBookieIdx(String exclude) throws Exception {
        // shutdown bookie which is not an auditor
        int indexOf = indexOfServer(getAuditorBookie());
        int bkIndexDownBookie = 0;
        for (int i = 0; i <= lastBookieIndex(); i++) {
            if (i == indexOf || addressByIndex(i).toString().equals(exclude)) {
                continue;
            }
            bkIndexDownBookie = i;
            break;
        }
        return bkIndexDownBookie;
    }

    private String shutDownNonAuditorBookie(String exclude) throws Exception {
        return shutdownBookie(getShutDownNonAuditorBookieIdx(exclude));
    }
}
