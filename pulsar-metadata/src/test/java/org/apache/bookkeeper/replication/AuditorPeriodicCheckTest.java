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
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotSame;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;
import io.netty.buffer.ByteBuf;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieAccessor;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.bookie.IndexPersistenceMgr;
import org.apache.bookkeeper.bookie.TestBookieImpl;
import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.apache.bookkeeper.meta.MetadataBookieDriver;
import org.apache.bookkeeper.meta.MetadataDrivers;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.bookkeeper.replication.ReplicationException.UnavailableException;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.test.TestStatsProvider;
import org.apache.bookkeeper.test.TestStatsProvider.TestOpStatsLogger;
import org.apache.bookkeeper.test.TestStatsProvider.TestStatsLogger;
import org.awaitility.Awaitility;
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

    /**
     * test that the periodic checking will detect corruptions in
     * the bookie entry log.
     */
    @Test
    public void testEntryLogCorruption() throws Exception {
        LedgerManagerFactory mFactory = driver.getLedgerManagerFactory();
        LedgerUnderreplicationManager underReplicationManager = mFactory.newLedgerUnderreplicationManager();
        underReplicationManager.disableLedgerReplication();

        LedgerHandle lh = bkc.createLedger(3, 3, DigestType.CRC32, "passwd".getBytes());
        long ledgerId = lh.getId();
        for (int i = 0; i < 100; i++) {
            lh.addEntry("testdata".getBytes());
        }
        lh.close();

        BookieAccessor.forceFlush((BookieImpl) serverByIndex(0).getBookie());


        File ledgerDir = confByIndex(0).getLedgerDirs()[0];
        ledgerDir = BookieImpl.getCurrentDirectory(ledgerDir);
        // corrupt of entryLogs
        File[] entryLogs = ledgerDir.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.endsWith(".log");
            }
        });
        ByteBuffer junk = ByteBuffer.allocate(1024 * 1024);
        for (File f : entryLogs) {
            FileOutputStream out = new FileOutputStream(f);
            out.getChannel().write(junk);
            out.close();
        }
        restartBookies(); // restart to clear read buffers

        underReplicationManager.enableLedgerReplication();
        long underReplicatedLedger = -1;
        for (int i = 0; i < 10; i++) {
            underReplicatedLedger = underReplicationManager.pollLedgerToRereplicate();
            if (underReplicatedLedger != -1) {
                break;
            }
            Thread.sleep(CHECK_INTERVAL * 1000);
        }
        assertEquals("Ledger should be under replicated", ledgerId, underReplicatedLedger);
        underReplicationManager.close();
    }

    /**
     * test that the period checker will detect corruptions in
     * the bookie index files.
     */
    @Test
    public void testIndexCorruption() throws Exception {
        LedgerManagerFactory mFactory = driver.getLedgerManagerFactory();

        LedgerUnderreplicationManager underReplicationManager = mFactory.newLedgerUnderreplicationManager();

        LedgerHandle lh = bkc.createLedger(3, 3, DigestType.CRC32, "passwd".getBytes());
        long ledgerToCorrupt = lh.getId();
        for (int i = 0; i < 100; i++) {
            lh.addEntry("testdata".getBytes());
        }
        lh.close();

        // push ledgerToCorrupt out of page cache (bookie is configured to only use 1 page)
        lh = bkc.createLedger(3, 3, DigestType.CRC32, "passwd".getBytes());
        for (int i = 0; i < 100; i++) {
            lh.addEntry("testdata".getBytes());
        }
        lh.close();

        BookieAccessor.forceFlush((BookieImpl) serverByIndex(0).getBookie());

        File ledgerDir = confByIndex(0).getLedgerDirs()[0];
        ledgerDir = BookieImpl.getCurrentDirectory(ledgerDir);

        // corrupt of entryLogs
        File index = new File(ledgerDir, IndexPersistenceMgr.getLedgerName(ledgerToCorrupt));
        LOG.info("file to corrupt{}", index);
        ByteBuffer junk = ByteBuffer.allocate(1024 * 1024);
        FileOutputStream out = new FileOutputStream(index);
        out.getChannel().write(junk);
        out.close();

        long underReplicatedLedger = -1;
        for (int i = 0; i < 15; i++) {
            underReplicatedLedger = underReplicationManager.pollLedgerToRereplicate();
            if (underReplicatedLedger != -1) {
                break;
            }
            Thread.sleep(CHECK_INTERVAL * 1000);
        }
        assertEquals("Ledger should be under replicated", ledgerToCorrupt, underReplicatedLedger);
        underReplicationManager.close();
    }

    /**
     * Test that the period checker will not run when auto replication has been disabled.
     */
    @Test
    public void testPeriodicCheckWhenDisabled() throws Exception {
        LedgerManagerFactory mFactory = driver.getLedgerManagerFactory();
        final LedgerUnderreplicationManager underReplicationManager = mFactory.newLedgerUnderreplicationManager();
        final int numLedgers = 10;
        final int numMsgs = 2;
        final CountDownLatch completeLatch = new CountDownLatch(numMsgs * numLedgers);
        final AtomicInteger rc = new AtomicInteger(BKException.Code.OK);

        List<LedgerHandle> lhs = new ArrayList<LedgerHandle>();
        for (int i = 0; i < numLedgers; i++) {
            LedgerHandle lh = bkc.createLedger(3, 3, DigestType.CRC32, "passwd".getBytes());
            lhs.add(lh);
            for (int j = 0; j < 2; j++) {
                lh.asyncAddEntry("testdata".getBytes(), new AddCallback() {
                    public void addComplete(int rc2, LedgerHandle lh, long entryId, Object ctx) {
                        if (rc.compareAndSet(BKException.Code.OK, rc2)) {
                            LOG.info("Failed to add entry : {}", BKException.getMessage(rc2));
                        }
                        completeLatch.countDown();
                    }
                }, null);
            }
        }
        completeLatch.await();
        if (rc.get() != BKException.Code.OK) {
            throw BKException.create(rc.get());
        }

        for (LedgerHandle lh : lhs) {
            lh.close();
        }

        underReplicationManager.disableLedgerReplication();

        final AtomicInteger numReads = new AtomicInteger(0);
        ServerConfiguration conf = killBookie(0);

        Bookie deadBookie = new TestBookieImpl(conf) {
            @Override
            public ByteBuf readEntry(long ledgerId, long entryId)
                    throws IOException, NoLedgerException {
                // we want to disable during checking
                numReads.incrementAndGet();
                throw new IOException("Fake I/O exception");
            }
        };
        startAndAddBookie(conf, deadBookie);

        Thread.sleep(CHECK_INTERVAL * 2000);
        assertEquals("Nothing should have tried to read", 0, numReads.get());
        underReplicationManager.enableLedgerReplication();
        Thread.sleep(CHECK_INTERVAL * 2000); // give it time to run

        underReplicationManager.disableLedgerReplication();
        // give it time to stop, from this point nothing new should be marked
        Thread.sleep(CHECK_INTERVAL * 2000);

        int numUnderreplicated = 0;
        long underReplicatedLedger = -1;
        do {
            underReplicatedLedger = underReplicationManager.pollLedgerToRereplicate();
            if (underReplicatedLedger == -1) {
                break;
            }
            numUnderreplicated++;

            underReplicationManager.markLedgerReplicated(underReplicatedLedger);
        } while (underReplicatedLedger != -1);

        Thread.sleep(CHECK_INTERVAL * 2000); // give a chance to run again (it shouldn't, it's disabled)

        // ensure that nothing is marked as underreplicated
        underReplicatedLedger = underReplicationManager.pollLedgerToRereplicate();
        assertEquals("There should be no underreplicated ledgers", -1, underReplicatedLedger);

        LOG.info("{} of {} ledgers underreplicated", numUnderreplicated, numUnderreplicated);
        assertTrue("All should be underreplicated",
                numUnderreplicated <= numLedgers && numUnderreplicated > 0);
    }

    /**
     * Test that the period check will succeed if a ledger is deleted midway.
     */
    @Test
    public void testPeriodicCheckWhenLedgerDeleted() throws Exception {
        for (AuditorElector e : auditorElectors.values()) {
            e.shutdown();
        }

        final int numLedgers = 10;
        List<Long> ids = new LinkedList<Long>();
        for (int i = 0; i < numLedgers; i++) {
            LedgerHandle lh = bkc.createLedger(3, 3, DigestType.CRC32, "passwd".getBytes());
            ids.add(lh.getId());
            for (int j = 0; j < 2; j++) {
                lh.addEntry("testdata".getBytes());
            }
            lh.close();
        }

        try (final Auditor auditor = new Auditor(
                BookieImpl.getBookieId(confByIndex(0)).toString(),
                confByIndex(0), NullStatsLogger.INSTANCE)) {
            final AtomicBoolean exceptionCaught = new AtomicBoolean(false);
            final CountDownLatch latch = new CountDownLatch(1);
            Thread t = new Thread() {
                public void run() {
                    try {
                        latch.countDown();
                        for (int i = 0; i < numLedgers; i++) {
                            ((AuditorCheckAllLedgersTask) auditor.auditorCheckAllLedgersTask).checkAllLedgers();
                        }
                    } catch (Exception e) {
                        LOG.error("Caught exception while checking all ledgers", e);
                        exceptionCaught.set(true);
                    }
                }
            };
            t.start();
            latch.await();
            for (Long id : ids) {
                bkc.deleteLedger(id);
            }
            t.join();
            assertFalse("Shouldn't have thrown exception", exceptionCaught.get());
        }
    }

    @Test
    public void testGetLedgerFromZookeeperThrottled() throws Exception {
        final int numberLedgers = 30;

        // write ledgers into bookkeeper cluster
        try {
            for (AuditorElector e : auditorElectors.values()) {
                e.shutdown();
            }

            for (int i = 0; i < numberLedgers; ++i) {
                LedgerHandle lh = bkc.createLedger(3, 3, DigestType.CRC32, "passwd".getBytes());
                for (int j = 0; j < 5; j++) {
                    lh.addEntry("testdata".getBytes());
                }
                lh.close();
            }
        } catch (InterruptedException | BKException e) {
            LOG.error("Failed to shutdown auditor elector or write data to ledgers ", e);
            fail();
        }

        // create auditor and call `checkAllLedgers`
        ServerConfiguration configuration = confByIndex(0);
        configuration.setAuditorMaxNumberOfConcurrentOpenLedgerOperations(10);

        TestStatsProvider statsProvider = new TestStatsProvider();
        TestStatsLogger statsLogger = statsProvider.getStatsLogger(AUDITOR_SCOPE);
        Counter numLedgersChecked = statsLogger
                .getCounter(ReplicationStats.NUM_LEDGERS_CHECKED);
        Auditor auditor = new Auditor(BookieImpl.getBookieId(configuration).toString(),
                configuration, statsLogger);

        try {
            ((AuditorCheckAllLedgersTask) auditor.auditorCheckAllLedgersTask).checkAllLedgers();
            assertEquals("NUM_LEDGERS_CHECKED", numberLedgers, (long) numLedgersChecked.get());
        } catch (Exception e) {
            LOG.error("Caught exception while checking all ledgers ", e);
            fail();
        }
    }

    @Test
    public void testInitialDelayOfCheckAllLedgers() throws Exception {
        for (AuditorElector e : auditorElectors.values()) {
            e.shutdown();
        }

        final int numLedgers = 10;
        List<Long> ids = new LinkedList<Long>();
        for (int i = 0; i < numLedgers; i++) {
            LedgerHandle lh = bkc.createLedger(3, 3, DigestType.CRC32, "passwd".getBytes());
            ids.add(lh.getId());
            for (int j = 0; j < 2; j++) {
                lh.addEntry("testdata".getBytes());
            }
            lh.close();
        }

        LedgerManagerFactory mFactory = driver.getLedgerManagerFactory();
        LedgerUnderreplicationManager urm = mFactory.newLedgerUnderreplicationManager();

        ServerConfiguration servConf = new ServerConfiguration(confByIndex(0));
        validateInitialDelayOfCheckAllLedgers(urm, -1, 1000, servConf, bkc);
        validateInitialDelayOfCheckAllLedgers(urm, 999, 1000, servConf, bkc);
        validateInitialDelayOfCheckAllLedgers(urm, 1001, 1000, servConf, bkc);
    }

    void validateInitialDelayOfCheckAllLedgers(LedgerUnderreplicationManager urm, long timeSinceLastExecutedInSecs,
                                               long auditorPeriodicCheckInterval, ServerConfiguration servConf,
                                               BookKeeper bkc)
            throws UnavailableException, UnknownHostException, InterruptedException {
        TestStatsProvider statsProvider = new TestStatsProvider();
        TestStatsLogger statsLogger = statsProvider.getStatsLogger(AUDITOR_SCOPE);
        TestOpStatsLogger checkAllLedgersStatsLogger = (TestOpStatsLogger) statsLogger
                .getOpStatsLogger(ReplicationStats.CHECK_ALL_LEDGERS_TIME);
        servConf.setAuditorPeriodicCheckInterval(auditorPeriodicCheckInterval);
        servConf.setAuditorPeriodicPlacementPolicyCheckInterval(0);
        servConf.setAuditorPeriodicBookieCheckInterval(0);

        final TestAuditor auditor = new TestAuditor(BookieImpl.getBookieId(servConf).toString(), servConf, bkc, false,
                statsLogger, null);
        CountDownLatch latch = auditor.getLatch();
        assertEquals("CHECK_ALL_LEDGERS_TIME SuccessCount", 0, checkAllLedgersStatsLogger.getSuccessCount());
        long curTimeBeforeStart = System.currentTimeMillis();
        long checkAllLedgersCTime = -1;
        long initialDelayInMsecs = -1;
        long nextExpectedCheckAllLedgersExecutionTime = -1;
        long bufferTimeInMsecs = 12000L;
        if (timeSinceLastExecutedInSecs == -1) {
            /*
             * if we are setting checkAllLedgersCTime to -1, it means that
             * checkAllLedgers hasn't run before. So initialDelay for
             * checkAllLedgers should be 0.
             */
            checkAllLedgersCTime = -1;
            initialDelayInMsecs = 0;
        } else {
            checkAllLedgersCTime = curTimeBeforeStart - timeSinceLastExecutedInSecs * 1000L;
            initialDelayInMsecs = timeSinceLastExecutedInSecs > auditorPeriodicCheckInterval ? 0
                    : (auditorPeriodicCheckInterval - timeSinceLastExecutedInSecs) * 1000L;
        }
        /*
         * next checkAllLedgers should happen atleast after
         * nextExpectedCheckAllLedgersExecutionTime.
         */
        nextExpectedCheckAllLedgersExecutionTime = curTimeBeforeStart + initialDelayInMsecs;

        urm.setCheckAllLedgersCTime(checkAllLedgersCTime);
        auditor.start();
        /*
         * since auditorPeriodicCheckInterval are higher values (in the order of
         * 100s of seconds), its ok bufferTimeInMsecs to be ` 10 secs.
         */
        assertTrue("checkAllLedgers should have executed with initialDelay " + initialDelayInMsecs,
                latch.await(initialDelayInMsecs + bufferTimeInMsecs, TimeUnit.MILLISECONDS));
        for (int i = 0; i < 10; i++) {
            Thread.sleep(100);
            if (checkAllLedgersStatsLogger.getSuccessCount() >= 1) {
                break;
            }
        }
        assertEquals("CHECK_ALL_LEDGERS_TIME SuccessCount", 1, checkAllLedgersStatsLogger.getSuccessCount());
        long currentCheckAllLedgersCTime = urm.getCheckAllLedgersCTime();
        assertTrue(
                "currentCheckAllLedgersCTime: " + currentCheckAllLedgersCTime
                        + " should be greater than nextExpectedCheckAllLedgersExecutionTime: "
                        + nextExpectedCheckAllLedgersExecutionTime,
                currentCheckAllLedgersCTime > nextExpectedCheckAllLedgersExecutionTime);
        assertTrue(
                "currentCheckAllLedgersCTime: " + currentCheckAllLedgersCTime
                        + " should be lesser than nextExpectedCheckAllLedgersExecutionTime+bufferTimeInMsecs: "
                        + (nextExpectedCheckAllLedgersExecutionTime + bufferTimeInMsecs),
                currentCheckAllLedgersCTime < (nextExpectedCheckAllLedgersExecutionTime + bufferTimeInMsecs));
        auditor.close();
    }

    @Test
    public void testInitialDelayOfPlacementPolicyCheck() throws Exception {
        for (AuditorElector e : auditorElectors.values()) {
            e.shutdown();
        }

        final int numLedgers = 10;
        List<Long> ids = new LinkedList<Long>();
        for (int i = 0; i < numLedgers; i++) {
            LedgerHandle lh = bkc.createLedger(3, 3, DigestType.CRC32, "passwd".getBytes());
            ids.add(lh.getId());
            for (int j = 0; j < 2; j++) {
                lh.addEntry("testdata".getBytes());
            }
            lh.close();
        }

        LedgerManagerFactory mFactory = driver.getLedgerManagerFactory();
        LedgerUnderreplicationManager urm = mFactory.newLedgerUnderreplicationManager();

        ServerConfiguration servConf = new ServerConfiguration(confByIndex(0));
        validateInitialDelayOfPlacementPolicyCheck(urm, -1, 1000, servConf, bkc);
        validateInitialDelayOfPlacementPolicyCheck(urm, 999, 1000, servConf, bkc);
        validateInitialDelayOfPlacementPolicyCheck(urm, 1001, 1000, servConf, bkc);
    }

    void validateInitialDelayOfPlacementPolicyCheck(LedgerUnderreplicationManager urm, long timeSinceLastExecutedInSecs,
                                                    long auditorPeriodicPlacementPolicyCheckInterval,
                                                    ServerConfiguration servConf, BookKeeper bkc)
            throws UnavailableException, UnknownHostException, InterruptedException {
        TestStatsProvider statsProvider = new TestStatsProvider();
        TestStatsLogger statsLogger = statsProvider.getStatsLogger(AUDITOR_SCOPE);
        TestOpStatsLogger placementPolicyCheckStatsLogger = (TestOpStatsLogger) statsLogger
                .getOpStatsLogger(ReplicationStats.PLACEMENT_POLICY_CHECK_TIME);
        servConf.setAuditorPeriodicPlacementPolicyCheckInterval(auditorPeriodicPlacementPolicyCheckInterval);
        servConf.setAuditorPeriodicCheckInterval(0);
        servConf.setAuditorPeriodicBookieCheckInterval(0);

        final TestAuditor auditor = new TestAuditor(BookieImpl.getBookieId(servConf).toString(), servConf, bkc, false,
                statsLogger, null);
        CountDownLatch latch = auditor.getLatch();
        assertEquals("PLACEMENT_POLICY_CHECK_TIME SuccessCount", 0, placementPolicyCheckStatsLogger.getSuccessCount());
        long curTimeBeforeStart = System.currentTimeMillis();
        long placementPolicyCheckCTime = -1;
        long initialDelayInMsecs = -1;
        long nextExpectedPlacementPolicyCheckExecutionTime = -1;
        long bufferTimeInMsecs = 20000L;
        if (timeSinceLastExecutedInSecs == -1) {
            /*
             * if we are setting placementPolicyCheckCTime to -1, it means that
             * placementPolicyCheck hasn't run before. So initialDelay for
             * placementPolicyCheck should be 0.
             */
            placementPolicyCheckCTime = -1;
            initialDelayInMsecs = 0;
        } else {
            placementPolicyCheckCTime = curTimeBeforeStart - timeSinceLastExecutedInSecs * 1000L;
            initialDelayInMsecs = timeSinceLastExecutedInSecs > auditorPeriodicPlacementPolicyCheckInterval ? 0
                    : (auditorPeriodicPlacementPolicyCheckInterval - timeSinceLastExecutedInSecs) * 1000L;
        }
        /*
         * next placementPolicyCheck should happen atleast after
         * nextExpectedPlacementPolicyCheckExecutionTime.
         */
        nextExpectedPlacementPolicyCheckExecutionTime = curTimeBeforeStart + initialDelayInMsecs;

        urm.setPlacementPolicyCheckCTime(placementPolicyCheckCTime);
        auditor.start();
        /*
         * since auditorPeriodicPlacementPolicyCheckInterval are higher values (in the
         * order of 100s of seconds), its ok bufferTimeInMsecs to be ` 20 secs.
         */
        assertTrue("placementPolicyCheck should have executed with initialDelay " + initialDelayInMsecs,
                latch.await(initialDelayInMsecs + bufferTimeInMsecs, TimeUnit.MILLISECONDS));
        for (int i = 0; i < 20; i++) {
            Thread.sleep(100);
            if (placementPolicyCheckStatsLogger.getSuccessCount() >= 1) {
                break;
            }
        }
        assertEquals("PLACEMENT_POLICY_CHECK_TIME SuccessCount", 1, placementPolicyCheckStatsLogger.getSuccessCount());
        long currentPlacementPolicyCheckCTime = urm.getPlacementPolicyCheckCTime();
        assertTrue(
                "currentPlacementPolicyCheckCTime: " + currentPlacementPolicyCheckCTime
                        + " should be greater than nextExpectedPlacementPolicyCheckExecutionTime: "
                        + nextExpectedPlacementPolicyCheckExecutionTime,
                currentPlacementPolicyCheckCTime > nextExpectedPlacementPolicyCheckExecutionTime);
        assertTrue(
                "currentPlacementPolicyCheckCTime: " + currentPlacementPolicyCheckCTime
                        + " should be lesser than nextExpectedPlacementPolicyCheckExecutionTime+bufferTimeInMsecs: "
                        + (nextExpectedPlacementPolicyCheckExecutionTime + bufferTimeInMsecs),
                currentPlacementPolicyCheckCTime < (nextExpectedPlacementPolicyCheckExecutionTime + bufferTimeInMsecs));
        auditor.close();
    }

    @Test
    public void testInitialDelayOfReplicasCheck() throws Exception {
        for (AuditorElector e : auditorElectors.values()) {
            e.shutdown();
        }

        LedgerHandle lh = bkc.createLedger(3, 2, DigestType.CRC32, "passwd".getBytes());
        for (int j = 0; j < 5; j++) {
            lh.addEntry("testdata".getBytes());
        }
        lh.close();

        long ledgerId = 100000L;
        lh = bkc.createLedgerAdv(ledgerId, 3, 2, 2, DigestType.CRC32, "passwd".getBytes(), null);
        lh.close();

        ledgerId = 100001234L;
        lh = bkc.createLedgerAdv(ledgerId, 3, 3, 2, DigestType.CRC32, "passwd".getBytes(), null);
        for (int j = 0; j < 4; j++) {
            lh.addEntry(j, "testdata".getBytes());
        }
        lh.close();

        ledgerId = 991234L;
        lh = bkc.createLedgerAdv(ledgerId, 3, 2, 2, DigestType.CRC32, "passwd".getBytes(), null);
        lh.addEntry(0, "testdata".getBytes());
        lh.close();

        LedgerManagerFactory mFactory = driver.getLedgerManagerFactory();
        LedgerUnderreplicationManager urm = mFactory.newLedgerUnderreplicationManager();

        ServerConfiguration servConf = new ServerConfiguration(confByIndex(0));
        validateInitialDelayOfReplicasCheck(urm, -1, 1000, servConf, bkc);
        validateInitialDelayOfReplicasCheck(urm, 999, 1000, servConf, bkc);
        validateInitialDelayOfReplicasCheck(urm, 1001, 1000, servConf, bkc);
    }

    void validateInitialDelayOfReplicasCheck(LedgerUnderreplicationManager urm, long timeSinceLastExecutedInSecs,
                                             long auditorPeriodicReplicasCheckInterval, ServerConfiguration servConf,
                                             BookKeeper bkc)
            throws UnavailableException, UnknownHostException, InterruptedException {
        TestStatsProvider statsProvider = new TestStatsProvider();
        TestStatsLogger statsLogger = statsProvider.getStatsLogger(AUDITOR_SCOPE);
        TestOpStatsLogger replicasCheckStatsLogger = (TestOpStatsLogger) statsLogger
                .getOpStatsLogger(ReplicationStats.REPLICAS_CHECK_TIME);
        servConf.setAuditorPeriodicReplicasCheckInterval(auditorPeriodicReplicasCheckInterval);
        servConf.setAuditorPeriodicCheckInterval(0);
        servConf.setAuditorPeriodicBookieCheckInterval(0);
        final TestAuditor auditor = new TestAuditor(BookieImpl.getBookieId(servConf).toString(), servConf, bkc, false,
                statsLogger, null);
        CountDownLatch latch = auditor.getLatch();
        assertEquals("REPLICAS_CHECK_TIME SuccessCount", 0, replicasCheckStatsLogger.getSuccessCount());
        long curTimeBeforeStart = System.currentTimeMillis();
        long replicasCheckCTime = -1;
        long initialDelayInMsecs = -1;
        long nextExpectedReplicasCheckExecutionTime = -1;
        long bufferTimeInMsecs = 20000L;
        if (timeSinceLastExecutedInSecs == -1) {
            /*
             * if we are setting replicasCheckCTime to -1, it means that
             * replicasCheck hasn't run before. So initialDelay for
             * replicasCheck should be 0.
             */
            replicasCheckCTime = -1;
            initialDelayInMsecs = 0;
        } else {
            replicasCheckCTime = curTimeBeforeStart - timeSinceLastExecutedInSecs * 1000L;
            initialDelayInMsecs = timeSinceLastExecutedInSecs > auditorPeriodicReplicasCheckInterval ? 0
                    : (auditorPeriodicReplicasCheckInterval - timeSinceLastExecutedInSecs) * 1000L;
        }
        /*
         * next replicasCheck should happen atleast after
         * nextExpectedReplicasCheckExecutionTime.
         */
        nextExpectedReplicasCheckExecutionTime = curTimeBeforeStart + initialDelayInMsecs;

        urm.setReplicasCheckCTime(replicasCheckCTime);
        auditor.start();
        /*
         * since auditorPeriodicReplicasCheckInterval are higher values (in the
         * order of 100s of seconds), its ok bufferTimeInMsecs to be ` 20 secs.
         */
        assertTrue("replicasCheck should have executed with initialDelay " + initialDelayInMsecs,
                latch.await(initialDelayInMsecs + bufferTimeInMsecs, TimeUnit.MILLISECONDS));
        for (int i = 0; i < 20; i++) {
            Thread.sleep(100);
            if (replicasCheckStatsLogger.getSuccessCount() >= 1) {
                break;
            }
        }
        assertEquals("REPLICAS_CHECK_TIME SuccessCount", 1, replicasCheckStatsLogger.getSuccessCount());
        long currentReplicasCheckCTime = urm.getReplicasCheckCTime();
        assertTrue(
                "currentReplicasCheckCTime: " + currentReplicasCheckCTime
                        + " should be greater than nextExpectedReplicasCheckExecutionTime: "
                        + nextExpectedReplicasCheckExecutionTime,
                currentReplicasCheckCTime > nextExpectedReplicasCheckExecutionTime);
        assertTrue(
                "currentReplicasCheckCTime: " + currentReplicasCheckCTime
                        + " should be lesser than nextExpectedReplicasCheckExecutionTime+bufferTimeInMsecs: "
                        + (nextExpectedReplicasCheckExecutionTime + bufferTimeInMsecs),
                currentReplicasCheckCTime < (nextExpectedReplicasCheckExecutionTime + bufferTimeInMsecs));
        auditor.close();
    }

    @Test
    public void testDelayBookieAuditOfCheckAllLedgers() throws Exception {
        for (AuditorElector e : auditorElectors.values()) {
            e.shutdown();
        }

        final int numLedgers = 10;
        List<Long> ids = new LinkedList<Long>();
        for (int i = 0; i < numLedgers; i++) {
            LedgerHandle lh = bkc.createLedger(3, 3, DigestType.CRC32, "passwd".getBytes());
            ids.add(lh.getId());
            for (int j = 0; j < 2; j++) {
                lh.addEntry("testdata".getBytes());
            }
            lh.close();
        }

        LedgerManagerFactory mFactory = driver.getLedgerManagerFactory();
        LedgerUnderreplicationManager urm = mFactory.newLedgerUnderreplicationManager();

        ServerConfiguration servConf = new ServerConfiguration(confByIndex(0));

        TestStatsProvider statsProvider = new TestStatsProvider();
        TestStatsLogger statsLogger = statsProvider.getStatsLogger(AUDITOR_SCOPE);
        Counter numBookieAuditsDelayed =
                statsLogger.getCounter(ReplicationStats.NUM_BOOKIE_AUDITS_DELAYED);
        TestOpStatsLogger underReplicatedLedgerTotalSizeStatsLogger = (TestOpStatsLogger) statsLogger
                .getOpStatsLogger(ReplicationStats.UNDER_REPLICATED_LEDGERS_TOTAL_SIZE);

        servConf.setAuditorPeriodicCheckInterval(1);
        servConf.setAuditorPeriodicPlacementPolicyCheckInterval(0);
        servConf.setAuditorPeriodicBookieCheckInterval(Long.MAX_VALUE);

        urm.setLostBookieRecoveryDelay(Integer.MAX_VALUE);

        AtomicBoolean canRun = new AtomicBoolean(false);

        final TestAuditor auditor = new TestAuditor(BookieImpl.getBookieId(servConf).toString(), servConf, bkc,
                false, statsLogger, canRun);
        final CountDownLatch latch = auditor.getLatch();

        auditor.start();

        killBookie(addressByIndex(0));

        Awaitility.await().untilAsserted(() -> assertEquals(1, (long) numBookieAuditsDelayed.get()));
        final Future<?> auditTask = auditor.auditTask;
        assertTrue(auditTask != null && !auditTask.isDone());

        canRun.set(true);

        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertTrue(auditor.auditTask.equals(auditTask)
                && auditor.auditTask != null && !auditor.auditTask.isDone());
        // wrong num is numLedgers, right num is 0
        assertEquals("UNDER_REPLICATED_LEDGERS_TOTAL_SIZE",
                0,
                underReplicatedLedgerTotalSizeStatsLogger.getSuccessCount());

        auditor.close();
    }

    @Test
    public void testDelayBookieAuditOfPlacementPolicy() throws Exception {
        for (AuditorElector e : auditorElectors.values()) {
            e.shutdown();
        }

        final int numLedgers = 10;
        List<Long> ids = new LinkedList<Long>();
        for (int i = 0; i < numLedgers; i++) {
            LedgerHandle lh = bkc.createLedger(3, 3, DigestType.CRC32, "passwd".getBytes());
            ids.add(lh.getId());
            for (int j = 0; j < 2; j++) {
                lh.addEntry("testdata".getBytes());
            }
            lh.close();
        }

        LedgerManagerFactory mFactory = driver.getLedgerManagerFactory();
        LedgerUnderreplicationManager urm = mFactory.newLedgerUnderreplicationManager();

        ServerConfiguration servConf = new ServerConfiguration(confByIndex(0));

        TestStatsProvider statsProvider = new TestStatsProvider();
        TestStatsLogger statsLogger = statsProvider.getStatsLogger(AUDITOR_SCOPE);
        Counter numBookieAuditsDelayed =
                statsLogger.getCounter(ReplicationStats.NUM_BOOKIE_AUDITS_DELAYED);
        TestOpStatsLogger placementPolicyCheckTime = (TestOpStatsLogger) statsLogger
                .getOpStatsLogger(ReplicationStats.PLACEMENT_POLICY_CHECK_TIME);

        servConf.setAuditorPeriodicCheckInterval(0);
        servConf.setAuditorPeriodicPlacementPolicyCheckInterval(1);
        servConf.setAuditorPeriodicBookieCheckInterval(Long.MAX_VALUE);

        urm.setLostBookieRecoveryDelay(Integer.MAX_VALUE);

        AtomicBoolean canRun = new AtomicBoolean(false);

        final TestAuditor auditor = new TestAuditor(BookieImpl.getBookieId(servConf).toString(), servConf, bkc,
                false, statsLogger, canRun);
        final CountDownLatch latch = auditor.getLatch();

        auditor.start();

        killBookie(addressByIndex(0));

        Awaitility.await().untilAsserted(() -> assertEquals(1, (long) numBookieAuditsDelayed.get()));
        final Future<?> auditTask = auditor.auditTask;
        assertTrue(auditTask != null && !auditTask.isDone());
        assertEquals("PLACEMENT_POLICY_CHECK_TIME", 0, placementPolicyCheckTime.getSuccessCount());

        canRun.set(true);

        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertTrue(auditor.auditTask.equals(auditTask)
                && auditor.auditTask != null && !auditor.auditTask.isDone());
        // wrong successCount is > 0, right successCount is = 0
        assertEquals("PLACEMENT_POLICY_CHECK_TIME", 0, placementPolicyCheckTime.getSuccessCount());

        auditor.close();
    }

    @Test
    public void testDelayBookieAuditOfReplicasCheck() throws Exception {
        for (AuditorElector e : auditorElectors.values()) {
            e.shutdown();
        }

        final int numLedgers = 10;
        List<Long> ids = new LinkedList<Long>();
        for (int i = 0; i < numLedgers; i++) {
            LedgerHandle lh = bkc.createLedger(3, 3, DigestType.CRC32, "passwd".getBytes());
            ids.add(lh.getId());
            for (int j = 0; j < 2; j++) {
                lh.addEntry("testdata".getBytes());
            }
            lh.close();
        }

        LedgerManagerFactory mFactory = driver.getLedgerManagerFactory();
        LedgerUnderreplicationManager urm = mFactory.newLedgerUnderreplicationManager();

        ServerConfiguration servConf = new ServerConfiguration(confByIndex(0));

        TestStatsProvider statsProvider = new TestStatsProvider();
        TestStatsLogger statsLogger = statsProvider.getStatsLogger(AUDITOR_SCOPE);
        Counter numBookieAuditsDelayed =
                statsLogger.getCounter(ReplicationStats.NUM_BOOKIE_AUDITS_DELAYED);
        TestOpStatsLogger replicasCheckTime = (TestOpStatsLogger) statsLogger
                .getOpStatsLogger(ReplicationStats.REPLICAS_CHECK_TIME);

        servConf.setAuditorPeriodicCheckInterval(0);
        servConf.setAuditorPeriodicPlacementPolicyCheckInterval(0);
        servConf.setAuditorPeriodicBookieCheckInterval(Long.MAX_VALUE);
        servConf.setAuditorPeriodicReplicasCheckInterval(1);

        urm.setLostBookieRecoveryDelay(Integer.MAX_VALUE);

        AtomicBoolean canRun = new AtomicBoolean(false);

        final TestAuditor auditor = new TestAuditor(BookieImpl.getBookieId(servConf).toString(), servConf, bkc,
                false, statsLogger, canRun);
        final CountDownLatch latch = auditor.getLatch();

        auditor.start();

        killBookie(addressByIndex(0));

        Awaitility.await().untilAsserted(() -> assertEquals(1, (long) numBookieAuditsDelayed.get()));
        final Future<?> auditTask = auditor.auditTask;
        assertTrue(auditTask != null && !auditTask.isDone());
        assertEquals("REPLICAS_CHECK_TIME", 0, replicasCheckTime.getSuccessCount());

        canRun.set(true);

        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertTrue(auditor.auditTask.equals(auditTask)
                && auditor.auditTask != null && !auditor.auditTask.isDone());
        // wrong successCount is > 0, right successCount is = 0
        assertEquals("REPLICAS_CHECK_TIME", 0, replicasCheckTime.getSuccessCount());

        auditor.close();
    }

    static class TestAuditor extends Auditor {

        final AtomicReference<CountDownLatch> latchRef = new AtomicReference<CountDownLatch>(new CountDownLatch(1));

        public TestAuditor(String bookieIdentifier, ServerConfiguration conf, BookKeeper bkc, boolean ownBkc,
                           StatsLogger statsLogger, AtomicBoolean exceptedRun) throws UnavailableException {
            super(bookieIdentifier, conf, bkc, ownBkc, statsLogger);
            renewAuditorTestWrapperTask(exceptedRun);
        }

        public TestAuditor(String bookieIdentifier, ServerConfiguration conf, BookKeeper bkc, boolean ownBkc,
                           BookKeeperAdmin bkadmin, boolean ownadmin, StatsLogger statsLogger,
                           AtomicBoolean exceptedRun) throws UnavailableException {
            super(bookieIdentifier, conf, bkc, ownBkc, bkadmin, ownadmin, statsLogger);
            renewAuditorTestWrapperTask(exceptedRun);
        }

        public TestAuditor(final String bookieIdentifier, ServerConfiguration conf, StatsLogger statsLogger,
                           AtomicBoolean exceptedRun)
                throws UnavailableException {
            super(bookieIdentifier, conf, statsLogger);
            renewAuditorTestWrapperTask(exceptedRun);
        }

        private void renewAuditorTestWrapperTask(AtomicBoolean exceptedRun) {
            super.auditorCheckAllLedgersTask =
                    new AuditorTestWrapperTask(super.auditorCheckAllLedgersTask, latchRef, exceptedRun);
            super.auditorPlacementPolicyCheckTask =
                    new AuditorTestWrapperTask(super.auditorPlacementPolicyCheckTask, latchRef, exceptedRun);
            super.auditorReplicasCheckTask =
                    new AuditorTestWrapperTask(super.auditorReplicasCheckTask, latchRef, exceptedRun);
        }

        CountDownLatch getLatch() {
            return latchRef.get();
        }

        void setLatch(CountDownLatch latch) {
            latchRef.set(latch);
        }

        private static class AuditorTestWrapperTask extends AuditorTask {
            private final AuditorTask innerTask;
            private final AtomicReference<CountDownLatch> latchRef;
            private final AtomicBoolean exceptedRun;

            AuditorTestWrapperTask(AuditorTask innerTask,
                                   AtomicReference<CountDownLatch> latchRef,
                                   AtomicBoolean exceptedRun) {
                super(null, null, null, null, null,
                        null, null);
                this.innerTask = innerTask;
                this.latchRef = latchRef;
                this.exceptedRun = exceptedRun;
            }

            @Override
            protected void runTask() {
                if (exceptedRun == null || exceptedRun.get()) {
                    innerTask.runTask();
                    latchRef.get().countDown();
                }
            }

            @Override
            public void shutdown() {
                innerTask.shutdown();
            }
        }
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
