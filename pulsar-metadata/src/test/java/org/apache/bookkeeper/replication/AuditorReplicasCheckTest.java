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
import static org.testng.AssertJUnit.assertTrue;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.LedgerMetadataBuilder;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.BookieServiceInfo;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.apache.bookkeeper.meta.MetadataBookieDriver;
import org.apache.bookkeeper.meta.MetadataDrivers;
import org.apache.bookkeeper.meta.exceptions.MetadataException;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.replication.AuditorPeriodicCheckTest.TestAuditor;
import org.apache.bookkeeper.replication.ReplicationException.CompatibilityException;
import org.apache.bookkeeper.replication.ReplicationException.UnavailableException;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.test.TestStatsProvider;
import org.apache.bookkeeper.test.TestStatsProvider.TestOpStatsLogger;
import org.apache.bookkeeper.test.TestStatsProvider.TestStatsLogger;
import org.apache.bookkeeper.util.AvailabilityOfEntriesOfLedger;
import org.apache.bookkeeper.util.StaticDNSResolver;
import org.apache.commons.collections4.map.MultiKeyMap;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.zookeeper.KeeperException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests the logic of Auditor's ReplicasCheck.
 */
public class AuditorReplicasCheckTest extends BookKeeperClusterTestCase {
    private MetadataBookieDriver driver;
    private RegistrationManager regManager;

    public AuditorReplicasCheckTest() throws Exception {
        super(1);
        baseConf.setPageLimit(1); // to make it easy to push ledger out of cache
        Class.forName("org.apache.pulsar.metadata.bookkeeper.PulsarMetadataClientDriver");
        Class.forName("org.apache.pulsar.metadata.bookkeeper.PulsarMetadataBookieDriver");
    }

    @BeforeMethod
    @Override
    public void setUp() throws Exception {
        super.setUp();
        StaticDNSResolver.reset();

        URI uri = URI.create(confByIndex(0).getMetadataServiceUri().replaceAll("zk://", "metadata-store:")
                .replaceAll("/ledgers", ""));
        driver = MetadataDrivers.getBookieDriver(uri);
        ServerConfiguration serverConfiguration = new ServerConfiguration(confByIndex(0));
        serverConfiguration.setMetadataServiceUri(
                serverConfiguration.getMetadataServiceUri().replaceAll("zk://", "metadata-store:")
                        .replaceAll("/ledgers", ""));
        driver.initialize(serverConfiguration, NullStatsLogger.INSTANCE);
        regManager = driver.createRegistrationManager();
    }

    @AfterMethod
    @Override
    public void tearDown() throws Exception {
        if (null != regManager) {
            regManager.close();
        }
        if (null != driver) {
            driver.close();
        }
        super.tearDown();
    }

    private class TestBookKeeperAdmin extends BookKeeperAdmin {

        private final MultiKeyMap<String, AvailabilityOfEntriesOfLedger> returnAvailabilityOfEntriesOfLedger;
        private final MultiKeyMap<String, Integer> errorReturnValueForGetAvailabilityOfEntriesOfLedger;

        public TestBookKeeperAdmin(BookKeeper bkc, StatsLogger statsLogger,
                MultiKeyMap<String, AvailabilityOfEntriesOfLedger> returnAvailabilityOfEntriesOfLedger,
                MultiKeyMap<String, Integer> errorReturnValueForGetAvailabilityOfEntriesOfLedger) {
            super(bkc, statsLogger, baseClientConf);
            this.returnAvailabilityOfEntriesOfLedger = returnAvailabilityOfEntriesOfLedger;
            this.errorReturnValueForGetAvailabilityOfEntriesOfLedger =
                    errorReturnValueForGetAvailabilityOfEntriesOfLedger;
        }

        @Override
        public CompletableFuture<AvailabilityOfEntriesOfLedger> asyncGetListOfEntriesOfLedger(
                BookieId address, long ledgerId) {
            CompletableFuture<AvailabilityOfEntriesOfLedger> futureResult =
                    new CompletableFuture<AvailabilityOfEntriesOfLedger>();
            Integer errorReturnValue = errorReturnValueForGetAvailabilityOfEntriesOfLedger.get(address.toString(),
                    Long.toString(ledgerId));
            if (errorReturnValue != null) {
                futureResult.completeExceptionally(BKException.create(errorReturnValue).fillInStackTrace());
            } else {
                AvailabilityOfEntriesOfLedger availabilityOfEntriesOfLedger = returnAvailabilityOfEntriesOfLedger
                        .get(address.toString(), Long.toString(ledgerId));
                futureResult.complete(availabilityOfEntriesOfLedger);
            }
            return futureResult;
        }
    }

    private TestStatsLogger startAuditorAndWaitForReplicasCheck(ServerConfiguration servConf,
            MutableObject<Auditor> auditorRef,
            MultiKeyMap<String, AvailabilityOfEntriesOfLedger> expectedReturnAvailabilityOfEntriesOfLedger,
            MultiKeyMap<String, Integer> errorReturnValueForGetAvailabilityOfEntriesOfLedger)
            throws MetadataException, CompatibilityException, KeeperException, InterruptedException,
            UnavailableException, UnknownHostException {
        LedgerManagerFactory mFactory = driver.getLedgerManagerFactory();
        LedgerUnderreplicationManager urm = mFactory.newLedgerUnderreplicationManager();
        TestStatsProvider statsProvider = new TestStatsProvider();
        TestStatsLogger statsLogger = statsProvider.getStatsLogger(AUDITOR_SCOPE);
        TestOpStatsLogger replicasCheckStatsLogger = (TestOpStatsLogger) statsLogger
                .getOpStatsLogger(ReplicationStats.REPLICAS_CHECK_TIME);

        final TestAuditor auditor = new TestAuditor(BookieImpl.getBookieId(servConf).toString(), servConf, bkc, true,
                new TestBookKeeperAdmin(bkc, statsLogger, expectedReturnAvailabilityOfEntriesOfLedger,
                        errorReturnValueForGetAvailabilityOfEntriesOfLedger),
                true, statsLogger, null);
        auditorRef.setValue(auditor);
        CountDownLatch latch = auditor.getLatch();
        assertEquals("REPLICAS_CHECK_TIME SuccessCount", 0, replicasCheckStatsLogger.getSuccessCount());
        urm.setReplicasCheckCTime(-1);
        auditor.start();
        /*
         * since replicasCheckCTime is set to -1, replicasCheck should be
         * scheduled to run with no initialdelay
         */
        assertTrue("replicasCheck should have executed", latch.await(20, TimeUnit.SECONDS));
        for (int i = 0; i < 200; i++) {
            Thread.sleep(100);
            if (replicasCheckStatsLogger.getSuccessCount() >= 1) {
                break;
            }
        }
        assertEquals("REPLICAS_CHECK_TIME SuccessCount", 1, replicasCheckStatsLogger.getSuccessCount());
        return statsLogger;
    }

    private void setServerConfigProperties(ServerConfiguration servConf) {
        servConf.setAuditorPeriodicCheckInterval(0);
        servConf.setAuditorPeriodicBookieCheckInterval(0);
        servConf.setAuditorPeriodicPlacementPolicyCheckInterval(0);
        servConf.setAuditorPeriodicReplicasCheckInterval(1000);
    }

    List<BookieId> addAndRegisterBookies(int numOfBookies)
            throws BookieException {
        BookieId bookieAddress;
        List<BookieId> bookieAddresses = new ArrayList<BookieId>();
        for (int i = 0; i < numOfBookies; i++) {
            bookieAddress = new BookieSocketAddress("98.98.98." + i, 2181).toBookieId();
            bookieAddresses.add(bookieAddress);
            regManager.registerBookie(bookieAddress, false, BookieServiceInfo.EMPTY);
        }
        return bookieAddresses;
    }

    private void createClosedLedgerMetadata(LedgerManager lm, long ledgerId, int ensembleSize, int writeQuorumSize,
            int ackQuorumSize, Map<Long, List<BookieId>> segmentEnsembles, long lastEntryId, int length,
            DigestType digestType, byte[] password) throws InterruptedException, ExecutionException {
        LedgerMetadataBuilder ledgerMetadataBuilder = LedgerMetadataBuilder.create();
        ledgerMetadataBuilder.withId(ledgerId).withEnsembleSize(ensembleSize).withWriteQuorumSize(writeQuorumSize)
                .withAckQuorumSize(ackQuorumSize).withClosedState().withLastEntryId(lastEntryId).withLength(length)
                .withDigestType(digestType).withPassword(password);
        for (Map.Entry<Long, List<BookieId>> mapEntry : segmentEnsembles.entrySet()) {
            ledgerMetadataBuilder.newEnsembleEntry(mapEntry.getKey(), mapEntry.getValue());
        }
        LedgerMetadata initMeta = ledgerMetadataBuilder.build();
        lm.createLedgerMetadata(ledgerId, initMeta).get();
    }

    private void createNonClosedLedgerMetadata(LedgerManager lm, long ledgerId, int ensembleSize, int writeQuorumSize,
            int ackQuorumSize, Map<Long, List<BookieId>> segmentEnsembles, DigestType digestType,
            byte[] password) throws InterruptedException, ExecutionException {
        LedgerMetadataBuilder ledgerMetadataBuilder = LedgerMetadataBuilder.create();
        ledgerMetadataBuilder.withId(ledgerId).withEnsembleSize(ensembleSize).withWriteQuorumSize(writeQuorumSize)
                .withAckQuorumSize(ackQuorumSize).withDigestType(digestType).withPassword(password);
        for (Map.Entry<Long, List<BookieId>> mapEntry : segmentEnsembles.entrySet()) {
            ledgerMetadataBuilder.newEnsembleEntry(mapEntry.getKey(), mapEntry.getValue());
        }
        LedgerMetadata initMeta = ledgerMetadataBuilder.build();
        lm.createLedgerMetadata(ledgerId, initMeta).get();
    }

    private void runTestScenario(MultiKeyMap<String, AvailabilityOfEntriesOfLedger> returnAvailabilityOfEntriesOfLedger,
            MultiKeyMap<String, Integer> errorReturnValueForGetAvailabilityOfEntriesOfLedger,
            int expectedNumLedgersFoundHavingNoReplicaOfAnEntry,
            int expectedNumLedgersHavingLessThanAQReplicasOfAnEntry,
            int expectedNumLedgersHavingLessThanWQReplicasOfAnEntry) throws Exception {
        ServerConfiguration servConf = new ServerConfiguration(confByIndex(0));
        setServerConfigProperties(servConf);
        MutableObject<Auditor> auditorRef = new MutableObject<Auditor>();
        try {
            TestStatsLogger statsLogger = startAuditorAndWaitForReplicasCheck(servConf, auditorRef,
                    returnAvailabilityOfEntriesOfLedger, errorReturnValueForGetAvailabilityOfEntriesOfLedger);
            checkReplicasCheckStats(statsLogger, expectedNumLedgersFoundHavingNoReplicaOfAnEntry,
                    expectedNumLedgersHavingLessThanAQReplicasOfAnEntry,
                    expectedNumLedgersHavingLessThanWQReplicasOfAnEntry);
        } finally {
            Auditor auditor = auditorRef.getValue();
            if (auditor != null) {
                auditor.close();
            }
        }
    }

    private void checkReplicasCheckStats(TestStatsLogger statsLogger,
            int expectedNumLedgersFoundHavingNoReplicaOfAnEntry,
            int expectedNumLedgersHavingLessThanAQReplicasOfAnEntry,
            int expectedNumLedgersHavingLessThanWQReplicasOfAnEntry) {
        Gauge<? extends Number> numLedgersFoundHavingNoReplicaOfAnEntryGuage = statsLogger
                .getGauge(ReplicationStats.NUM_LEDGERS_HAVING_NO_REPLICA_OF_AN_ENTRY);
        Gauge<? extends Number> numLedgersHavingLessThanAQReplicasOfAnEntryGuage = statsLogger
                .getGauge(ReplicationStats.NUM_LEDGERS_HAVING_LESS_THAN_AQ_REPLICAS_OF_AN_ENTRY);
        Gauge<? extends Number> numLedgersHavingLessThanWQReplicasOfAnEntryGuage = statsLogger
                .getGauge(ReplicationStats.NUM_LEDGERS_HAVING_LESS_THAN_WQ_REPLICAS_OF_AN_ENTRY);

        assertEquals("NUM_LEDGERS_HAVING_NO_REPLICA_OF_AN_ENTRY guage value",
                expectedNumLedgersFoundHavingNoReplicaOfAnEntry,
                numLedgersFoundHavingNoReplicaOfAnEntryGuage.getSample());
        assertEquals("NUM_LEDGERS_HAVING_LESS_THAN_AQ_REPLICAS_OF_AN_ENTRY guage value",
                expectedNumLedgersHavingLessThanAQReplicasOfAnEntry,
                numLedgersHavingLessThanAQReplicasOfAnEntryGuage.getSample());
        assertEquals("NUM_LEDGERS_HAVING_LESS_THAN_WQ_REPLICAS_OF_AN_ENTRY guage value",
                expectedNumLedgersHavingLessThanWQReplicasOfAnEntry,
                numLedgersHavingLessThanWQReplicasOfAnEntryGuage.getSample());
    }

    /*
     * For all the ledgers and for all the bookies,
     * asyncGetListOfEntriesOfLedger would return
     * BookieHandleNotAvailableException, so these ledgers wouldn't be counted
     * against expectedNumLedgersFoundHavingNoReplicaOfAnEntry /
     * LessThanAQReplicasOfAnEntry / LessThanWQReplicasOfAnEntry.
     */
    @Test
    public void testReplicasCheckForBookieHandleNotAvailable() throws Exception {
        int numOfBookies = 5;
        MultiKeyMap<String, AvailabilityOfEntriesOfLedger> returnAvailabilityOfEntriesOfLedger =
                new MultiKeyMap<String, AvailabilityOfEntriesOfLedger>();
        MultiKeyMap<String, Integer> errorReturnValueForGetAvailabilityOfEntriesOfLedger =
                new MultiKeyMap<String, Integer>();
        List<BookieId> bookieAddresses = addAndRegisterBookies(numOfBookies);

        LedgerManagerFactory mFactory = driver.getLedgerManagerFactory();
        LedgerManager lm = mFactory.newLedgerManager();
        int ensembleSize = 5;
        int writeQuorumSize = 4;
        int ackQuorumSize = 2;
        long lastEntryId = 100;
        int length = 10000;
        DigestType digestType = DigestType.DUMMY;
        byte[] password = new byte[0];
        Collections.shuffle(bookieAddresses);

        /*
         * closed ledger
         *
         * for this ledger, for all the bookies we are setting
         * errorReturnValueForGetAvailabilityOfEntriesOfLedger to
         * BookieHandleNotAvailableException so asyncGetListOfEntriesOfLedger will
         * return BookieHandleNotAvailableException.
         */
        Map<Long, List<BookieId>> segmentEnsembles = new LinkedHashMap<Long, List<BookieId>>();
        segmentEnsembles.put(0L, bookieAddresses);
        long ledgerId = 1L;
        createClosedLedgerMetadata(lm, ledgerId, ensembleSize, writeQuorumSize, ackQuorumSize, segmentEnsembles,
                lastEntryId, length, digestType, password);
        for (BookieId bookieSocketAddress : bookieAddresses) {
            errorReturnValueForGetAvailabilityOfEntriesOfLedger.put(bookieSocketAddress.toString(),
                    Long.toString(ledgerId), BKException.Code.BookieHandleNotAvailableException);
        }

        ensembleSize = 4;
        /*
         * closed ledger with multiple segments
         *
         * for this ledger, for all the bookies we are setting
         * errorReturnValueForGetAvailabilityOfEntriesOfLedger to
         * BookieHandleNotAvailableException so asyncGetListOfEntriesOfLedger will
         * return BookieHandleNotAvailableException.
         */
        segmentEnsembles.clear();
        segmentEnsembles.put(0L, bookieAddresses.subList(0, 4));
        segmentEnsembles.put(20L, bookieAddresses.subList(1, 5));
        segmentEnsembles.put(60L, bookieAddresses.subList(0, 4));
        ledgerId = 2L;
        createClosedLedgerMetadata(lm, ledgerId, ensembleSize, writeQuorumSize, ackQuorumSize, segmentEnsembles,
                lastEntryId, length, digestType, password);
        for (BookieId bookieSocketAddress : bookieAddresses) {
            errorReturnValueForGetAvailabilityOfEntriesOfLedger.put(bookieSocketAddress.toString(),
                    Long.toString(ledgerId), BKException.Code.BookieHandleNotAvailableException);
        }

        /*
         * non-closed ledger
         */
        segmentEnsembles.clear();
        segmentEnsembles.put(0L, bookieAddresses.subList(0, 4));
        ledgerId = 3L;
        createNonClosedLedgerMetadata(lm, ledgerId, ensembleSize, writeQuorumSize, ackQuorumSize, segmentEnsembles,
                digestType, password);
        for (BookieId bookieSocketAddress : bookieAddresses) {
            errorReturnValueForGetAvailabilityOfEntriesOfLedger.put(bookieSocketAddress.toString(),
                    Long.toString(ledgerId), BKException.Code.BookieHandleNotAvailableException);
        }

        /*
         * non-closed ledger with multiple segments
         *
         */
        segmentEnsembles.clear();
        segmentEnsembles.put(0L, bookieAddresses.subList(0, 4));
        segmentEnsembles.put(20L, bookieAddresses.subList(1, 5));
        segmentEnsembles.put(60L, bookieAddresses.subList(0, 4));
        ledgerId = 4L;
        createNonClosedLedgerMetadata(lm, ledgerId, ensembleSize, writeQuorumSize, ackQuorumSize, segmentEnsembles,
                digestType, password);
        for (BookieId bookieSocketAddress : bookieAddresses) {
            errorReturnValueForGetAvailabilityOfEntriesOfLedger.put(bookieSocketAddress.toString(),
                    Long.toString(ledgerId), BKException.Code.BookieHandleNotAvailableException);
        }

        runTestScenario(returnAvailabilityOfEntriesOfLedger, errorReturnValueForGetAvailabilityOfEntriesOfLedger, 0, 0,
                0);
    }

    /*
     * In this testscenario all the ledgers have a missing entry. So all closed
     * ledgers should be counted towards
     * numLedgersFoundHavingNoReplicaOfAnEntry.
     */
    @Test
    public void testReplicasCheckForLedgersFoundHavingNoReplica() throws Exception {
        int numOfBookies = 5;
        MultiKeyMap<String, AvailabilityOfEntriesOfLedger> returnAvailabilityOfEntriesOfLedger =
                new MultiKeyMap<String, AvailabilityOfEntriesOfLedger>();
        MultiKeyMap<String, Integer> errorReturnValueForGetAvailabilityOfEntriesOfLedger =
                new MultiKeyMap<String, Integer>();
        List<BookieId> bookieAddresses = addAndRegisterBookies(numOfBookies);

        LedgerManagerFactory mFactory = driver.getLedgerManagerFactory();
        LedgerManager lm = mFactory.newLedgerManager();
        int ensembleSize = 5;
        int writeQuorumSize = 4;
        int ackQuorumSize = 2;
        long lastEntryId = 100;
        int length = 10000;
        DigestType digestType = DigestType.DUMMY;
        byte[] password = new byte[0];
        Collections.shuffle(bookieAddresses);

        int numLedgersFoundHavingNoReplicaOfAnEntry = 0;

        /*
         * closed ledger
         *
         * for this ledger we are setting returnAvailabilityOfEntriesOfLedger to
         * Empty one for all of the bookies, so this ledger would be counted in
         * ledgersFoundHavingNoReplicaOfAnEntry .
         */
        Map<Long, List<BookieId>> segmentEnsembles = new LinkedHashMap<Long, List<BookieId>>();
        segmentEnsembles.put(0L, bookieAddresses);
        long ledgerId = 1L;
        createClosedLedgerMetadata(lm, ledgerId, ensembleSize, writeQuorumSize, ackQuorumSize, segmentEnsembles,
                lastEntryId, length, digestType, password);
        for (BookieId bookieSocketAddress : bookieAddresses) {
            returnAvailabilityOfEntriesOfLedger.put(bookieSocketAddress.toString(), Long.toString(ledgerId),
                    AvailabilityOfEntriesOfLedger.EMPTY_AVAILABILITYOFENTRIESOFLEDGER);
        }
        numLedgersFoundHavingNoReplicaOfAnEntry++;

        ensembleSize = 4;
        /*
         * closed ledger with multiple segments
         *
         * for this ledger we are setting
         * errorReturnValueForGetAvailabilityOfEntriesOfLedger to
         * NoSuchLedgerExistsException. This is equivalent to
         * EMPTY_AVAILABILITYOFENTRIESOFLEDGER. So this ledger would be counted
         * in ledgersFoundHavingNoReplicaOfAnEntry
         */
        segmentEnsembles.clear();
        segmentEnsembles.put(0L, bookieAddresses.subList(0, 4));
        segmentEnsembles.put(20L, bookieAddresses.subList(1, 5));
        segmentEnsembles.put(60L, bookieAddresses.subList(0, 4));
        ledgerId = 2L;
        createClosedLedgerMetadata(lm, ledgerId, ensembleSize, writeQuorumSize, ackQuorumSize, segmentEnsembles,
                lastEntryId, length, digestType, password);
        for (BookieId bookieSocketAddress : bookieAddresses) {
            errorReturnValueForGetAvailabilityOfEntriesOfLedger.put(bookieSocketAddress.toString(),
                    Long.toString(ledgerId), BKException.Code.NoSuchLedgerExistsException);
        }
        numLedgersFoundHavingNoReplicaOfAnEntry++;

        /*
         * non-closed ledger
         *
         * since this is non-closed ledger, it should not be counted in
         * ledgersFoundHavingNoReplicaOfAnEntry
         */
        segmentEnsembles.clear();
        segmentEnsembles.put(0L, bookieAddresses.subList(0, 4));
        ledgerId = 3L;
        createNonClosedLedgerMetadata(lm, ledgerId, ensembleSize, writeQuorumSize, ackQuorumSize, segmentEnsembles,
                digestType, password);
        for (BookieId bookieSocketAddress : bookieAddresses) {
            returnAvailabilityOfEntriesOfLedger.put(bookieSocketAddress.toString(), Long.toString(ledgerId),
                    AvailabilityOfEntriesOfLedger.EMPTY_AVAILABILITYOFENTRIESOFLEDGER);
        }

        ensembleSize = 3;
        writeQuorumSize = 3;
        ackQuorumSize = 2;
        lastEntryId = 1;
        length = 1000;
        /*
         * closed ledger
         *
         * for this ledger we are setting returnAvailabilityOfEntriesOfLedger to
         * just {0l} for all of the bookies and entry 1l is missing for all of
         * the bookies, so this ledger would be counted in
         * ledgersFoundHavingNoReplicaOfAnEntry
         */
        segmentEnsembles.clear();
        segmentEnsembles.put(0L, bookieAddresses.subList(0, 3));
        ledgerId = 4L;
        createClosedLedgerMetadata(lm, ledgerId, ensembleSize, writeQuorumSize, ackQuorumSize, segmentEnsembles,
                lastEntryId, length, digestType, password);
        for (BookieId bookieSocketAddress : bookieAddresses) {
            returnAvailabilityOfEntriesOfLedger.put(bookieSocketAddress.toString(), Long.toString(ledgerId),
                    new AvailabilityOfEntriesOfLedger(new long[] { 0L }));
        }
        numLedgersFoundHavingNoReplicaOfAnEntry++;

        /*
         * For this closed ledger, entry 1 is missing. So it should be counted
         * towards numLedgersFoundHavingNoReplicaOfAnEntry.
         */
        ensembleSize = 4;
        writeQuorumSize = 3;
        ackQuorumSize = 2;
        lastEntryId = 3;
        length = 10000;
        segmentEnsembles.put(0L, bookieAddresses.subList(0, 4));
        ledgerId = 5L;
        createClosedLedgerMetadata(lm, ledgerId, ensembleSize, writeQuorumSize, ackQuorumSize, segmentEnsembles,
                lastEntryId, length, digestType, password);
        returnAvailabilityOfEntriesOfLedger.put(bookieAddresses.get(0).toString(), Long.toString(ledgerId),
                new AvailabilityOfEntriesOfLedger(new long[] { 0 }));
        returnAvailabilityOfEntriesOfLedger.put(bookieAddresses.get(1).toString(), Long.toString(ledgerId),
                new AvailabilityOfEntriesOfLedger(new long[] { 0, 3 }));
        returnAvailabilityOfEntriesOfLedger.put(bookieAddresses.get(2).toString(), Long.toString(ledgerId),
                new AvailabilityOfEntriesOfLedger(new long[] { 0 }));
        returnAvailabilityOfEntriesOfLedger.put(bookieAddresses.get(3).toString(), Long.toString(ledgerId),
                new AvailabilityOfEntriesOfLedger(new long[] { 2, 3 }));
        numLedgersFoundHavingNoReplicaOfAnEntry++;

        runTestScenario(returnAvailabilityOfEntriesOfLedger, errorReturnValueForGetAvailabilityOfEntriesOfLedger,
                numLedgersFoundHavingNoReplicaOfAnEntry, 0, 0);
    }

    /*
     * In this testscenario all the ledgers have an entry with less than AQ
     * number of copies. So all closed ledgers should be counted towards
     * numLedgersFoundHavingLessThanAQReplicasOfAnEntry.
     */
    @Test
    public void testReplicasCheckForLedgersFoundHavingLessThanAQReplicasOfAnEntry() throws Exception {
        int numOfBookies = 5;
        MultiKeyMap<String, AvailabilityOfEntriesOfLedger> returnAvailabilityOfEntriesOfLedger =
                new MultiKeyMap<String, AvailabilityOfEntriesOfLedger>();
        MultiKeyMap<String, Integer> errorReturnValueForGetAvailabilityOfEntriesOfLedger =
                new MultiKeyMap<String, Integer>();
        List<BookieId> bookieAddresses = addAndRegisterBookies(numOfBookies);

        LedgerManagerFactory mFactory = driver.getLedgerManagerFactory();
        LedgerManager lm = mFactory.newLedgerManager();
        DigestType digestType = DigestType.DUMMY;
        byte[] password = new byte[0];
        Collections.shuffle(bookieAddresses);

        int numLedgersFoundHavingLessThanAQReplicasOfAnEntry = 0;

        /*
         * closed ledger
         *
         * for this ledger there is only one copy of entry 2, so this ledger
         * would be counted towards
         * ledgersFoundHavingLessThanAQReplicasOfAnEntry.
         */
        Map<Long, List<BookieId>> segmentEnsembles = new LinkedHashMap<Long, List<BookieId>>();
        int ensembleSize = 4;
        int writeQuorumSize = 3;
        int ackQuorumSize = 2;
        long lastEntryId = 3;
        int length = 10000;
        segmentEnsembles.put(0L, bookieAddresses.subList(0, 4));
        long ledgerId = 1L;
        createClosedLedgerMetadata(lm, ledgerId, ensembleSize, writeQuorumSize, ackQuorumSize, segmentEnsembles,
                lastEntryId, length, digestType, password);
        returnAvailabilityOfEntriesOfLedger.put(bookieAddresses.get(0).toString(), Long.toString(ledgerId),
                new AvailabilityOfEntriesOfLedger(new long[] { 0 }));
        returnAvailabilityOfEntriesOfLedger.put(bookieAddresses.get(1).toString(), Long.toString(ledgerId),
                new AvailabilityOfEntriesOfLedger(new long[] { 0, 1, 3 }));
        returnAvailabilityOfEntriesOfLedger.put(bookieAddresses.get(2).toString(), Long.toString(ledgerId),
                new AvailabilityOfEntriesOfLedger(new long[] { 0, 1 }));
        returnAvailabilityOfEntriesOfLedger.put(bookieAddresses.get(3).toString(), Long.toString(ledgerId),
                new AvailabilityOfEntriesOfLedger(new long[] { 1, 2, 3 }));
        numLedgersFoundHavingLessThanAQReplicasOfAnEntry++;

        /*
         * closed ledger with multiple segments.
         *
         * for this ledger there is only one copy of entry 2, so this ledger
         * would be counted towards
         * ledgersFoundHavingLessThanAQReplicasOfAnEntry.
         *
         */
        segmentEnsembles.clear();
        segmentEnsembles.put(0L, bookieAddresses.subList(0, 4));
        segmentEnsembles.put(2L, bookieAddresses.subList(1, 5));
        ledgerId = 2L;
        createClosedLedgerMetadata(lm, ledgerId, ensembleSize, writeQuorumSize, ackQuorumSize, segmentEnsembles,
                lastEntryId, length, digestType, password);
        returnAvailabilityOfEntriesOfLedger.put(bookieAddresses.get(0).toString(), Long.toString(ledgerId),
                new AvailabilityOfEntriesOfLedger(new long[] {}));
        returnAvailabilityOfEntriesOfLedger.put(bookieAddresses.get(1).toString(), Long.toString(ledgerId),
                new AvailabilityOfEntriesOfLedger(new long[] { 0, 1, 2, 3 }));
        returnAvailabilityOfEntriesOfLedger.put(bookieAddresses.get(2).toString(), Long.toString(ledgerId),
                new AvailabilityOfEntriesOfLedger(new long[] { 0, 1, 3 }));
        returnAvailabilityOfEntriesOfLedger.put(bookieAddresses.get(3).toString(), Long.toString(ledgerId),
                new AvailabilityOfEntriesOfLedger(new long[] { 1 }));
        returnAvailabilityOfEntriesOfLedger.put(bookieAddresses.get(4).toString(), Long.toString(ledgerId),
                new AvailabilityOfEntriesOfLedger(new long[] { 3 }));
        numLedgersFoundHavingLessThanAQReplicasOfAnEntry++;

        /*
         * closed ledger with multiple segments
         *
         * for this ledger entry 2 is overrreplicated, but it has only one copy
         * in the set of bookies it is supposed to be. So it should be counted
         * towards ledgersFoundHavingLessThanAQReplicasOfAnEntry.
         */
        segmentEnsembles.clear();
        segmentEnsembles.put(0L, bookieAddresses.subList(0, 4));
        segmentEnsembles.put(2L, bookieAddresses.subList(1, 5));
        ledgerId = 3L;
        createClosedLedgerMetadata(lm, ledgerId, ensembleSize, writeQuorumSize, ackQuorumSize, segmentEnsembles,
                lastEntryId, length, digestType, password);
        returnAvailabilityOfEntriesOfLedger.put(bookieAddresses.get(0).toString(), Long.toString(ledgerId),
                new AvailabilityOfEntriesOfLedger(new long[] { 2 }));
        returnAvailabilityOfEntriesOfLedger.put(bookieAddresses.get(1).toString(), Long.toString(ledgerId),
                new AvailabilityOfEntriesOfLedger(new long[] { 0, 1, 2, 3 }));
        returnAvailabilityOfEntriesOfLedger.put(bookieAddresses.get(2).toString(), Long.toString(ledgerId),
                new AvailabilityOfEntriesOfLedger(new long[] { 0, 1, 3 }));
        returnAvailabilityOfEntriesOfLedger.put(bookieAddresses.get(3).toString(), Long.toString(ledgerId),
                new AvailabilityOfEntriesOfLedger(new long[] { 1 }));
        returnAvailabilityOfEntriesOfLedger.put(bookieAddresses.get(4).toString(), Long.toString(ledgerId),
                new AvailabilityOfEntriesOfLedger(new long[] { 3 }));
        numLedgersFoundHavingLessThanAQReplicasOfAnEntry++;

        /*
         * non-closed ledger
         *
         * since this is non-closed ledger, it should not be counted towards
         * ledgersFoundHavingLessThanAQReplicasOfAnEntry
         */
        segmentEnsembles.clear();
        segmentEnsembles.put(0L, bookieAddresses.subList(0, 4));
        segmentEnsembles.put(2L, bookieAddresses.subList(1, 5));
        ledgerId = 4L;
        createNonClosedLedgerMetadata(lm, ledgerId, ensembleSize, writeQuorumSize, ackQuorumSize, segmentEnsembles,
                digestType, password);
        returnAvailabilityOfEntriesOfLedger.put(bookieAddresses.get(0).toString(), Long.toString(ledgerId),
                new AvailabilityOfEntriesOfLedger(new long[] {}));
        returnAvailabilityOfEntriesOfLedger.put(bookieAddresses.get(1).toString(), Long.toString(ledgerId),
                new AvailabilityOfEntriesOfLedger(new long[] { 0, 1, 2, 3 }));
        returnAvailabilityOfEntriesOfLedger.put(bookieAddresses.get(2).toString(), Long.toString(ledgerId),
                new AvailabilityOfEntriesOfLedger(new long[] { 0, 1, 3 }));
        returnAvailabilityOfEntriesOfLedger.put(bookieAddresses.get(3).toString(), Long.toString(ledgerId),
                new AvailabilityOfEntriesOfLedger(new long[] { 1 }));
        returnAvailabilityOfEntriesOfLedger.put(bookieAddresses.get(4).toString(), Long.toString(ledgerId),
                new AvailabilityOfEntriesOfLedger(new long[] { 3 }));

        /*
         * this is closed ledger.
         *
         * For third bookie, asyncGetListOfEntriesOfLedger will fail with
         * BookieHandleNotAvailableException, so this should not be counted
         * against missing copies of an entry. Other than that, for both entries
         * 0 and 1, two copies are missing. Hence this should be counted towards
         * numLedgersFoundHavingLessThanAQReplicasOfAnEntry.
         */
        ensembleSize = 3;
        writeQuorumSize = 3;
        ackQuorumSize = 2;
        lastEntryId = 1;
        length = 1000;
        segmentEnsembles.clear();
        segmentEnsembles.put(0L, bookieAddresses.subList(0, 3));
        ledgerId = 5L;
        createClosedLedgerMetadata(lm, ledgerId, ensembleSize, writeQuorumSize, ackQuorumSize, segmentEnsembles,
                lastEntryId, length, digestType, password);
        returnAvailabilityOfEntriesOfLedger.put(bookieAddresses.get(0).toString(), Long.toString(ledgerId),
                AvailabilityOfEntriesOfLedger.EMPTY_AVAILABILITYOFENTRIESOFLEDGER);
        returnAvailabilityOfEntriesOfLedger.put(bookieAddresses.get(1).toString(), Long.toString(ledgerId),
                AvailabilityOfEntriesOfLedger.EMPTY_AVAILABILITYOFENTRIESOFLEDGER);
        errorReturnValueForGetAvailabilityOfEntriesOfLedger.put(bookieAddresses.get(2).toString(),
                    Long.toString(ledgerId), BKException.Code.BookieHandleNotAvailableException);
        numLedgersFoundHavingLessThanAQReplicasOfAnEntry++;

        runTestScenario(returnAvailabilityOfEntriesOfLedger, errorReturnValueForGetAvailabilityOfEntriesOfLedger, 0,
                numLedgersFoundHavingLessThanAQReplicasOfAnEntry, 0);
    }

    /*
     * In this testscenario all the ledgers have an entry with less than WQ
     * number of copies but greater than AQ. So all closed ledgers should be
     * counted towards numLedgersFoundHavingLessThanWQReplicasOfAnEntry.
     */
    @Test
    public void testReplicasCheckForLedgersFoundHavingLessThanWQReplicasOfAnEntry() throws Exception {
        int numOfBookies = 5;
        MultiKeyMap<String, AvailabilityOfEntriesOfLedger> returnAvailabilityOfEntriesOfLedger =
                new MultiKeyMap<String, AvailabilityOfEntriesOfLedger>();
        MultiKeyMap<String, Integer> errorReturnValueForGetAvailabilityOfEntriesOfLedger =
                new MultiKeyMap<String, Integer>();
        List<BookieId> bookieAddresses = addAndRegisterBookies(numOfBookies);

        LedgerManagerFactory mFactory = driver.getLedgerManagerFactory();
        LedgerManager lm = mFactory.newLedgerManager();
        DigestType digestType = DigestType.DUMMY;
        byte[] password = new byte[0];
        Collections.shuffle(bookieAddresses);

        int numLedgersFoundHavingLessThanWQReplicasOfAnEntry = 0;

        /*
         * closed ledger
         *
         * for this ledger a copy of entry 3, so this ledger would be counted
         * towards ledgersFoundHavingLessThanWQReplicasOfAnEntry.
         */
        Map<Long, List<BookieId>> segmentEnsembles = new LinkedHashMap<Long, List<BookieId>>();
        int ensembleSize = 4;
        int writeQuorumSize = 3;
        int ackQuorumSize = 2;
        long lastEntryId = 3;
        int length = 10000;
        segmentEnsembles.put(0L, bookieAddresses.subList(0, 4));
        long ledgerId = 1L;
        createClosedLedgerMetadata(lm, ledgerId, ensembleSize, writeQuorumSize, ackQuorumSize, segmentEnsembles,
                lastEntryId, length, digestType, password);
        returnAvailabilityOfEntriesOfLedger.put(bookieAddresses.get(0).toString(), Long.toString(ledgerId),
                new AvailabilityOfEntriesOfLedger(new long[] { 0, 2 }));
        returnAvailabilityOfEntriesOfLedger.put(bookieAddresses.get(1).toString(), Long.toString(ledgerId),
                new AvailabilityOfEntriesOfLedger(new long[] { 0, 1, 3 }));
        returnAvailabilityOfEntriesOfLedger.put(bookieAddresses.get(2).toString(), Long.toString(ledgerId),
                new AvailabilityOfEntriesOfLedger(new long[] { 0, 1 }));
        returnAvailabilityOfEntriesOfLedger.put(bookieAddresses.get(3).toString(), Long.toString(ledgerId),
                new AvailabilityOfEntriesOfLedger(new long[] { 1, 2, 3 }));
        numLedgersFoundHavingLessThanWQReplicasOfAnEntry++;

        /*
         * closed ledger with multiple segments
         *
         * for this ledger a copy of entry 0 and entry 2 are missing, so this
         * ledger would be counted towards
         * ledgersFoundHavingLessThanWQReplicasOfAnEntry.
         */
        segmentEnsembles.clear();
        segmentEnsembles.put(0L, bookieAddresses.subList(0, 4));
        segmentEnsembles.put(2L, bookieAddresses.subList(1, 5));
        ledgerId = 2L;
        createClosedLedgerMetadata(lm, ledgerId, ensembleSize, writeQuorumSize, ackQuorumSize, segmentEnsembles,
                lastEntryId, length, digestType, password);
        returnAvailabilityOfEntriesOfLedger.put(bookieAddresses.get(0).toString(), Long.toString(ledgerId),
                new AvailabilityOfEntriesOfLedger(new long[] {}));
        returnAvailabilityOfEntriesOfLedger.put(bookieAddresses.get(1).toString(), Long.toString(ledgerId),
                new AvailabilityOfEntriesOfLedger(new long[] { 0, 1, 2, 3 }));
        returnAvailabilityOfEntriesOfLedger.put(bookieAddresses.get(2).toString(), Long.toString(ledgerId),
                new AvailabilityOfEntriesOfLedger(new long[] { 0, 1, 3 }));
        returnAvailabilityOfEntriesOfLedger.put(bookieAddresses.get(3).toString(), Long.toString(ledgerId),
                new AvailabilityOfEntriesOfLedger(new long[] { 1 }));
        returnAvailabilityOfEntriesOfLedger.put(bookieAddresses.get(4).toString(), Long.toString(ledgerId),
                new AvailabilityOfEntriesOfLedger(new long[] { 2, 3 }));
        numLedgersFoundHavingLessThanWQReplicasOfAnEntry++;

        /*
         * non-closed ledger with multiple segments
         *
         * since this is non-closed ledger, it should not be counted towards
         * ledgersFoundHavingLessThanWQReplicasOfAnEntry
         */
        segmentEnsembles.clear();
        segmentEnsembles.put(0L, bookieAddresses.subList(0, 4));
        segmentEnsembles.put(2L, bookieAddresses.subList(1, 5));
        ledgerId = 3L;
        createNonClosedLedgerMetadata(lm, ledgerId, ensembleSize, writeQuorumSize, ackQuorumSize, segmentEnsembles,
                digestType, password);
        errorReturnValueForGetAvailabilityOfEntriesOfLedger.put(bookieAddresses.get(0).toString(),
                Long.toString(ledgerId), BKException.Code.NoSuchLedgerExistsException);
        returnAvailabilityOfEntriesOfLedger.put(bookieAddresses.get(1).toString(), Long.toString(ledgerId),
                new AvailabilityOfEntriesOfLedger(new long[] { 0, 1, 2, 3 }));
        returnAvailabilityOfEntriesOfLedger.put(bookieAddresses.get(2).toString(), Long.toString(ledgerId),
                new AvailabilityOfEntriesOfLedger(new long[] { 0, 1, 3 }));
        returnAvailabilityOfEntriesOfLedger.put(bookieAddresses.get(3).toString(), Long.toString(ledgerId),
                new AvailabilityOfEntriesOfLedger(new long[] { 1 }));
        returnAvailabilityOfEntriesOfLedger.put(bookieAddresses.get(4).toString(), Long.toString(ledgerId),
                new AvailabilityOfEntriesOfLedger(new long[] { 2, 3 }));

        /*
         * closed ledger.
         *
         * for this ledger entry 0 is overrreplicated, but a copy is missing in
         * the set of bookies it is supposed to be. So it should be counted
         * towards ledgersFoundHavingLessThanWQReplicasOfAnEntry.
         */
        ensembleSize = 4;
        writeQuorumSize = 3;
        ackQuorumSize = 2;
        lastEntryId = 1;
        length = 1000;
        segmentEnsembles.clear();
        segmentEnsembles.put(0L, bookieAddresses.subList(0, 4));
        ledgerId = 4L;
        createClosedLedgerMetadata(lm, ledgerId, ensembleSize, writeQuorumSize, ackQuorumSize, segmentEnsembles,
                lastEntryId, length, digestType, password);
        returnAvailabilityOfEntriesOfLedger.put(bookieAddresses.get(0).toString(), Long.toString(ledgerId),
                new AvailabilityOfEntriesOfLedger(new long[] { 0, 1 }));
        returnAvailabilityOfEntriesOfLedger.put(bookieAddresses.get(1).toString(), Long.toString(ledgerId),
                new AvailabilityOfEntriesOfLedger(new long[] { 0, 1, 2, 3 }));
        returnAvailabilityOfEntriesOfLedger.put(bookieAddresses.get(2).toString(), Long.toString(ledgerId),
                new AvailabilityOfEntriesOfLedger(new long[] { 1, 3 }));
        returnAvailabilityOfEntriesOfLedger.put(bookieAddresses.get(3).toString(), Long.toString(ledgerId),
                new AvailabilityOfEntriesOfLedger(new long[] { 0 }));
        numLedgersFoundHavingLessThanWQReplicasOfAnEntry++;

        /*
         * this is closed ledger.
         *
         * For third bookie, asyncGetListOfEntriesOfLedger will fail with
         * BookieHandleNotAvailableException, so this should not be counted
         * against missing copies of an entry. Other than that, for both entries
         * 0 and 1, a copy is missing. Hence this should be counted towards
         * numLedgersFoundHavingLessThanWQReplicasOfAnEntry.
         */
        ensembleSize = 3;
        writeQuorumSize = 3;
        ackQuorumSize = 2;
        lastEntryId = 1;
        length = 1000;
        segmentEnsembles.clear();
        segmentEnsembles.put(0L, bookieAddresses.subList(0, 3));
        ledgerId = 5L;
        createClosedLedgerMetadata(lm, ledgerId, ensembleSize, writeQuorumSize, ackQuorumSize, segmentEnsembles,
                lastEntryId, length, digestType, password);
        returnAvailabilityOfEntriesOfLedger.put(bookieAddresses.get(0).toString(), Long.toString(ledgerId),
                AvailabilityOfEntriesOfLedger.EMPTY_AVAILABILITYOFENTRIESOFLEDGER);
        returnAvailabilityOfEntriesOfLedger.put(bookieAddresses.get(1).toString(), Long.toString(ledgerId),
                new AvailabilityOfEntriesOfLedger(new long[] { 0, 1 }));
        errorReturnValueForGetAvailabilityOfEntriesOfLedger.put(bookieAddresses.get(2).toString(),
                Long.toString(ledgerId), BKException.Code.BookieHandleNotAvailableException);
        numLedgersFoundHavingLessThanWQReplicasOfAnEntry++;

        runTestScenario(returnAvailabilityOfEntriesOfLedger, errorReturnValueForGetAvailabilityOfEntriesOfLedger, 0, 0,
                numLedgersFoundHavingLessThanWQReplicasOfAnEntry);
    }

    /*
     * In this testscenario all the ledgers have empty segments.
     */
    @Test
    public void testReplicasCheckForLedgersWithEmptySegments() throws Exception {
        int numOfBookies = 5;
        MultiKeyMap<String, AvailabilityOfEntriesOfLedger> returnAvailabilityOfEntriesOfLedger =
                new MultiKeyMap<String, AvailabilityOfEntriesOfLedger>();
        MultiKeyMap<String, Integer> errorReturnValueForGetAvailabilityOfEntriesOfLedger =
                new MultiKeyMap<String, Integer>();
        List<BookieId> bookieAddresses = addAndRegisterBookies(numOfBookies);

        LedgerManagerFactory mFactory = driver.getLedgerManagerFactory();
        LedgerManager lm = mFactory.newLedgerManager();
        DigestType digestType = DigestType.DUMMY;
        byte[] password = new byte[0];
        Collections.shuffle(bookieAddresses);

        int numLedgersFoundHavingNoReplicaOfAnEntry = 0;
        int numLedgersFoundHavingLessThanAQReplicasOfAnEntry = 0;
        int numLedgersFoundHavingLessThanWQReplicasOfAnEntry = 0;

        /*
         * closed ledger.
         *
         * This closed Ledger has no entry. So it should not be counted towards
         * numLedgersFoundHavingNoReplicaOfAnEntry/LessThanAQReplicasOfAnEntry
         * /WQReplicasOfAnEntry.
         */
        Map<Long, List<BookieId>> segmentEnsembles = new LinkedHashMap<Long, List<BookieId>>();
        int ensembleSize = 4;
        int writeQuorumSize = 3;
        int ackQuorumSize = 2;
        long lastEntryId = -1L;
        int length = 0;
        segmentEnsembles.put(0L, bookieAddresses.subList(0, 4));
        long ledgerId = 1L;
        createClosedLedgerMetadata(lm, ledgerId, ensembleSize, writeQuorumSize, ackQuorumSize, segmentEnsembles,
                lastEntryId, length, digestType, password);

        /*
         * closed ledger with multiple segments.
         *
         * This ledger has empty last segment, but all the entries have
         * writeQuorumSize number of copies, So it should not be counted towards
         * numLedgersFoundHavingNoReplicaOfAnEntry/LessThanAQReplicasOfAnEntry/
         * WQReplicasOfAnEntry.
         */
        lastEntryId = 2;
        segmentEnsembles.clear();
        segmentEnsembles.put(0L, bookieAddresses.subList(0, 4));
        segmentEnsembles.put((lastEntryId + 1), bookieAddresses.subList(1, 5));
        ledgerId = 2L;
        createClosedLedgerMetadata(lm, ledgerId, ensembleSize, writeQuorumSize, ackQuorumSize, segmentEnsembles,
                lastEntryId, length, digestType, password);
        returnAvailabilityOfEntriesOfLedger.put(bookieAddresses.get(0).toString(), Long.toString(ledgerId),
                new AvailabilityOfEntriesOfLedger(new long[] { 0, 2 }));
        returnAvailabilityOfEntriesOfLedger.put(bookieAddresses.get(1).toString(), Long.toString(ledgerId),
                new AvailabilityOfEntriesOfLedger(new long[] { 0, 1 }));
        returnAvailabilityOfEntriesOfLedger.put(bookieAddresses.get(2).toString(), Long.toString(ledgerId),
                new AvailabilityOfEntriesOfLedger(new long[] { 0, 1, 2 }));
        returnAvailabilityOfEntriesOfLedger.put(bookieAddresses.get(3).toString(), Long.toString(ledgerId),
                new AvailabilityOfEntriesOfLedger(new long[] { 1, 2 }));

        /*
         * Closed ledger with multiple segments.
         *
         * Segment0, Segment1, Segment3, Segment5 and Segment6 are empty.
         * Entries from entryid 3 are missing. So it should be counted towards
         * numLedgersFoundHavingNoReplicaOfAnEntry.
         */
        lastEntryId = 5;
        segmentEnsembles.clear();
        segmentEnsembles.put(0L, bookieAddresses.subList(1, 5));
        segmentEnsembles.put(0L, bookieAddresses.subList(0, 4));
        segmentEnsembles.put(0L, bookieAddresses.subList(0, 4));
        segmentEnsembles.put(4L, bookieAddresses.subList(1, 5));
        segmentEnsembles.put(4L, bookieAddresses.subList(0, 4));
        segmentEnsembles.put((lastEntryId + 1), bookieAddresses.subList(1, 5));
        segmentEnsembles.put((lastEntryId + 1), bookieAddresses.subList(0, 4));
        ledgerId = 3L;
        createClosedLedgerMetadata(lm, ledgerId, ensembleSize, writeQuorumSize, ackQuorumSize, segmentEnsembles,
                lastEntryId, length, digestType, password);
        returnAvailabilityOfEntriesOfLedger.put(bookieAddresses.get(0).toString(), Long.toString(ledgerId),
                new AvailabilityOfEntriesOfLedger(new long[] { 0, 2 }));
        returnAvailabilityOfEntriesOfLedger.put(bookieAddresses.get(1).toString(), Long.toString(ledgerId),
                new AvailabilityOfEntriesOfLedger(new long[] { 0, 1 }));
        returnAvailabilityOfEntriesOfLedger.put(bookieAddresses.get(2).toString(), Long.toString(ledgerId),
                new AvailabilityOfEntriesOfLedger(new long[] { 0, 1, 2 }));
        returnAvailabilityOfEntriesOfLedger.put(bookieAddresses.get(3).toString(), Long.toString(ledgerId),
                new AvailabilityOfEntriesOfLedger(new long[] { 1, 2 }));
        numLedgersFoundHavingNoReplicaOfAnEntry++;

        /*
         * non-closed ledger with multiple segments
         *
         * since this is non-closed ledger, it should not be counted towards
         * ledgersFoundHavingLessThanWQReplicasOfAnEntry
         */
        lastEntryId = 2;
        segmentEnsembles.clear();
        segmentEnsembles.put(0L, bookieAddresses.subList(0, 4));
        segmentEnsembles.put(0L, bookieAddresses.subList(1, 5));
        segmentEnsembles.put((lastEntryId + 1), bookieAddresses.subList(1, 5));
        ledgerId = 4L;
        createNonClosedLedgerMetadata(lm, ledgerId, ensembleSize, writeQuorumSize, ackQuorumSize, segmentEnsembles,
                digestType, password);
        returnAvailabilityOfEntriesOfLedger.put(bookieAddresses.get(0).toString(), Long.toString(ledgerId),
                new AvailabilityOfEntriesOfLedger(new long[] { 0, 2 }));
        returnAvailabilityOfEntriesOfLedger.put(bookieAddresses.get(1).toString(), Long.toString(ledgerId),
                new AvailabilityOfEntriesOfLedger(new long[] { 0, 1 }));
        returnAvailabilityOfEntriesOfLedger.put(bookieAddresses.get(2).toString(), Long.toString(ledgerId),
                new AvailabilityOfEntriesOfLedger(new long[] { 0, 1, 2 }));
        returnAvailabilityOfEntriesOfLedger.put(bookieAddresses.get(3).toString(), Long.toString(ledgerId),
                new AvailabilityOfEntriesOfLedger(new long[] { 1, 2 }));

        runTestScenario(returnAvailabilityOfEntriesOfLedger, errorReturnValueForGetAvailabilityOfEntriesOfLedger,
                numLedgersFoundHavingNoReplicaOfAnEntry, numLedgersFoundHavingLessThanAQReplicasOfAnEntry,
                numLedgersFoundHavingLessThanWQReplicasOfAnEntry);
    }
}
