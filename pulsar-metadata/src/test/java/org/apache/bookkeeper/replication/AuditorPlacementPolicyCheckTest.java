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

import static org.apache.bookkeeper.client.RackawareEnsemblePlacementPolicyImpl.REPP_DNS_RESOLVER_CLASS;
import static org.apache.bookkeeper.replication.ReplicationStats.AUDITOR_SCOPE;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.client.LedgerMetadataBuilder;
import org.apache.bookkeeper.client.RackawareEnsemblePlacementPolicy;
import org.apache.bookkeeper.client.ZoneawareEnsemblePlacementPolicy;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.conf.ClientConfiguration;
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
import org.apache.bookkeeper.test.TestStatsProvider;
import org.apache.bookkeeper.test.TestStatsProvider.TestOpStatsLogger;
import org.apache.bookkeeper.test.TestStatsProvider.TestStatsLogger;
import org.apache.bookkeeper.util.StaticDNSResolver;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.zookeeper.KeeperException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests the logic of Auditor's PlacementPolicyCheck.
 */
public class AuditorPlacementPolicyCheckTest extends BookKeeperClusterTestCase {
    private MetadataBookieDriver driver;

    public AuditorPlacementPolicyCheckTest() throws Exception {
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
    }

    @AfterMethod
    @Override
    public void tearDown() throws Exception {
        if (null != driver) {
            driver.close();
        }
        super.tearDown();
    }

    @Test
    public void testPlacementPolicyCheckWithBookiesFromDifferentRacks() throws Exception {
        int numOfBookies = 5;
        List<BookieId> bookieAddresses = new ArrayList<>();
        BookieSocketAddress bookieAddress;
        RegistrationManager regManager = driver.createRegistrationManager();
        // all the numOfBookies (5) are going to be in different racks
        for (int i = 0; i < numOfBookies; i++) {
            bookieAddress = new BookieSocketAddress("98.98.98." + i, 2181);
            StaticDNSResolver.addNodeToRack(bookieAddress.getHostName(), "/rack" + (i));
            bookieAddresses.add(bookieAddress.toBookieId());
            regManager.registerBookie(bookieAddress.toBookieId(), false, BookieServiceInfo.EMPTY);
        }

        LedgerManagerFactory mFactory = driver.getLedgerManagerFactory();
        LedgerManager lm = mFactory.newLedgerManager();
        int ensembleSize = 5;
        int writeQuorumSize = 4;
        int ackQuorumSize = 2;
        int minNumRacksPerWriteQuorumConfValue = 4;
        Collections.shuffle(bookieAddresses);

        // closed ledger
        LedgerMetadata initMeta = LedgerMetadataBuilder.create()
                .withId(1L)
                .withEnsembleSize(ensembleSize)
                .withWriteQuorumSize(writeQuorumSize)
                .withAckQuorumSize(ackQuorumSize)
                .newEnsembleEntry(0L, bookieAddresses)
                .withClosedState()
                .withLastEntryId(100)
                .withLength(10000)
                .withDigestType(DigestType.DUMMY)
                .withPassword(new byte[0])
                .build();
        lm.createLedgerMetadata(1L, initMeta).get();

        Collections.shuffle(bookieAddresses);
        ensembleSize = 4;
        // closed ledger with multiple segments
        initMeta = LedgerMetadataBuilder.create()
                .withId(2L)
                .withEnsembleSize(ensembleSize)
                .withWriteQuorumSize(writeQuorumSize)
                .withAckQuorumSize(ackQuorumSize)
                .newEnsembleEntry(0L, bookieAddresses.subList(0, 4))
                .newEnsembleEntry(20L, bookieAddresses.subList(1, 5))
                .newEnsembleEntry(60L, bookieAddresses.subList(0, 4))
                .withClosedState()
                .withLastEntryId(100)
                .withLength(10000)
                .withDigestType(DigestType.DUMMY)
                .withPassword(new byte[0])
                .build();
        lm.createLedgerMetadata(2L, initMeta).get();

        Collections.shuffle(bookieAddresses);
        // non-closed ledger
        initMeta = LedgerMetadataBuilder.create()
                .withId(3L)
                .withEnsembleSize(ensembleSize)
                .withWriteQuorumSize(writeQuorumSize)
                .withAckQuorumSize(ackQuorumSize)
                .newEnsembleEntry(0L, bookieAddresses.subList(0, 4))
                .withDigestType(DigestType.DUMMY)
                .withPassword(new byte[0])
                .build();
        lm.createLedgerMetadata(3L, initMeta).get();

        Collections.shuffle(bookieAddresses);
        // non-closed ledger with multiple segments
        initMeta = LedgerMetadataBuilder.create()
                .withId(4L)
                .withEnsembleSize(ensembleSize)
                .withWriteQuorumSize(writeQuorumSize)
                .withAckQuorumSize(ackQuorumSize)
                .newEnsembleEntry(0L, bookieAddresses.subList(0, 4))
                .newEnsembleEntry(20L, bookieAddresses.subList(1, 5))
                .newEnsembleEntry(60L, bookieAddresses.subList(0, 4))
                .withDigestType(DigestType.DUMMY)
                .withPassword(new byte[0])
                .build();
        lm.createLedgerMetadata(4L, initMeta).get();

        ServerConfiguration servConf = new ServerConfiguration(confByIndex(0));
        servConf.setMinNumRacksPerWriteQuorum(minNumRacksPerWriteQuorumConfValue);
        setServerConfigPropertiesForRackPlacement(servConf);
        MutableObject<Auditor> auditorRef = new MutableObject<Auditor>();
        try {
            TestStatsLogger statsLogger = startAuditorAndWaitForPlacementPolicyCheck(servConf, auditorRef);
            Gauge<? extends Number> ledgersNotAdheringToPlacementPolicyGuage = statsLogger
                    .getGauge(ReplicationStats.NUM_LEDGERS_NOT_ADHERING_TO_PLACEMENT_POLICY);
            Gauge<? extends Number> ledgersSoftlyAdheringToPlacementPolicyGuage = statsLogger
                    .getGauge(ReplicationStats.NUM_LEDGERS_SOFTLY_ADHERING_TO_PLACEMENT_POLICY);
            /*
             * since all of the bookies are in different racks, there shouldn't be any ledger not adhering
             * to placement policy.
             */
            assertEquals("NUM_LEDGERS_NOT_ADHERING_TO_PLACEMENT_POLICY guage value", 0,
                    ledgersNotAdheringToPlacementPolicyGuage.getSample());
            /*
             * since all of the bookies are in different racks, there shouldn't be any ledger softly adhering
             * to placement policy.
             */
            assertEquals("NUM_LEDGERS_SOFTLY_ADHERING_TO_PLACEMENT_POLICY guage value", 0,
                    ledgersSoftlyAdheringToPlacementPolicyGuage.getSample());
        } finally {
            Auditor auditor = auditorRef.getValue();
            if (auditor != null) {
                auditor.close();
            }
            regManager.close();
        }
    }

    @Test
    public void testPlacementPolicyCheckWithLedgersNotAdheringToPlacementPolicy() throws Exception {
        int numOfBookies = 5;
        int numOfLedgersNotAdheringToPlacementPolicy = 0;
        List<BookieId> bookieAddresses = new ArrayList<>();
        RegistrationManager regManager = driver.createRegistrationManager();
        for (int i = 0; i < numOfBookies; i++) {
            BookieId bookieAddress = new BookieSocketAddress("98.98.98." + i, 2181).toBookieId();
            bookieAddresses.add(bookieAddress);
            regManager.registerBookie(bookieAddress, false, BookieServiceInfo.EMPTY);
        }

        // only three racks
        StaticDNSResolver.addNodeToRack("98.98.98.0", "/rack1");
        StaticDNSResolver.addNodeToRack("98.98.98.1", "/rack2");
        StaticDNSResolver.addNodeToRack("98.98.98.2", "/rack3");
        StaticDNSResolver.addNodeToRack("98.98.98.3", "/rack1");
        StaticDNSResolver.addNodeToRack("98.98.98.4", "/rack2");

        LedgerManagerFactory mFactory = driver.getLedgerManagerFactory();
        LedgerManager lm = mFactory.newLedgerManager();
        int ensembleSize = 5;
        int writeQuorumSize = 3;
        int ackQuorumSize = 2;
        int minNumRacksPerWriteQuorumConfValue = 3;

        /*
         * this closed ledger doesn't adhere to placement policy because there are only
         * 3 racks, and the ensembleSize is 5.
         */
        LedgerMetadata initMeta = LedgerMetadataBuilder.create()
                .withId(1L)
                .withEnsembleSize(ensembleSize)
                .withWriteQuorumSize(writeQuorumSize)
                .withAckQuorumSize(ackQuorumSize)
                .newEnsembleEntry(0L, bookieAddresses)
                .withClosedState()
                .withLastEntryId(100)
                .withLength(10000)
                .withDigestType(DigestType.DUMMY)
                .withPassword(new byte[0])
                .build();
        lm.createLedgerMetadata(1L, initMeta).get();
        numOfLedgersNotAdheringToPlacementPolicy++;

        /*
         * this is non-closed ledger, so it shouldn't count as ledger not
         * adhering to placement policy
         */
        initMeta = LedgerMetadataBuilder.create()
                .withId(2L)
                .withEnsembleSize(ensembleSize)
                .withWriteQuorumSize(writeQuorumSize)
                .withAckQuorumSize(ackQuorumSize)
                .newEnsembleEntry(0L, bookieAddresses)
                .withDigestType(DigestType.DUMMY)
                .withPassword(new byte[0])
                .build();
        lm.createLedgerMetadata(2L, initMeta).get();

        ServerConfiguration servConf = new ServerConfiguration(confByIndex(0));
        servConf.setMinNumRacksPerWriteQuorum(minNumRacksPerWriteQuorumConfValue);
        setServerConfigPropertiesForRackPlacement(servConf);
        MutableObject<Auditor> auditorRef = new MutableObject<Auditor>();
        try {
            TestStatsLogger statsLogger = startAuditorAndWaitForPlacementPolicyCheck(servConf, auditorRef);
            Gauge<? extends Number> ledgersNotAdheringToPlacementPolicyGuage = statsLogger
                    .getGauge(ReplicationStats.NUM_LEDGERS_NOT_ADHERING_TO_PLACEMENT_POLICY);
            assertEquals("NUM_LEDGERS_NOT_ADHERING_TO_PLACEMENT_POLICY guage value",
                    numOfLedgersNotAdheringToPlacementPolicy, ledgersNotAdheringToPlacementPolicyGuage.getSample());
            Gauge<? extends Number> ledgersSoftlyAdheringToPlacementPolicyGuage = statsLogger
                    .getGauge(ReplicationStats.NUM_LEDGERS_SOFTLY_ADHERING_TO_PLACEMENT_POLICY);
            assertEquals("NUM_LEDGERS_SOFTLY_ADHERING_TO_PLACEMENT_POLICY guage value",
                    0, ledgersSoftlyAdheringToPlacementPolicyGuage.getSample());
        } finally {
            Auditor auditor = auditorRef.getValue();
            if (auditor != null) {
                auditor.close();
            }
            regManager.close();
        }
    }

    @Test
    public void testPlacementPolicyCheckWithLedgersNotAdheringToPlacementPolicyAndNotMarkToUnderreplication()
            throws Exception {
        int numOfBookies = 5;
        int numOfLedgersNotAdheringToPlacementPolicy = 0;
        List<BookieId> bookieAddresses = new ArrayList<>();
        RegistrationManager regManager = driver.createRegistrationManager();
        for (int i = 0; i < numOfBookies; i++) {
            BookieId bookieAddress = new BookieSocketAddress("98.98.98." + i, 2181).toBookieId();
            bookieAddresses.add(bookieAddress);
            regManager.registerBookie(bookieAddress, false, BookieServiceInfo.EMPTY);
        }

        // only three racks
        StaticDNSResolver.addNodeToRack("98.98.98.0", "/rack1");
        StaticDNSResolver.addNodeToRack("98.98.98.1", "/rack2");
        StaticDNSResolver.addNodeToRack("98.98.98.2", "/rack3");
        StaticDNSResolver.addNodeToRack("98.98.98.3", "/rack1");
        StaticDNSResolver.addNodeToRack("98.98.98.4", "/rack2");

        LedgerManagerFactory mFactory = driver.getLedgerManagerFactory();
        LedgerManager lm = mFactory.newLedgerManager();
        int ensembleSize = 5;
        int writeQuorumSize = 3;
        int ackQuorumSize = 2;
        int minNumRacksPerWriteQuorumConfValue = 3;

        /*
         * this closed ledger doesn't adhere to placement policy because there are only
         * 3 racks, and the ensembleSize is 5.
         */
        LedgerMetadata initMeta = LedgerMetadataBuilder.create()
                .withId(1L)
                .withEnsembleSize(ensembleSize)
                .withWriteQuorumSize(writeQuorumSize)
                .withAckQuorumSize(ackQuorumSize)
                .newEnsembleEntry(0L, bookieAddresses)
                .withClosedState()
                .withLastEntryId(100)
                .withLength(10000)
                .withDigestType(DigestType.DUMMY)
                .withPassword(new byte[0])
                .build();
        lm.createLedgerMetadata(1L, initMeta).get();
        numOfLedgersNotAdheringToPlacementPolicy++;

        ServerConfiguration servConf = new ServerConfiguration(confByIndex(0));
        servConf.setMinNumRacksPerWriteQuorum(minNumRacksPerWriteQuorumConfValue);
        setServerConfigPropertiesForRackPlacement(servConf);
        MutableObject<Auditor> auditorRef = new MutableObject<Auditor>();
        try {
            TestStatsLogger statsLogger = startAuditorAndWaitForPlacementPolicyCheck(servConf, auditorRef);
            Gauge<? extends Number> ledgersNotAdheringToPlacementPolicyGuage = statsLogger
                    .getGauge(ReplicationStats.NUM_LEDGERS_NOT_ADHERING_TO_PLACEMENT_POLICY);
            assertEquals("NUM_LEDGERS_NOT_ADHERING_TO_PLACEMENT_POLICY guage value",
                    numOfLedgersNotAdheringToPlacementPolicy, ledgersNotAdheringToPlacementPolicyGuage.getSample());
            Gauge<? extends Number> ledgersSoftlyAdheringToPlacementPolicyGuage = statsLogger
                    .getGauge(ReplicationStats.NUM_LEDGERS_SOFTLY_ADHERING_TO_PLACEMENT_POLICY);
            assertEquals("NUM_LEDGERS_SOFTLY_ADHERING_TO_PLACEMENT_POLICY guage value",
                    0, ledgersSoftlyAdheringToPlacementPolicyGuage.getSample());
        } finally {
            Auditor auditor = auditorRef.getValue();
            if (auditor != null) {
                auditor.close();
            }
            regManager.close();
        }
        LedgerUnderreplicationManager underreplicationManager = mFactory.newLedgerUnderreplicationManager();
        long unnderReplicateLedgerId = underreplicationManager.pollLedgerToRereplicate();
        assertEquals(unnderReplicateLedgerId, -1);
    }

    @Test
    public void testPlacementPolicyCheckWithLedgersNotAdheringToPlacementPolicyAndMarkToUnderreplication()
            throws Exception {
        int numOfBookies = 5;
        int numOfLedgersNotAdheringToPlacementPolicy = 0;
        List<BookieId> bookieAddresses = new ArrayList<>();
        RegistrationManager regManager = driver.createRegistrationManager();
        for (int i = 0; i < numOfBookies; i++) {
            BookieId bookieAddress = new BookieSocketAddress("98.98.98." + i, 2181).toBookieId();
            bookieAddresses.add(bookieAddress);
            regManager.registerBookie(bookieAddress, false, BookieServiceInfo.EMPTY);
        }

        // only three racks
        StaticDNSResolver.addNodeToRack("98.98.98.0", "/rack1");
        StaticDNSResolver.addNodeToRack("98.98.98.1", "/rack2");
        StaticDNSResolver.addNodeToRack("98.98.98.2", "/rack3");
        StaticDNSResolver.addNodeToRack("98.98.98.3", "/rack1");
        StaticDNSResolver.addNodeToRack("98.98.98.4", "/rack2");

        LedgerManagerFactory mFactory = driver.getLedgerManagerFactory();
        LedgerManager lm = mFactory.newLedgerManager();
        int ensembleSize = 5;
        int writeQuorumSize = 3;
        int ackQuorumSize = 2;
        int minNumRacksPerWriteQuorumConfValue = 3;

        /*
         * this closed ledger doesn't adhere to placement policy because there are only
         * 3 racks, and the ensembleSize is 5.
         */
        LedgerMetadata initMeta = LedgerMetadataBuilder.create()
                .withId(1L)
                .withEnsembleSize(ensembleSize)
                .withWriteQuorumSize(writeQuorumSize)
                .withAckQuorumSize(ackQuorumSize)
                .newEnsembleEntry(0L, bookieAddresses)
                .withClosedState()
                .withLastEntryId(100)
                .withLength(10000)
                .withDigestType(DigestType.DUMMY)
                .withPassword(new byte[0])
                .build();
        lm.createLedgerMetadata(1L, initMeta).get();
        numOfLedgersNotAdheringToPlacementPolicy++;

        ServerConfiguration servConf = new ServerConfiguration(confByIndex(0));
        servConf.setMinNumRacksPerWriteQuorum(minNumRacksPerWriteQuorumConfValue);
        servConf.setRepairedPlacementPolicyNotAdheringBookieEnable(true);
        setServerConfigPropertiesForRackPlacement(servConf);
        MutableObject<Auditor> auditorRef = new MutableObject<Auditor>();
        try {
            TestStatsLogger statsLogger = startAuditorAndWaitForPlacementPolicyCheck(servConf, auditorRef);
            Gauge<? extends Number> ledgersNotAdheringToPlacementPolicyGuage = statsLogger
                    .getGauge(ReplicationStats.NUM_LEDGERS_NOT_ADHERING_TO_PLACEMENT_POLICY);
            assertEquals("NUM_LEDGERS_NOT_ADHERING_TO_PLACEMENT_POLICY guage value",
                    numOfLedgersNotAdheringToPlacementPolicy, ledgersNotAdheringToPlacementPolicyGuage.getSample());
            Gauge<? extends Number> ledgersSoftlyAdheringToPlacementPolicyGuage = statsLogger
                    .getGauge(ReplicationStats.NUM_LEDGERS_SOFTLY_ADHERING_TO_PLACEMENT_POLICY);
            assertEquals("NUM_LEDGERS_SOFTLY_ADHERING_TO_PLACEMENT_POLICY guage value",
                    0, ledgersSoftlyAdheringToPlacementPolicyGuage.getSample());
        } finally {
            Auditor auditor = auditorRef.getValue();
            if (auditor != null) {
                auditor.close();
            }
            regManager.close();
        }
        LedgerUnderreplicationManager underreplicationManager = mFactory.newLedgerUnderreplicationManager();
        long unnderReplicateLedgerId = underreplicationManager.pollLedgerToRereplicate();
        assertEquals(unnderReplicateLedgerId, 1L);
    }

    @Test
    public void testPlacementPolicyCheckForURLedgersElapsedRecoveryGracePeriod() throws Exception {
        testPlacementPolicyCheckWithURLedgers(true);
    }

    @Test
    public void testPlacementPolicyCheckForURLedgersNotElapsedRecoveryGracePeriod() throws Exception {
        testPlacementPolicyCheckWithURLedgers(false);
    }

    public void testPlacementPolicyCheckWithURLedgers(boolean timeElapsed) throws Exception {
        int numOfBookies = 4;
        /*
         * in timeElapsed=true scenario, set some low value, otherwise set some
         * highValue.
         */
        int underreplicatedLedgerRecoveryGracePeriod = timeElapsed ? 1 : 1000;
        int numOfURLedgersElapsedRecoveryGracePeriod = 0;
        List<BookieId> bookieAddresses = new ArrayList<BookieId>();
        RegistrationManager regManager = driver.createRegistrationManager();
        for (int i = 0; i < numOfBookies; i++) {
            BookieId bookieAddress = new BookieSocketAddress("98.98.98." + i, 2181).toBookieId();
            bookieAddresses.add(bookieAddress);
            regManager.registerBookie(bookieAddress, false, BookieServiceInfo.EMPTY);
        }


        LedgerManagerFactory mFactory = driver.getLedgerManagerFactory();
        LedgerManager lm = mFactory.newLedgerManager();
        LedgerUnderreplicationManager underreplicationManager = mFactory.newLedgerUnderreplicationManager();
        int ensembleSize = 4;
        int writeQuorumSize = 3;
        int ackQuorumSize = 2;

        long ledgerId1 = 1L;
        LedgerMetadata initMeta = LedgerMetadataBuilder.create()
                .withId(ledgerId1)
                .withEnsembleSize(ensembleSize)
                .withWriteQuorumSize(writeQuorumSize)
                .withAckQuorumSize(ackQuorumSize)
                .newEnsembleEntry(0L, bookieAddresses)
                .withClosedState()
                .withLastEntryId(100)
                .withLength(10000)
                .withDigestType(DigestType.DUMMY)
                .withPassword(new byte[0])
                .build();
        lm.createLedgerMetadata(ledgerId1, initMeta).get();
        underreplicationManager.markLedgerUnderreplicated(ledgerId1, bookieAddresses.get(0).toString());
        if (timeElapsed) {
            numOfURLedgersElapsedRecoveryGracePeriod++;
        }

        /*
         * this is non-closed ledger, it should also be reported as
         * URLedgersElapsedRecoveryGracePeriod
         */
        ensembleSize = 3;
        long ledgerId2 = 21234561L;
        initMeta = LedgerMetadataBuilder.create()
                .withId(ledgerId2)
                .withEnsembleSize(ensembleSize)
                .withWriteQuorumSize(writeQuorumSize)
                .withAckQuorumSize(ackQuorumSize)
                .newEnsembleEntry(0L,
                        Arrays.asList(bookieAddresses.get(0), bookieAddresses.get(1), bookieAddresses.get(2)))
                .newEnsembleEntry(100L,
                        Arrays.asList(bookieAddresses.get(3), bookieAddresses.get(1), bookieAddresses.get(2)))
                .withDigestType(DigestType.DUMMY)
                .withPassword(new byte[0])
                .build();
        lm.createLedgerMetadata(ledgerId2, initMeta).get();
        underreplicationManager.markLedgerUnderreplicated(ledgerId2, bookieAddresses.get(0).toString());
        if (timeElapsed) {
            numOfURLedgersElapsedRecoveryGracePeriod++;
        }

        /*
         * this ledger is not marked underreplicated.
         */
        long ledgerId3 = 31234561L;
        initMeta = LedgerMetadataBuilder.create()
                .withId(ledgerId3)
                .withEnsembleSize(ensembleSize)
                .withWriteQuorumSize(writeQuorumSize)
                .withAckQuorumSize(ackQuorumSize)
                .newEnsembleEntry(0L,
                        Arrays.asList(bookieAddresses.get(1), bookieAddresses.get(2), bookieAddresses.get(3)))
                .withClosedState()
                .withLastEntryId(100)
                .withLength(10000)
                .withDigestType(DigestType.DUMMY)
                .withPassword(new byte[0])
                .build();
        lm.createLedgerMetadata(ledgerId3, initMeta).get();

        if (timeElapsed) {
            /*
             * in timeelapsed scenario, by waiting for
             * underreplicatedLedgerRecoveryGracePeriod, recovery time must be
             * elapsed.
             */
            Thread.sleep((underreplicatedLedgerRecoveryGracePeriod + 1) * 1000);
        } else {
            /*
             * in timeElapsed=false scenario, since
             * underreplicatedLedgerRecoveryGracePeriod is set to some high
             * value, there is no value in waiting. So just wait for some time
             * and make sure urledgers are not reported as recoverytime elapsed
             * urledgers.
             */
            Thread.sleep(5000);
        }

        ServerConfiguration servConf = new ServerConfiguration(confByIndex(0));
        servConf.setUnderreplicatedLedgerRecoveryGracePeriod(underreplicatedLedgerRecoveryGracePeriod);
        setServerConfigPropertiesForRackPlacement(servConf);
        MutableObject<Auditor> auditorRef = new MutableObject<Auditor>();
        try {
            TestStatsLogger statsLogger = startAuditorAndWaitForPlacementPolicyCheck(servConf, auditorRef);
            Gauge<? extends Number> underreplicatedLedgersElapsedRecoveryGracePeriodGuage = statsLogger
                    .getGauge(ReplicationStats.NUM_UNDERREPLICATED_LEDGERS_ELAPSED_RECOVERY_GRACE_PERIOD);
            assertEquals("NUM_UNDERREPLICATED_LEDGERS_ELAPSED_RECOVERY_GRACE_PERIOD guage value",
                    numOfURLedgersElapsedRecoveryGracePeriod,
                    underreplicatedLedgersElapsedRecoveryGracePeriodGuage.getSample());
        } finally {
            Auditor auditor = auditorRef.getValue();
            if (auditor != null) {
                auditor.close();
            }
            regManager.close();
        }
    }

    @Test
    public void testPlacementPolicyCheckWithLedgersNotAdheringToPolicyWithMultipleSegments() throws Exception {
        int numOfBookies = 7;
        int numOfLedgersNotAdheringToPlacementPolicy = 0;
        List<BookieId> bookieAddresses = new ArrayList<>();
        RegistrationManager regManager = driver.createRegistrationManager();
        for (int i = 0; i < numOfBookies; i++) {
            BookieId bookieAddress = new BookieSocketAddress("98.98.98." + i, 2181).toBookieId();
            bookieAddresses.add(bookieAddress);
            regManager.registerBookie(bookieAddress, false, BookieServiceInfo.EMPTY);
        }

        // only three racks
        StaticDNSResolver.addNodeToRack("98.98.98.0", "/rack1");
        StaticDNSResolver.addNodeToRack("98.98.98.1", "/rack2");
        StaticDNSResolver.addNodeToRack("98.98.98.2", "/rack3");
        StaticDNSResolver.addNodeToRack("98.98.98.3", "/rack4");
        StaticDNSResolver.addNodeToRack("98.98.98.4", "/rack1");
        StaticDNSResolver.addNodeToRack("98.98.98.5", "/rack2");
        StaticDNSResolver.addNodeToRack("98.98.98.6", "/rack3");

        LedgerManagerFactory mFactory = driver.getLedgerManagerFactory();
        LedgerManager lm = mFactory.newLedgerManager();
        int ensembleSize = 5;
        int writeQuorumSize = 5;
        int ackQuorumSize = 2;
        int minNumRacksPerWriteQuorumConfValue = 4;

        /*
         * this closed ledger in each writeQuorumSize (5), there would be
         * atleast minNumRacksPerWriteQuorumConfValue (4) racks. So it wont be
         * counted as ledgers not adhering to placement policy.
         */
        LedgerMetadata initMeta = LedgerMetadataBuilder.create()
                .withId(1L)
                .withEnsembleSize(ensembleSize)
                .withWriteQuorumSize(writeQuorumSize)
                .withAckQuorumSize(ackQuorumSize)
                .newEnsembleEntry(0L, bookieAddresses.subList(0, 5))
                .newEnsembleEntry(20L, bookieAddresses.subList(1, 6))
                .withClosedState()
                .withLastEntryId(100)
                .withLength(10000)
                .withDigestType(DigestType.DUMMY)
                .withPassword(new byte[0])
                .build();
        lm.createLedgerMetadata(1L, initMeta).get();

        /*
         * for the second segment bookies are from /rack1, /rack2 and /rack3,
         * which is < minNumRacksPerWriteQuorumConfValue (4). So it is not
         * adhering to placement policy.
         *
         * also for the third segment are from /rack1, /rack2 and /rack3, which
         * is < minNumRacksPerWriteQuorumConfValue (4). So it is not adhering to
         * placement policy.
         *
         * Though there are multiple segments are not adhering to placement
         * policy, it should be counted as single ledger.
         */
        initMeta = LedgerMetadataBuilder.create()
                .withId(2L)
                .withEnsembleSize(ensembleSize)
                .withWriteQuorumSize(writeQuorumSize)
                .withAckQuorumSize(ackQuorumSize)
                .newEnsembleEntry(0L, bookieAddresses.subList(0, 5))
                .newEnsembleEntry(20L,
                        Arrays.asList(bookieAddresses.get(0), bookieAddresses.get(1), bookieAddresses.get(2),
                                bookieAddresses.get(4), bookieAddresses.get(5)))
                .newEnsembleEntry(40L,
                        Arrays.asList(bookieAddresses.get(0), bookieAddresses.get(1), bookieAddresses.get(2),
                                bookieAddresses.get(4), bookieAddresses.get(6)))
                .withClosedState()
                .withLastEntryId(100)
                .withLength(10000)
                .withDigestType(DigestType.DUMMY)
                .withPassword(new byte[0])
                .build();
        lm.createLedgerMetadata(2L, initMeta).get();
        numOfLedgersNotAdheringToPlacementPolicy++;

        ServerConfiguration servConf = new ServerConfiguration(confByIndex(0));
        servConf.setMinNumRacksPerWriteQuorum(minNumRacksPerWriteQuorumConfValue);
        setServerConfigPropertiesForRackPlacement(servConf);
        MutableObject<Auditor> auditorRef = new MutableObject<Auditor>();
        try {
            TestStatsLogger statsLogger = startAuditorAndWaitForPlacementPolicyCheck(servConf, auditorRef);
            Gauge<? extends Number> ledgersNotAdheringToPlacementPolicyGuage = statsLogger
                    .getGauge(ReplicationStats.NUM_LEDGERS_NOT_ADHERING_TO_PLACEMENT_POLICY);
            assertEquals("NUM_LEDGERS_NOT_ADHERING_TO_PLACEMENT_POLICY gauge value",
                    numOfLedgersNotAdheringToPlacementPolicy, ledgersNotAdheringToPlacementPolicyGuage.getSample());
            Gauge<? extends Number> ledgersSoftlyAdheringToPlacementPolicyGuage = statsLogger
                    .getGauge(ReplicationStats.NUM_LEDGERS_SOFTLY_ADHERING_TO_PLACEMENT_POLICY);
            assertEquals("NUM_LEDGERS_SOFTLY_ADHERING_TO_PLACEMENT_POLICY gauge value",
                    0, ledgersSoftlyAdheringToPlacementPolicyGuage.getSample());
        } finally {
            Auditor auditor = auditorRef.getValue();
            if (auditor != null) {
                auditor.close();
            }
            regManager.close();
        }
    }

    @Test
    public void testZoneawarePlacementPolicyCheck() throws Exception {
        int numOfBookies = 6;
        int numOfLedgersNotAdheringToPlacementPolicy = 0;
        int numOfLedgersSoftlyAdheringToPlacementPolicy = 0;
        List<BookieId> bookieAddresses = new ArrayList<BookieId>();
        RegistrationManager regManager = driver.createRegistrationManager();
        /*
         * 6 bookies - 3 zones and 2 uds
         */
        for (int i = 0; i < numOfBookies; i++) {
            BookieSocketAddress bookieAddress = new BookieSocketAddress("98.98.98." + i, 2181);
            bookieAddresses.add(bookieAddress.toBookieId());
            regManager.registerBookie(bookieAddress.toBookieId(), false, BookieServiceInfo.EMPTY);
            String zone = "/zone" + (i % 3);
            String upgradeDomain = "/ud" + (i % 2);
            String networkLocation = zone + upgradeDomain;
            StaticDNSResolver.addNodeToRack(bookieAddress.getHostName(), networkLocation);
        }

        LedgerManagerFactory mFactory = driver.getLedgerManagerFactory();
        LedgerManager lm = mFactory.newLedgerManager();

        ServerConfiguration servConf = new ServerConfiguration(confByIndex(0));
        servConf.setDesiredNumZonesPerWriteQuorum(3);
        servConf.setMinNumZonesPerWriteQuorum(2);
        setServerConfigPropertiesForZonePlacement(servConf);

        /*
         * this closed ledger adheres to ZoneAwarePlacementPolicy, since
         * ensemble is spread across 3 zones and 2 UDs
         */
        LedgerMetadata initMeta = LedgerMetadataBuilder.create()
                .withId(1L)
                .withEnsembleSize(6)
                .withWriteQuorumSize(6)
                .withAckQuorumSize(2)
                .newEnsembleEntry(0L, bookieAddresses)
                .withClosedState()
                .withLastEntryId(100)
                .withLength(10000)
                .withDigestType(DigestType.DUMMY)
                .withPassword(new byte[0])
                .build();
        lm.createLedgerMetadata(1L, initMeta).get();

        /*
         * this is non-closed ledger, so though ensemble is not adhering to
         * placement policy (since ensemble is not multiple of writeQuorum),
         * this shouldn't be reported
         */
        initMeta = LedgerMetadataBuilder.create()
                .withId(2L)
                .withEnsembleSize(6)
                .withWriteQuorumSize(5)
                .withAckQuorumSize(2)
                .newEnsembleEntry(0L, bookieAddresses)
                .withDigestType(DigestType.DUMMY)
                .withPassword(new byte[0])
                .build();
        lm.createLedgerMetadata(2L, initMeta).get();

        /*
         * this is closed ledger, since ensemble is not multiple of writeQuorum,
         * this ledger is not adhering to placement policy.
         */
        initMeta = LedgerMetadataBuilder.create()
                .withId(3L)
                .withEnsembleSize(6)
                .withWriteQuorumSize(5)
                .withAckQuorumSize(2)
                .newEnsembleEntry(0L, bookieAddresses)
                .withClosedState()
                .withLastEntryId(100)
                .withLength(10000)
                .withDigestType(DigestType.DUMMY)
                .withPassword(new byte[0])
                .build();
        lm.createLedgerMetadata(3L, initMeta).get();
        numOfLedgersNotAdheringToPlacementPolicy++;

        /*
         * this closed ledger adheres softly to ZoneAwarePlacementPolicy, since
         * ensemble/writeQuorum of size 4 has spread across just
         * minNumZonesPerWriteQuorum (2).
         */
        List<BookieId> newEnsemble = new ArrayList<BookieId>();
        newEnsemble.add(bookieAddresses.get(0));
        newEnsemble.add(bookieAddresses.get(1));
        newEnsemble.add(bookieAddresses.get(3));
        newEnsemble.add(bookieAddresses.get(4));
        initMeta = LedgerMetadataBuilder.create()
                .withId(4L)
                .withEnsembleSize(4)
                .withWriteQuorumSize(4)
                .withAckQuorumSize(2)
                .newEnsembleEntry(0L, newEnsemble)
                .withClosedState()
                .withLastEntryId(100)
                .withLength(10000)
                .withDigestType(DigestType.DUMMY)
                .withPassword(new byte[0])
                .build();
        lm.createLedgerMetadata(4L, initMeta).get();
        numOfLedgersSoftlyAdheringToPlacementPolicy++;

        MutableObject<Auditor> auditorRef = new MutableObject<Auditor>();
        try {
            TestStatsLogger statsLogger = startAuditorAndWaitForPlacementPolicyCheck(servConf, auditorRef);
            Gauge<? extends Number> ledgersNotAdheringToPlacementPolicyGuage = statsLogger
                    .getGauge(ReplicationStats.NUM_LEDGERS_NOT_ADHERING_TO_PLACEMENT_POLICY);
            assertEquals("NUM_LEDGERS_NOT_ADHERING_TO_PLACEMENT_POLICY guage value",
                    numOfLedgersNotAdheringToPlacementPolicy, ledgersNotAdheringToPlacementPolicyGuage.getSample());
            Gauge<? extends Number> ledgersSoftlyAdheringToPlacementPolicyGuage = statsLogger
                    .getGauge(ReplicationStats.NUM_LEDGERS_SOFTLY_ADHERING_TO_PLACEMENT_POLICY);
            assertEquals("NUM_LEDGERS_SOFTLY_ADHERING_TO_PLACEMENT_POLICY guage value",
                    numOfLedgersSoftlyAdheringToPlacementPolicy,
                    ledgersSoftlyAdheringToPlacementPolicyGuage.getSample());
        } finally {
            Auditor auditor = auditorRef.getValue();
            if (auditor != null) {
                auditor.close();
            }
            regManager.close();
        }
    }

    private void setServerConfigPropertiesForRackPlacement(ServerConfiguration servConf) {
        setServerConfigProperties(servConf, RackawareEnsemblePlacementPolicy.class.getName());
    }

    private void setServerConfigPropertiesForZonePlacement(ServerConfiguration servConf) {
        setServerConfigProperties(servConf, ZoneawareEnsemblePlacementPolicy.class.getName());
    }

    private void setServerConfigProperties(ServerConfiguration servConf, String ensemblePlacementPolicyClass) {
        servConf.setProperty(REPP_DNS_RESOLVER_CLASS, StaticDNSResolver.class.getName());
        servConf.setProperty(ClientConfiguration.ENSEMBLE_PLACEMENT_POLICY, ensemblePlacementPolicyClass);
        servConf.setAuditorPeriodicCheckInterval(0);
        servConf.setAuditorPeriodicBookieCheckInterval(0);
        servConf.setAuditorPeriodicReplicasCheckInterval(0);
        servConf.setAuditorPeriodicPlacementPolicyCheckInterval(1000);
    }

    private TestStatsLogger startAuditorAndWaitForPlacementPolicyCheck(ServerConfiguration servConf,
            MutableObject<Auditor> auditorRef) throws MetadataException, CompatibilityException, KeeperException,
            InterruptedException, UnavailableException, UnknownHostException {
        LedgerManagerFactory mFactory = driver.getLedgerManagerFactory();
        LedgerUnderreplicationManager urm = mFactory.newLedgerUnderreplicationManager();
        TestStatsProvider statsProvider = new TestStatsProvider();
        TestStatsLogger statsLogger = statsProvider.getStatsLogger(AUDITOR_SCOPE);
        TestOpStatsLogger placementPolicyCheckStatsLogger = (TestOpStatsLogger) statsLogger
                .getOpStatsLogger(ReplicationStats.PLACEMENT_POLICY_CHECK_TIME);

        final TestAuditor auditor = new TestAuditor(BookieImpl.getBookieId(servConf).toString(), servConf,
                statsLogger, null);
        auditorRef.setValue(auditor);
        CountDownLatch latch = auditor.getLatch();
        assertEquals("PLACEMENT_POLICY_CHECK_TIME SuccessCount", 0, placementPolicyCheckStatsLogger.getSuccessCount());
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
        assertEquals("PLACEMENT_POLICY_CHECK_TIME SuccessCount", 1, placementPolicyCheckStatsLogger.getSuccessCount());
        return statsLogger;
    }
}
