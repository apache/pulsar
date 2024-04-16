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
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.SortedMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.BookKeeperTestClient;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.apache.bookkeeper.meta.MetadataClientDriver;
import org.apache.bookkeeper.meta.MetadataDrivers;
import org.apache.bookkeeper.meta.ZkLedgerUnderreplicationManager;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Integration tests verifies the complete functionality of the
 * Auditor-rereplication process: Auditor will publish the bookie failures,
 * consequently ReplicationWorker will get the notifications and act on it.
 */
public class BookieAutoRecoveryTest extends BookKeeperClusterTestCase {
    private static final Logger LOG = LoggerFactory
            .getLogger(BookieAutoRecoveryTest.class);
    private static final byte[] PASSWD = "admin".getBytes();
    private static final byte[] data = "TESTDATA".getBytes();
    private static final String openLedgerRereplicationGracePeriod = "3000"; // milliseconds

    private DigestType digestType;
    private MetadataClientDriver metadataClientDriver;
    private LedgerManagerFactory mFactory;
    private LedgerUnderreplicationManager underReplicationManager;
    private LedgerManager ledgerManager;
    private OrderedScheduler scheduler;

    private final String underreplicatedPath = "/ledgers/underreplication/ledgers";

    public BookieAutoRecoveryTest() throws Exception {
        super(3);

        baseConf.setLedgerManagerFactoryClassName(
                "org.apache.pulsar.metadata.bookkeeper.PulsarLedgerManagerFactory");
        baseConf.setOpenLedgerRereplicationGracePeriod(openLedgerRereplicationGracePeriod);
        baseConf.setRwRereplicateBackoffMs(500);
        baseClientConf.setLedgerManagerFactoryClassName(
                "org.apache.pulsar.metadata.bookkeeper.PulsarLedgerManagerFactory");
        this.digestType = DigestType.MAC;
        setAutoRecoveryEnabled(true);
        Class.forName("org.apache.pulsar.metadata.bookkeeper.PulsarMetadataClientDriver");
        Class.forName("org.apache.pulsar.metadata.bookkeeper.PulsarMetadataBookieDriver");
    }

    @BeforeMethod
    @Override
    public void setUp() throws Exception {
        super.setUp();
        baseConf.setMetadataServiceUri(
                zkUtil.getMetadataServiceUri().replaceAll("zk://", "metadata-store:").replaceAll("/ledgers", ""));
        baseClientConf.setMetadataServiceUri(
                zkUtil.getMetadataServiceUri().replaceAll("zk://", "metadata-store:").replaceAll("/ledgers", ""));

        scheduler = OrderedScheduler.newSchedulerBuilder()
                .name("test-scheduler")
                .numThreads(1)
                .build();

        metadataClientDriver = MetadataDrivers.getClientDriver(
                URI.create(baseClientConf.getMetadataServiceUri()));
        metadataClientDriver.initialize(
                baseClientConf,
                scheduler,
                NullStatsLogger.INSTANCE,
                Optional.empty());

        // initialize urReplicationManager
        mFactory = metadataClientDriver.getLedgerManagerFactory();
        underReplicationManager = mFactory.newLedgerUnderreplicationManager();
        ledgerManager = mFactory.newLedgerManager();
    }

    @AfterMethod
    @Override
    public void tearDown() throws Exception {
        super.tearDown();

        if (null != underReplicationManager) {
            underReplicationManager.close();
            underReplicationManager = null;
        }
        if (null != ledgerManager) {
            ledgerManager.close();
            ledgerManager = null;
        }
        if (null != metadataClientDriver) {
            metadataClientDriver.close();
            metadataClientDriver = null;
        }
        if (null != scheduler) {
            scheduler.shutdown();
        }
    }

    /**
     * Test verifies publish urLedger by Auditor and replication worker is
     * picking up the entries and finishing the rereplication of open ledger.
     */
    @Test
    public void testOpenLedgers() throws Exception {
        List<LedgerHandle> listOfLedgerHandle = createLedgersAndAddEntries(1, 5);
        LedgerHandle lh = listOfLedgerHandle.get(0);
        int ledgerReplicaIndex = 0;
        BookieId replicaToKillAddr = lh.getLedgerMetadata().getAllEnsembles().get(0L).get(0);

        final String urLedgerZNode = getUrLedgerZNode(lh);
        ledgerReplicaIndex = getReplicaIndexInLedger(lh, replicaToKillAddr);

        CountDownLatch latch = new CountDownLatch(1);
        assertNull("UrLedger already exists!",
                watchUrLedgerNode(urLedgerZNode, latch));

        LOG.info("Killing Bookie :" + replicaToKillAddr);
        killBookie(replicaToKillAddr);

        // waiting to publish urLedger znode by Auditor
        latch.await();
        latch = new CountDownLatch(1);
        LOG.info("Watching on urLedgerPath:" + urLedgerZNode
                + " to know the status of rereplication process");
        assertNotNull("UrLedger doesn't exists!",
                watchUrLedgerNode(urLedgerZNode, latch));

        // starting the replication service, so that he will be able to act as
        // target bookie
        startNewBookie();
        int newBookieIndex = lastBookieIndex();
        BookieServer newBookieServer = serverByIndex(newBookieIndex);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Waiting to finish the replication of failed bookie : "
                    + replicaToKillAddr);
        }
        latch.await();

        // grace period to update the urledger metadata in zookeeper
        LOG.info("Waiting to update the urledger metadata in zookeeper");

        verifyLedgerEnsembleMetadataAfterReplication(newBookieServer,
                listOfLedgerHandle.get(0), ledgerReplicaIndex);
    }

    /**
     * Test verifies publish urLedger by Auditor and replication worker is
     * picking up the entries and finishing the rereplication of closed ledgers.
     */
    @Test
    public void testClosedLedgers() throws Exception {
        List<Integer> listOfReplicaIndex = new ArrayList<Integer>();
        List<LedgerHandle> listOfLedgerHandle = createLedgersAndAddEntries(1, 5);
        closeLedgers(listOfLedgerHandle);
        LedgerHandle lhandle = listOfLedgerHandle.get(0);
        int ledgerReplicaIndex = 0;
        BookieId replicaToKillAddr = lhandle.getLedgerMetadata().getAllEnsembles().get(0L).get(0);

        CountDownLatch latch = new CountDownLatch(listOfLedgerHandle.size());
        for (LedgerHandle lh : listOfLedgerHandle) {
            ledgerReplicaIndex = getReplicaIndexInLedger(lh, replicaToKillAddr);
            listOfReplicaIndex.add(ledgerReplicaIndex);
            assertNull("UrLedger already exists!",
                    watchUrLedgerNode(getUrLedgerZNode(lh), latch));
        }

        LOG.info("Killing Bookie :" + replicaToKillAddr);
        killBookie(replicaToKillAddr);

        // waiting to publish urLedger znode by Auditor
        latch.await();

        // Again watching the urLedger znode to know the replication status
        latch = new CountDownLatch(listOfLedgerHandle.size());
        for (LedgerHandle lh : listOfLedgerHandle) {
            String urLedgerZNode = getUrLedgerZNode(lh);
            LOG.info("Watching on urLedgerPath:" + urLedgerZNode
                    + " to know the status of rereplication process");
            assertNotNull("UrLedger doesn't exists!",
                    watchUrLedgerNode(urLedgerZNode, latch));
        }

        // starting the replication service, so that he will be able to act as
        // target bookie
        startNewBookie();
        int newBookieIndex = lastBookieIndex();
        BookieServer newBookieServer = serverByIndex(newBookieIndex);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Waiting to finish the replication of failed bookie : "
                    + replicaToKillAddr);
        }

        // waiting to finish replication
        latch.await();

        // grace period to update the urledger metadata in zookeeper
        LOG.info("Waiting to update the urledger metadata in zookeeper");

        for (int index = 0; index < listOfLedgerHandle.size(); index++) {
            verifyLedgerEnsembleMetadataAfterReplication(newBookieServer,
                    listOfLedgerHandle.get(index),
                    listOfReplicaIndex.get(index));
        }
    }

    /**
     * Test stopping replica service while replication in progress. Considering
     * when there is an exception will shutdown Auditor and RW processes. After
     * restarting should be able to finish the re-replication activities
     */
    @Test
    public void testStopWhileReplicationInProgress() throws Exception {
        int numberOfLedgers = 2;
        List<Integer> listOfReplicaIndex = new ArrayList<Integer>();
        List<LedgerHandle> listOfLedgerHandle = createLedgersAndAddEntries(
                numberOfLedgers, 5);
        closeLedgers(listOfLedgerHandle);
        LedgerHandle handle = listOfLedgerHandle.get(0);
        BookieId replicaToKillAddr = handle.getLedgerMetadata().getAllEnsembles().get(0L).get(0);
        LOG.info("Killing Bookie:" + replicaToKillAddr);

        // Each ledger, there will be two events : create urLedger and after
        // rereplication delete urLedger
        CountDownLatch latch = new CountDownLatch(listOfLedgerHandle.size());
        for (int i = 0; i < listOfLedgerHandle.size(); i++) {
            final String urLedgerZNode = getUrLedgerZNode(listOfLedgerHandle
                    .get(i));
            assertNull("UrLedger already exists!",
                    watchUrLedgerNode(urLedgerZNode, latch));
            int replicaIndexInLedger = getReplicaIndexInLedger(
                    listOfLedgerHandle.get(i), replicaToKillAddr);
            listOfReplicaIndex.add(replicaIndexInLedger);
        }

        LOG.info("Killing Bookie :" + replicaToKillAddr);
        killBookie(replicaToKillAddr);

        // waiting to publish urLedger znode by Auditor
        latch.await();

        // Again watching the urLedger znode to know the replication status
        latch = new CountDownLatch(listOfLedgerHandle.size());
        for (LedgerHandle lh : listOfLedgerHandle) {
            String urLedgerZNode = getUrLedgerZNode(lh);
            LOG.info("Watching on urLedgerPath:" + urLedgerZNode
                    + " to know the status of rereplication process");
            assertNotNull("UrLedger doesn't exists!",
                    watchUrLedgerNode(urLedgerZNode, latch));
        }

        // starting the replication service, so that he will be able to act as
        // target bookie
        startNewBookie();
        int newBookieIndex = lastBookieIndex();
        BookieServer newBookieServer = serverByIndex(newBookieIndex);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Waiting to finish the replication of failed bookie : "
                    + replicaToKillAddr);
        }
        while (true) {
            if (latch.getCount() < numberOfLedgers || latch.getCount() <= 0) {
                stopReplicationService();
                LOG.info("Latch Count is:" + latch.getCount());
                break;
            }
            // grace period to take breath
            Thread.sleep(1000);
        }

        startReplicationService();

        LOG.info("Waiting to finish rereplication processes");
        latch.await();

        // grace period to update the urledger metadata in zookeeper
        LOG.info("Waiting to update the urledger metadata in zookeeper");

        for (int index = 0; index < listOfLedgerHandle.size(); index++) {
            verifyLedgerEnsembleMetadataAfterReplication(newBookieServer,
                    listOfLedgerHandle.get(index),
                    listOfReplicaIndex.get(index));
        }
    }

    /**
     * Verify the published urledgers of deleted ledgers(those ledgers where
     * deleted after publishing as urledgers by Auditor) should be cleared off
     * by the newly selected replica bookie.
     */
    @Test
    public void testNoSuchLedgerExists() throws Exception {
        List<LedgerHandle> listOfLedgerHandle = createLedgersAndAddEntries(2, 5);
        CountDownLatch latch = new CountDownLatch(listOfLedgerHandle.size());
        for (LedgerHandle lh : listOfLedgerHandle) {
            assertNull("UrLedger already exists!",
                    watchUrLedgerNode(getUrLedgerZNode(lh), latch));
        }
        BookieId replicaToKillAddr = listOfLedgerHandle.get(0)
                .getLedgerMetadata().getAllEnsembles()
                .get(0L).get(0);
        killBookie(replicaToKillAddr);
        replicaToKillAddr = listOfLedgerHandle.get(0)
                .getLedgerMetadata().getAllEnsembles()
                .get(0L).get(0);
        killBookie(replicaToKillAddr);
        // waiting to publish urLedger znode by Auditor
        latch.await();

        latch = new CountDownLatch(listOfLedgerHandle.size());
        for (LedgerHandle lh : listOfLedgerHandle) {
            assertNotNull("UrLedger doesn't exists!",
                    watchUrLedgerNode(getUrLedgerZNode(lh), latch));
        }

        // delete ledgers
        for (LedgerHandle lh : listOfLedgerHandle) {
            bkc.deleteLedger(lh.getId());
        }
        startNewBookie();

        // waiting to delete published urledgers, since it doesn't exists
        latch.await();

        for (LedgerHandle lh : listOfLedgerHandle) {
            assertNull("UrLedger still exists after rereplication",
                    watchUrLedgerNode(getUrLedgerZNode(lh), latch));
        }
    }

    /**
     * Test that if a empty ledger loses the bookie not in the quorum for entry 0, it will
     * still be openable when it loses enough bookies to lose a whole quorum.
     */
    @Test
    public void testEmptyLedgerLosesQuorumEventually() throws Exception {
        LedgerHandle lh = bkc.createLedger(3, 2, 2, DigestType.CRC32, PASSWD);
        CountDownLatch latch = new CountDownLatch(1);
        String urZNode = getUrLedgerZNode(lh);
        watchUrLedgerNode(urZNode, latch);

        BookieId replicaToKill = lh.getLedgerMetadata().getAllEnsembles().get(0L).get(2);
        LOG.info("Killing last bookie, {}, in ensemble {}", replicaToKill,
                lh.getLedgerMetadata().getAllEnsembles().get(0L));
        killBookie(replicaToKill);
        startNewBookie();

        getAuditor(10, TimeUnit.SECONDS).submitAuditTask().get(); // ensure auditor runs

        assertTrue("Should be marked as underreplicated", latch.await(5, TimeUnit.SECONDS));
        latch = new CountDownLatch(1);
        Stat s = watchUrLedgerNode(urZNode, latch); // should be marked as replicated
        if (s != null) {
            assertTrue("Should be marked as replicated", latch.await(15, TimeUnit.SECONDS));
        }

        replicaToKill = lh.getLedgerMetadata().getAllEnsembles().get(0L).get(1);
        LOG.info("Killing second bookie, {}, in ensemble {}", replicaToKill,
                lh.getLedgerMetadata().getAllEnsembles().get(0L));
        killBookie(replicaToKill);

        getAuditor(10, TimeUnit.SECONDS).submitAuditTask().get(); // ensure auditor runs

        assertTrue("Should be marked as underreplicated", latch.await(5, TimeUnit.SECONDS));
        latch = new CountDownLatch(1);
        s = watchUrLedgerNode(urZNode, latch); // should be marked as replicated

        startNewBookie();
        getAuditor(10, TimeUnit.SECONDS).submitAuditTask().get(); // ensure auditor runs

        if (s != null) {
            assertTrue("Should be marked as replicated", latch.await(20, TimeUnit.SECONDS));
        }

        // should be able to open ledger without issue
        bkc.openLedger(lh.getId(), DigestType.CRC32, PASSWD);
    }

    /**
     * Test verifies bookie recovery, the host (recorded via ipaddress in
     * ledgermetadata).
     */
    @Test
    public void testLedgerMetadataContainsIpAddressAsBookieID()
            throws Exception {
        stopBKCluster();
        bkc = new BookKeeperTestClient(baseClientConf);
        // start bookie with useHostNameAsBookieID=false, as old bookie
        ServerConfiguration serverConf1 = newServerConfiguration();
        // start 2 more bookies with useHostNameAsBookieID=true
        ServerConfiguration serverConf2 = newServerConfiguration();
        serverConf2.setUseHostNameAsBookieID(true);
        ServerConfiguration serverConf3 = newServerConfiguration();
        serverConf3.setUseHostNameAsBookieID(true);
        startAndAddBookie(serverConf1);
        startAndAddBookie(serverConf2);
        startAndAddBookie(serverConf3);

        List<LedgerHandle> listOfLedgerHandle = createLedgersAndAddEntries(1, 5);
        LedgerHandle lh = listOfLedgerHandle.get(0);
        int ledgerReplicaIndex = 0;
        final SortedMap<Long, ? extends List<BookieId>> ensembles = lh.getLedgerMetadata().getAllEnsembles();
        final List<BookieId> bkAddresses = ensembles.get(0L);
        BookieId replicaToKillAddr = bkAddresses.get(0);
        for (BookieId bookieSocketAddress : bkAddresses) {
            if (!isCreatedFromIp(bookieSocketAddress)) {
                replicaToKillAddr = bookieSocketAddress;
                LOG.info("Kill bookie which has registered using hostname");
                break;
            }
        }

        final String urLedgerZNode = getUrLedgerZNode(lh);
        ledgerReplicaIndex = getReplicaIndexInLedger(lh, replicaToKillAddr);

        CountDownLatch latch = new CountDownLatch(1);
        assertNull("UrLedger already exists!",
                watchUrLedgerNode(urLedgerZNode, latch));

        LOG.info("Killing Bookie :" + replicaToKillAddr);
        killBookie(replicaToKillAddr);

        // waiting to publish urLedger znode by Auditor
        latch.await();
        latch = new CountDownLatch(1);
        LOG.info("Watching on urLedgerPath:" + urLedgerZNode
                + " to know the status of rereplication process");
        assertNotNull("UrLedger doesn't exists!",
                watchUrLedgerNode(urLedgerZNode, latch));

        // starting the replication service, so that he will be able to act as
        // target bookie
        ServerConfiguration serverConf = newServerConfiguration();
        serverConf.setUseHostNameAsBookieID(false);
        startAndAddBookie(serverConf);

        int newBookieIndex = lastBookieIndex();
        BookieServer newBookieServer = serverByIndex(newBookieIndex);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Waiting to finish the replication of failed bookie : "
                    + replicaToKillAddr);
        }
        latch.await();

        // grace period to update the urledger metadata in zookeeper
        LOG.info("Waiting to update the urledger metadata in zookeeper");

        verifyLedgerEnsembleMetadataAfterReplication(newBookieServer,
                listOfLedgerHandle.get(0), ledgerReplicaIndex);

    }

    /**
     * Test verifies bookie recovery, the host (recorded via useHostName in
     * ledgermetadata).
     */
    @Test
    public void testLedgerMetadataContainsHostNameAsBookieID()
            throws Exception {
        stopBKCluster();

        bkc = new BookKeeperTestClient(baseClientConf);
        // start bookie with useHostNameAsBookieID=false, as old bookie
        ServerConfiguration serverConf1 = newServerConfiguration();
        // start 2 more bookies with useHostNameAsBookieID=true
        ServerConfiguration serverConf2 = newServerConfiguration();
        serverConf2.setUseHostNameAsBookieID(true);
        ServerConfiguration serverConf3 = newServerConfiguration();
        serverConf3.setUseHostNameAsBookieID(true);
        startAndAddBookie(serverConf1);
        startAndAddBookie(serverConf2);
        startAndAddBookie(serverConf3);

        List<LedgerHandle> listOfLedgerHandle = createLedgersAndAddEntries(1, 5);
        LedgerHandle lh = listOfLedgerHandle.get(0);
        int ledgerReplicaIndex = 0;
        final SortedMap<Long, ? extends List<BookieId>> ensembles = lh.getLedgerMetadata().getAllEnsembles();
        final List<BookieId> bkAddresses = ensembles.get(0L);
        BookieId replicaToKillAddr = bkAddresses.get(0);
        for (BookieId bookieSocketAddress : bkAddresses) {
            if (isCreatedFromIp(bookieSocketAddress)) {
                replicaToKillAddr = bookieSocketAddress;
                LOG.info("Kill bookie which has registered using ipaddress");
                break;
            }
        }

        final String urLedgerZNode = getUrLedgerZNode(lh);
        ledgerReplicaIndex = getReplicaIndexInLedger(lh, replicaToKillAddr);

        CountDownLatch latch = new CountDownLatch(1);
        assertNull("UrLedger already exists!",
                watchUrLedgerNode(urLedgerZNode, latch));

        LOG.info("Killing Bookie :" + replicaToKillAddr);
        killBookie(replicaToKillAddr);

        // waiting to publish urLedger znode by Auditor
        latch.await();
        latch = new CountDownLatch(1);
        LOG.info("Watching on urLedgerPath:" + urLedgerZNode
                + " to know the status of rereplication process");
        assertNotNull("UrLedger doesn't exists!",
                watchUrLedgerNode(urLedgerZNode, latch));

        // creates new bkclient
        bkc = new BookKeeperTestClient(baseClientConf);
        // starting the replication service, so that he will be able to act as
        // target bookie
        ServerConfiguration serverConf = newServerConfiguration();
        serverConf.setUseHostNameAsBookieID(true);
        startAndAddBookie(serverConf);

        int newBookieIndex = lastBookieIndex();
        BookieServer newBookieServer = serverByIndex(newBookieIndex);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Waiting to finish the replication of failed bookie : "
                    + replicaToKillAddr);
        }
        latch.await();

        // grace period to update the urledger metadata in zookeeper
        LOG.info("Waiting to update the urledger metadata in zookeeper");

        verifyLedgerEnsembleMetadataAfterReplication(newBookieServer,
                listOfLedgerHandle.get(0), ledgerReplicaIndex);

    }

    private int getReplicaIndexInLedger(LedgerHandle lh, BookieId replicaToKill) {
        SortedMap<Long, ? extends List<BookieId>> ensembles = lh.getLedgerMetadata().getAllEnsembles();
        int ledgerReplicaIndex = -1;
        for (BookieId addr : ensembles.get(0L)) {
            ++ledgerReplicaIndex;
            if (addr.equals(replicaToKill)) {
                break;
            }
        }
        return ledgerReplicaIndex;
    }

    private void verifyLedgerEnsembleMetadataAfterReplication(
            BookieServer newBookieServer, LedgerHandle lh,
            int ledgerReplicaIndex) throws Exception {
        LedgerHandle openLedger = bkc
                .openLedger(lh.getId(), digestType, PASSWD);

        BookieId inetSocketAddress = openLedger.getLedgerMetadata().getAllEnsembles().get(0L)
                .get(ledgerReplicaIndex);
        assertEquals("Rereplication has been failed and ledgerReplicaIndex :"
                        + ledgerReplicaIndex, newBookieServer.getBookieId(),
                inetSocketAddress);
        openLedger.close();
    }

    private void closeLedgers(List<LedgerHandle> listOfLedgerHandle)
            throws InterruptedException, BKException {
        for (LedgerHandle lh : listOfLedgerHandle) {
            lh.close();
        }
    }

    private List<LedgerHandle> createLedgersAndAddEntries(int numberOfLedgers,
                                                          int numberOfEntries)
            throws InterruptedException, BKException {
        List<LedgerHandle> listOfLedgerHandle = new ArrayList<LedgerHandle>(
                numberOfLedgers);
        for (int index = 0; index < numberOfLedgers; index++) {
            LedgerHandle lh = bkc.createLedger(3, 3, digestType, PASSWD);
            listOfLedgerHandle.add(lh);
            for (int i = 0; i < numberOfEntries; i++) {
                lh.addEntry(data);
            }
        }
        return listOfLedgerHandle;
    }

    private String getUrLedgerZNode(LedgerHandle lh) {
        return ZkLedgerUnderreplicationManager.getUrLedgerZnode(
                underreplicatedPath, lh.getId());
    }

    private Stat watchUrLedgerNode(final String znode,
                                   final CountDownLatch latch) throws KeeperException,
            InterruptedException {
        return zkc.exists(znode, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (event.getType() == EventType.NodeDeleted) {
                    LOG.info("Received Ledger rereplication completion event :"
                            + event.getType());
                    latch.countDown();
                }
                if (event.getType() == EventType.NodeCreated) {
                    LOG.info("Received urLedger publishing event :"
                            + event.getType());
                    latch.countDown();
                }
            }
        });
    }
}
