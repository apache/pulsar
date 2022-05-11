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
package org.apache.pulsar.metadata.bookkeeper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import com.google.protobuf.TextFormat;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.meta.LayoutManager;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.apache.bookkeeper.meta.UnderreplicatedLedger;
import org.apache.bookkeeper.meta.ZkLedgerUnderreplicationManager;
import org.apache.bookkeeper.net.DNS;
import org.apache.bookkeeper.proto.DataFormats.UnderreplicatedLedgerFormat;
import org.apache.bookkeeper.replication.ReplicationException.UnavailableException;
import org.apache.bookkeeper.util.BookKeeperConstants;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.metadata.BaseMetadataStoreTest;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.NotificationType;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * Test the zookeeper implementation of the ledger replication manager.
 */
@Slf4j
public class LedgerUnderreplicationManagerTest extends BaseMetadataStoreTest {

    private Future<Long> getLedgerToReplicate(LedgerUnderreplicationManager m) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                log.info("Starting thread checking for ledgers");
                long l = m.getLedgerToRereplicate();
                log.info("Get ledger id: {}", Long.toHexString(l));
                return l;
            } catch (Exception e) {
                log.error("Error getting ledger id", e);
                return -1L;
            }
        }, executor);
    }

    private MetadataStoreExtended store;
    private LayoutManager layoutManager;
    private LedgerManagerFactory lmf;
    private LedgerUnderreplicationManager lum;

    private String basePath;
    private String urLedgerPath;
    private ExecutorService executor;

    private void methodSetup(Supplier<String> urlSupplier) throws Exception {
        this.executor = Executors.newSingleThreadExecutor();
        String ledgersRoot = "/ledgers-" + UUID.randomUUID();
        this.store = MetadataStoreExtended.create(urlSupplier.get(), MetadataStoreConfig.builder().build());
        this.layoutManager = new PulsarLayoutManager(store, ledgersRoot);
        this.lmf = new PulsarLedgerManagerFactory();

        ClientConfiguration conf = new ClientConfiguration();
        conf.setZkLedgersRootPath(ledgersRoot);
        this.lmf.initialize(conf, layoutManager, 1);
        this.lum = lmf.newLedgerUnderreplicationManager();

        basePath = ledgersRoot + '/'
                + BookKeeperConstants.UNDER_REPLICATION_NODE;
        urLedgerPath = basePath
                + BookKeeperConstants.DEFAULT_ZK_LEDGERS_ROOT_PATH;
    }

    @AfterMethod(alwaysRun = true)
    public final void methodCleanup() throws Exception {
        if (lum != null) {
            lum.close();
        }
        if (lmf != null) {
            lmf.close();
        }
        if (store != null) {
            store.close();
        }
        if (executor != null) {
            try {
                executor.shutdownNow();
                executor.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
            executor = null;
        }
    }

    /**
     * Test basic interactions with the ledger underreplication
     * manager.
     * Mark some ledgers as underreplicated.
     * Ensure that getLedgerToReplicate will block until it a ledger
     * becomes available.
     */
    @Test(dataProvider = "impl")
    public void testBasicInteraction(String provider, Supplier<String> urlSupplier) throws Exception {
        methodSetup(urlSupplier);

        Set<Long> ledgers = new HashSet<>();
        ledgers.add(0xdeadbeefL);
        ledgers.add(0xbeefcafeL);
        ledgers.add(0xffffbeefL);
        ledgers.add(0xfacebeefL);
        String missingReplica = "localhost:3181";

        int count = ledgers.size();
        for (long l : ledgers) {
            lum.markLedgerUnderreplicated(l, missingReplica);
        }

        List<Future<Long>> futures = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            futures.add(getLedgerToReplicate(lum));
        }

        for (Future<Long> f : futures) {
            Long l = f.get(5, TimeUnit.SECONDS);
            assertTrue(ledgers.remove(l));
        }

        Future<Long> f = getLedgerToReplicate(lum);
        try {
            f.get(1, TimeUnit.SECONDS);
            fail("Shouldn't be able to find a ledger to replicate");
        } catch (TimeoutException te) {
            // correct behaviour
        }
        Long newl = 0xfefefefefefeL;
        lum.markLedgerUnderreplicated(newl, missingReplica);
        assertEquals("Should have got the one just added", newl, f.get(5, TimeUnit.SECONDS));
    }

    @Test(dataProvider = "impl")
    public void testGetList(String provider, Supplier<String> urlSupplier) throws Exception {
        methodSetup(urlSupplier);

        Set<Long> ledgers = new HashSet<>();
        ledgers.add(0xdeadbeefL);
        ledgers.add(0xbeefcafeL);
        ledgers.add(0xffffbeefL);
        ledgers.add(0xfacebeefL);
        String missingReplica = "localhost:3181";

        for (long l : ledgers) {
            lum.markLedgerUnderreplicated(l, missingReplica);
        }

        Set<Long> foundLedgers = new HashSet<>();
        for (Iterator<UnderreplicatedLedger> it = lum.listLedgersToRereplicate(null); it.hasNext(); ) {
            UnderreplicatedLedger ul = it.next();
            foundLedgers.add(ul.getLedgerId());
        }

        assertEquals(foundLedgers, ledgers);
    }

    /**
     * Test locking for ledger unreplication manager.
     * If there's only one ledger marked for rereplication,
     * and one client has it, it should be locked; another
     * client shouldn't be able to get it. If the first client dies
     * however, the second client should be able to get it.
     */
    @Test(dataProvider = "impl")
    public void testLocking(String provider, Supplier<String> urlSupplier) throws Exception {
        methodSetup(urlSupplier);

        String missingReplica = "localhost:3181";

        LedgerUnderreplicationManager m1 = lmf.newLedgerUnderreplicationManager();

        @Cleanup
        LedgerUnderreplicationManager m2 = lmf.newLedgerUnderreplicationManager();

        Long ledger = 0xfeadeefdacL;
        m1.markLedgerUnderreplicated(ledger, missingReplica);
        Future<Long> f = getLedgerToReplicate(m1);
        Long l = f.get(5, TimeUnit.SECONDS);
        assertEquals("Should be the ledger I just marked", ledger, l);

        f = getLedgerToReplicate(m2);
        try {
            f.get(1, TimeUnit.SECONDS);
            fail("Shouldn't be able to find a ledger to replicate");
        } catch (TimeoutException te) {
            // correct behaviour
        }

        // Release the lock
        m1.close();

        l = f.get(5, TimeUnit.SECONDS);
        assertEquals("Should be the ledger I marked", ledger, l);
    }


    /**
     * Test that when a ledger has been marked as replicated, it
     * will not be offered to anther client.
     * This test checked that by marking two ledgers, and acquiring
     * them on a single client. It marks one as replicated and then
     * the client is killed. We then check that another client can
     * acquire a ledger, and that it's not the one that was previously
     * marked as replicated.
     */
    @Test(dataProvider = "impl")
    public void testMarkingAsReplicated(String provider, Supplier<String> urlSupplier) throws Exception {
        methodSetup(urlSupplier);

        String missingReplica = "localhost:3181";

        LedgerUnderreplicationManager m1 = lmf.newLedgerUnderreplicationManager();

        @Cleanup
        LedgerUnderreplicationManager m2 = lmf.newLedgerUnderreplicationManager();

        Long ledgerA = 0xfeadeefdacL;
        Long ledgerB = 0xdefadebL;
        m1.markLedgerUnderreplicated(ledgerA, missingReplica);
        m1.markLedgerUnderreplicated(ledgerB, missingReplica);

        Future<Long> fA = getLedgerToReplicate(m1);
        Future<Long> fB = getLedgerToReplicate(m1);

        Long lA = fA.get(5, TimeUnit.SECONDS);
        Long lB = fB.get(5, TimeUnit.SECONDS);

        assertTrue("Should be the ledgers I just marked",
                (lA.equals(ledgerA) && lB.equals(ledgerB))
                        || (lA.equals(ledgerB) && lB.equals(ledgerA)));

        Future<Long> f = getLedgerToReplicate(m2);
        try {
            f.get(1, TimeUnit.SECONDS);
            fail("Shouldn't be able to find a ledger to replicate");
        } catch (TimeoutException te) {
            // correct behaviour
        }
        m1.markLedgerReplicated(lA);

        // Release the locks
        m1.close();

        Long l = f.get(5, TimeUnit.SECONDS);
        assertEquals("Should be the ledger I marked", lB, l);
    }

    /**
     * Test releasing of a ledger
     * A ledger is released when a client decides it does not want
     * to replicate it (or cannot at the moment).
     * When a client releases a previously acquired ledger, another
     * client should then be able to acquire it.
     */
    @Test(dataProvider = "impl")
    public void testRelease(String provider, Supplier<String> urlSupplier) throws Exception {
        methodSetup(urlSupplier);

        String missingReplica = "localhost:3181";

        @Cleanup
        LedgerUnderreplicationManager m1 = lmf.newLedgerUnderreplicationManager();

        @Cleanup
        LedgerUnderreplicationManager m2 = lmf.newLedgerUnderreplicationManager();

        Long ledgerA = 0xfeadeefdacL;
        Long ledgerB = 0xdefadebL;
        m1.markLedgerUnderreplicated(ledgerA, missingReplica);
        m1.markLedgerUnderreplicated(ledgerB, missingReplica);

        Future<Long> fA = getLedgerToReplicate(m1);
        Future<Long> fB = getLedgerToReplicate(m1);

        Long lA = fA.get(5, TimeUnit.SECONDS);
        Long lB = fB.get(5, TimeUnit.SECONDS);

        assertTrue("Should be the ledgers I just marked",
                (lA.equals(ledgerA) && lB.equals(ledgerB))
                        || (lA.equals(ledgerB) && lB.equals(ledgerA)));

        Future<Long> f = getLedgerToReplicate(m2);
        try {
            f.get(1, TimeUnit.SECONDS);
            fail("Shouldn't be able to find a ledger to replicate");
        } catch (TimeoutException te) {
            // correct behaviour
        }
        m1.markLedgerReplicated(lA);
        m1.releaseUnderreplicatedLedger(lB);

        Long l = f.get(5, TimeUnit.SECONDS);
        assertEquals("Should be the ledger I marked", lB, l);
    }

    /**
     * Test that when a failure occurs on a ledger, while the ledger
     * is already being rereplicated, the ledger will still be in the
     * under replicated ledger list when first rereplicating client marks
     * it as replicated.
     */
    @Test(dataProvider = "impl")
    public void testManyFailures(String provider, Supplier<String> urlSupplier) throws Exception {
        methodSetup(urlSupplier);

        String missingReplica1 = "localhost:3181";
        String missingReplica2 = "localhost:3182";

        Long ledgerA = 0xfeadeefdacL;
        lum.markLedgerUnderreplicated(ledgerA, missingReplica1);

        Future<Long> fA = getLedgerToReplicate(lum);
        Long lA = fA.get(5, TimeUnit.SECONDS);

        lum.markLedgerUnderreplicated(ledgerA, missingReplica2);

        assertEquals("Should be the ledger I just marked",
                lA, ledgerA);
        lum.markLedgerReplicated(lA);

        Future<Long> f = getLedgerToReplicate(lum);
        lA = f.get(5, TimeUnit.SECONDS);
        assertEquals("Should be the ledger I had marked previously",
                lA, ledgerA);
    }

    /**
     * If replicationworker has acquired lock on it, then
     * getReplicationWorkerIdRereplicatingLedger should return
     * ReplicationWorkerId (BookieId) of the ReplicationWorker that is holding
     * lock. If lock for the underreplicated ledger is not yet acquired or if it
     * is released then it is supposed to return null.
     *
     * @throws Exception
     */
    @Test(dataProvider = "impl")
    public void testGetReplicationWorkerIdRereplicatingLedger(String provider, Supplier<String> urlSupplier)
            throws Exception {
        methodSetup(urlSupplier);
        String missingReplica1 = "localhost:3181";
        String missingReplica2 = "localhost:3182";

        Long ledgerA = 0xfeadeefdacL;
        lum.markLedgerUnderreplicated(ledgerA, missingReplica1);
        lum.markLedgerUnderreplicated(ledgerA, missingReplica2);

        // lock is not yet acquired so replicationWorkerIdRereplicatingLedger
        // should
        assertEquals("ReplicationWorkerId of the lock", null, lum.getReplicationWorkerIdRereplicatingLedger(ledgerA));

        Future<Long> fA = getLedgerToReplicate(lum);
        Long lA = fA.get(5, TimeUnit.SECONDS);
        assertEquals("Should be the ledger that was just marked", lA, ledgerA);

        /*
         * ZkLedgerUnderreplicationManager.getLockData uses
         * DNS.getDefaultHost("default") as the bookieId.
         *
         */
        assertEquals("ReplicationWorkerId of the lock", DNS.getDefaultHost("default"),
                lum.getReplicationWorkerIdRereplicatingLedger(ledgerA));

        lum.markLedgerReplicated(lA);

        assertEquals("ReplicationWorkerId of the lock", null, lum.getReplicationWorkerIdRereplicatingLedger(ledgerA));
    }

    /**
     * Test that when a ledger is marked as underreplicated with
     * the same missing replica twice, only marking as replicated
     * will be enough to remove it from the list.
     */
    @Test(dataProvider = "impl")
    public void test2reportSame(String provider, Supplier<String> urlSupplier)
            throws Exception {
        methodSetup(urlSupplier);
        String missingReplica1 = "localhost:3181";

        LedgerUnderreplicationManager m1 = lmf.newLedgerUnderreplicationManager();
        LedgerUnderreplicationManager m2 = lmf.newLedgerUnderreplicationManager();

        Long ledgerA = 0xfeadeefdacL;
        m1.markLedgerUnderreplicated(ledgerA, missingReplica1);
        m2.markLedgerUnderreplicated(ledgerA, missingReplica1);

        // verify duplicate missing replica
        UnderreplicatedLedgerFormat.Builder builderA = UnderreplicatedLedgerFormat
                .newBuilder();
        byte[] data = store.get(getUrLedgerZnode(ledgerA)).join().get().getValue();
        TextFormat.merge(new String(data, Charset.forName("UTF-8")), builderA);
        List<String> replicaList = builderA.getReplicaList();
        assertEquals("Published duplicate missing replica : " + replicaList, 1,
                replicaList.size());
        assertTrue("Published duplicate missing replica : " + replicaList,
                replicaList.contains(missingReplica1));

        Future<Long> fA = getLedgerToReplicate(m1);
        Long lA = fA.get(5, TimeUnit.SECONDS);

        assertEquals("Should be the ledger I just marked",
                lA, ledgerA);
        m1.markLedgerReplicated(lA);

        Future<Long> f = getLedgerToReplicate(m2);
        try {
            f.get(1, TimeUnit.SECONDS);
            fail("Shouldn't be able to find a ledger to replicate");
        } catch (TimeoutException te) {
            // correct behaviour
        }
    }

    /**
     * Test that multiple LedgerUnderreplicationManagers should be able to take
     * lock and release for same ledger.
     */
    @Test(dataProvider = "impl")
    public void testMultipleManagersShouldBeAbleToTakeAndReleaseLock(String provider, Supplier<String> urlSupplier)
            throws Exception {
        methodSetup(urlSupplier);
        String missingReplica1 = "localhost:3181";
        final LedgerUnderreplicationManager m1 = lmf
                .newLedgerUnderreplicationManager();
        final LedgerUnderreplicationManager m2 = lmf
                .newLedgerUnderreplicationManager();
        Long ledgerA = 0xfeadeefdacL;
        m1.markLedgerUnderreplicated(ledgerA, missingReplica1);
        final int iterationCount = 100;
        final CountDownLatch latch1 = new CountDownLatch(iterationCount);
        final CountDownLatch latch2 = new CountDownLatch(iterationCount);
        Thread thread1 = new Thread() {
            @Override
            public void run() {
                takeLedgerAndRelease(m1, latch1, iterationCount);
            }
        };

        Thread thread2 = new Thread() {
            @Override
            public void run() {
                takeLedgerAndRelease(m2, latch2, iterationCount);
            }
        };
        thread1.start();
        thread2.start();

        // wait until at least one thread completed
        while (!latch1.await(50, TimeUnit.MILLISECONDS)
                && !latch2.await(50, TimeUnit.MILLISECONDS)) {
            Thread.sleep(50);
        }

        m1.close();
        m2.close();

        // After completing 'lock acquire,release' job, it should notify below
        // wait
        latch1.await();
        latch2.await();
    }

    /**
     * Test verifies failures of bookies which are resembling each other.
     *
     * <p>BK servers named like*********************************************
     * 1.cluster.com, 2.cluster.com, 11.cluster.com, 12.cluster.com
     * *******************************************************************
     *
     * <p>BKserver IP:HOST like*********************************************
     * localhost:3181, localhost:318, localhost:31812
     * *******************************************************************
     */
    @Test(dataProvider = "impl")
    public void testMarkSimilarMissingReplica(String provider, Supplier<String> urlSupplier)
            throws Exception {
        methodSetup(urlSupplier);
        List<String> missingReplica = new ArrayList<String>();
        missingReplica.add("localhost:3181");
        missingReplica.add("localhost:318");
        missingReplica.add("localhost:31812");
        missingReplica.add("1.cluster.com");
        missingReplica.add("2.cluster.com");
        missingReplica.add("11.cluster.com");
        missingReplica.add("12.cluster.com");
        verifyMarkLedgerUnderreplicated(missingReplica);
    }

    /**
     * Test multiple bookie failures for a ledger and marked as underreplicated
     * one after another.
     */
    @Test(dataProvider = "impl")
    public void testManyFailuresInAnEnsemble(String provider, Supplier<String> urlSupplier)
            throws Exception {
        methodSetup(urlSupplier);
        List<String> missingReplica = new ArrayList<String>();
        missingReplica.add("localhost:3181");
        missingReplica.add("localhost:3182");
        verifyMarkLedgerUnderreplicated(missingReplica);
    }

    /**
     * Test disabling the ledger re-replication. After disabling, it will not be
     * able to getLedgerToRereplicate(). This calls will enter into infinite
     * waiting until enabling rereplication process
     */
    @Test(dataProvider = "impl")
    public void testDisableLedgerReplication(String provider, Supplier<String> urlSupplier)
            throws Exception {
        methodSetup(urlSupplier);
        // simulate few urLedgers before disabling
        final Long ledgerA = 0xfeadeefdacL;
        final String missingReplica = "localhost:3181";

        // disabling replication
        lum.disableLedgerReplication();
        log.info("Disabled Ledeger Replication");

        try {
            lum.markLedgerUnderreplicated(ledgerA, missingReplica);
        } catch (UnavailableException e) {
            log.error("Unexpected exception while marking urLedger", e);
            fail("Unexpected exception while marking urLedger" + e.getMessage());
        }

        Future<Long> fA = getLedgerToReplicate(lum);
        try {
            fA.get(1, TimeUnit.SECONDS);
            fail("Shouldn't be able to find a ledger to replicate");
        } catch (TimeoutException te) {
            // expected behaviour, as the replication is disabled
        }
    }

    /**
     * Test enabling the ledger re-replication. After enableLedegerReplication,
     * should continue getLedgerToRereplicate() task
     */
    @Test(dataProvider = "impl")
    public void testEnableLedgerReplication(String provider, Supplier<String> urlSupplier)
            throws Exception {
        methodSetup(urlSupplier);

        // simulate few urLedgers before disabling
        final Long ledgerA = 0xfeadeefdacL;
        final String missingReplica = "localhost:3181";
        try {
            lum.markLedgerUnderreplicated(ledgerA, missingReplica);
        } catch (UnavailableException e) {
            log.debug("Unexpected exception while marking urLedger", e);
            fail("Unexpected exception while marking urLedger" + e.getMessage());
        }

        // disabling replication
        lum.disableLedgerReplication();
        log.debug("Disabled Ledeger Replication");

        String znodeA = getUrLedgerZnode(ledgerA);
        final CountDownLatch znodeLatch = new CountDownLatch(2);
        String urledgerA = StringUtils.substringAfterLast(znodeA, "/");
        String urLockLedgerA = basePath + "/locks/" + urledgerA;
        store.registerListener(n -> {
            if (n.getType() == NotificationType.Created && n.getPath().equals(urLockLedgerA)) {
                znodeLatch.countDown();
                log.debug("Recieved node creation event for the zNodePath:"
                        + n.getPath());
            }
        });

        // getLedgerToRereplicate is waiting until enable rereplication
        Thread thread1 = new Thread() {
            @Override
            public void run() {
                try {
                    Long lA = lum.getLedgerToRereplicate();
                    assertEquals("Should be the ledger I just marked", lA,
                            ledgerA);
                    znodeLatch.countDown();
                } catch (UnavailableException e) {
                    e.printStackTrace();
                }
            }
        };
        thread1.start();

        try {
            assertFalse("shouldn't complete", znodeLatch.await(1, TimeUnit.SECONDS));
            assertEquals("Failed to disable ledger replication!", 2, znodeLatch
                    .getCount());

            lum.enableLedgerReplication();
            znodeLatch.await(5, TimeUnit.SECONDS);
            log.debug("Enabled Ledeger Replication");
            assertEquals("Failed to disable ledger replication!", 0, znodeLatch
                    .getCount());
        } finally {
            thread1.interrupt();
        }
    }

    @Test(dataProvider = "impl")
    public void testCheckAllLedgersCTime(String provider, Supplier<String> urlSupplier)
            throws Exception {
        methodSetup(urlSupplier);
        @Cleanup
        LedgerUnderreplicationManager underReplicaMgr1 = lmf.newLedgerUnderreplicationManager();
        @Cleanup
        LedgerUnderreplicationManager underReplicaMgr2 = lmf.newLedgerUnderreplicationManager();
        assertEquals(-1, underReplicaMgr1.getCheckAllLedgersCTime());
        long curTime = System.currentTimeMillis();
        underReplicaMgr2.setCheckAllLedgersCTime(curTime);
        assertEquals(curTime, underReplicaMgr1.getCheckAllLedgersCTime());
        curTime = System.currentTimeMillis();
        underReplicaMgr2.setCheckAllLedgersCTime(curTime);
        assertEquals(curTime, underReplicaMgr1.getCheckAllLedgersCTime());
    }

    @Test(dataProvider = "impl")
    public void testPlacementPolicyCheckCTime(String provider, Supplier<String> urlSupplier)
            throws Exception {
        methodSetup(urlSupplier);

        @Cleanup
        LedgerUnderreplicationManager underReplicaMgr1 = lmf.newLedgerUnderreplicationManager();
        @Cleanup
        LedgerUnderreplicationManager underReplicaMgr2 = lmf.newLedgerUnderreplicationManager();

        assertEquals(-1, underReplicaMgr1.getPlacementPolicyCheckCTime());
        long curTime = System.currentTimeMillis();
        underReplicaMgr2.setPlacementPolicyCheckCTime(curTime);

        assertEquals(curTime, underReplicaMgr1.getPlacementPolicyCheckCTime());
        curTime = System.currentTimeMillis();
        underReplicaMgr2.setPlacementPolicyCheckCTime(curTime);

        assertEquals(curTime, underReplicaMgr1.getPlacementPolicyCheckCTime());
    }

    @Test(dataProvider = "impl")
    public void testReplicasCheckCTime(String provider, Supplier<String> urlSupplier)
            throws Exception {
        methodSetup(urlSupplier);

        @Cleanup
        LedgerUnderreplicationManager underReplicaMgr1 = lmf.newLedgerUnderreplicationManager();
        @Cleanup
        LedgerUnderreplicationManager underReplicaMgr2 = lmf.newLedgerUnderreplicationManager();
        assertEquals(-1, underReplicaMgr1.getReplicasCheckCTime());
        long curTime = System.currentTimeMillis();
        underReplicaMgr2.setReplicasCheckCTime(curTime);
        assertEquals(curTime, underReplicaMgr1.getReplicasCheckCTime());
        curTime = System.currentTimeMillis();
        underReplicaMgr2.setReplicasCheckCTime(curTime);
        assertEquals(curTime, underReplicaMgr1.getReplicasCheckCTime());
    }

    private void verifyMarkLedgerUnderreplicated(Collection<String> missingReplica) throws Exception {
        Long ledgerA = 0xfeadeefdacL;
        String znodeA = getUrLedgerZnode(ledgerA);
        for (String replica : missingReplica) {
            lum.markLedgerUnderreplicated(ledgerA, replica);
        }

        String urLedgerA = new String(store.get(znodeA).join().get().getValue());
        UnderreplicatedLedgerFormat.Builder builderA = UnderreplicatedLedgerFormat
                .newBuilder();
        for (String replica : missingReplica) {
            builderA.addReplica(replica);
        }
        List<String> replicaList = builderA.getReplicaList();

        for (String replica : missingReplica) {
            assertTrue("UrLedger:" + urLedgerA
                    + " doesn't contain failed bookie :" + replica, replicaList
                    .contains(replica));
        }
    }

    private String getUrLedgerZnode(long ledgerId) {
        return ZkLedgerUnderreplicationManager.getUrLedgerZnode(urLedgerPath, ledgerId);
    }

    private void takeLedgerAndRelease(final LedgerUnderreplicationManager m,
                                      final CountDownLatch latch, int numberOfIterations) {
        for (int i = 0; i < numberOfIterations; i++) {
            try {
                long ledgerToRereplicate = m.getLedgerToRereplicate();
                m.releaseUnderreplicatedLedger(ledgerToRereplicate);
            } catch (UnavailableException e) {
                log.error("UnavailableException when "
                        + "taking or releasing lock", e);
            }
            latch.countDown();
        }
    }
}
