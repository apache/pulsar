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
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNotSame;
import static org.testng.AssertJUnit.assertTrue;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.meta.ZkLedgerUnderreplicationManager;
import org.apache.bookkeeper.meta.zk.ZKMetadataDriverBase;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.replication.ReplicationException.CompatibilityException;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
    private ZkLedgerUnderreplicationManager urLedgerMgr;
    private Set<Long> urLedgerList;
    private String electionPath;

    private List<Long> ledgerList;

    public AuditorLedgerCheckerTest()
            throws IOException, KeeperException, InterruptedException,
            CompatibilityException {
        this("org.apache.bookkeeper.meta.HierarchicalLedgerManagerFactory");
    }

    AuditorLedgerCheckerTest(String ledgerManagerFactoryClass)
            throws IOException, KeeperException, InterruptedException,
            CompatibilityException {
        super(3);
        LOG.info("Running test case using ledger manager : "
                + ledgerManagerFactoryClass);
        this.digestType = DigestType.CRC32;
        // set ledger manager name
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
                + "/underreplication/auditorelection";

        urLedgerMgr = new ZkLedgerUnderreplicationManager(baseClientConf, zkc);
        urLedgerMgr.setCheckAllLedgersCTime(System.currentTimeMillis());
        startAuditorElectors();
        rng = new Random(System.currentTimeMillis()); // Initialize the Random
        urLedgerList = new HashSet<Long>();
        ledgerList = new ArrayList<Long>(2);
        baseClientConf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        baseConf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());
    }

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
        String shutdownBookie = shutDownNonAuditorBookie();

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

    private String shutdownBookie(int bkShutdownIndex) throws Exception {
        BookieServer bkServer = serverByIndex(bkShutdownIndex);
        String bookieAddr = bkServer.getBookieId().toString();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Shutting down bookie:" + bookieAddr);
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

    private String  shutDownNonAuditorBookie() throws Exception {
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
}
