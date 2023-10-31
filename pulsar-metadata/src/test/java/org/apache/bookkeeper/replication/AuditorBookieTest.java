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
import static org.testng.AssertJUnit.assertSame;
import static org.testng.AssertJUnit.assertTrue;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.zk.ZKMetadataDriverBase;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.pulsar.metadata.bookkeeper.PulsarLedgerAuditorManager;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * This test verifies the auditor bookie scenarios which will be monitoring the
 * bookie failures.
 */
public class AuditorBookieTest extends BookKeeperClusterTestCase {
    // Depending on the taste, select the amount of logging
    // by decommenting one of the two lines below
    // private static final Logger LOG = Logger.getRootLogger();
    private static final Logger LOG = LoggerFactory
            .getLogger(AuditorBookieTest.class);
    private String electionPath;
    private HashMap<String, AuditorElector> auditorElectors = new HashMap<String, AuditorElector>();
    private List<ZooKeeper> zkClients = new LinkedList<ZooKeeper>();

    public AuditorBookieTest() throws Exception {
        super(6);
        Class.forName("org.apache.pulsar.metadata.bookkeeper.PulsarMetadataClientDriver");
        Class.forName("org.apache.pulsar.metadata.bookkeeper.PulsarMetadataBookieDriver");

    }

    @BeforeMethod
    @Override
    public void setUp() throws Exception {
        super.setUp();
        electionPath = ZKMetadataDriverBase.resolveZkLedgersRootPath(baseConf)
                + "/underreplication/" + PulsarLedgerAuditorManager.ELECTION_PATH;
        baseConf.setMetadataServiceUri(
                zkUtil.getMetadataServiceUri().replaceAll("zk://", "metadata-store:").replaceAll("/ledgers", ""));
        startAuditorElectors();
    }

    @AfterMethod
    @Override
    public void tearDown() throws Exception {
        stopAuditorElectors();
        for (ZooKeeper zk : zkClients) {
            zk.close();
        }
        zkClients.clear();
        super.tearDown();
    }

    /**
     * Test should ensure only one should act as Auditor. Starting/shutdown
     * other than auditor bookie shouldn't initiate re-election and multiple
     * auditors.
     */
    @Test
    public void testEnsureOnlySingleAuditor() throws Exception {
        BookieServer auditor = verifyAuditor();

        // shutdown bookie which is not an auditor
        int indexOf = indexOfServer(auditor);
        int bkIndexDownBookie;
        if (indexOf < lastBookieIndex()) {
            bkIndexDownBookie = indexOf + 1;
        } else {
            bkIndexDownBookie = indexOf - 1;
        }
        shutdownBookie(serverByIndex(bkIndexDownBookie));

        startNewBookie();
        startNewBookie();
        // grace period for the auditor re-election if any
        BookieServer newAuditor = waitForNewAuditor(auditor);
        assertSame(
                "Auditor re-election is not happened for auditor failure!",
                auditor, newAuditor);
    }

    /**
     * Test Auditor crashes should trigger re-election and another bookie should
     * take over the auditor ship.
     */
    @Test
    public void testSuccessiveAuditorCrashes() throws Exception {
        BookieServer auditor = verifyAuditor();
        shutdownBookie(auditor);

        BookieServer newAuditor1 = waitForNewAuditor(auditor);
        shutdownBookie(newAuditor1);
        BookieServer newAuditor2 = waitForNewAuditor(newAuditor1);
        assertNotSame(
                "Auditor re-election is not happened for auditor failure!",
                auditor, newAuditor2);
    }

    /**
     * Test restarting the entire bookie cluster. It shouldn't create multiple
     * bookie auditors.
     */
    @Test
    public void testBookieClusterRestart() throws Exception {
        BookieServer auditor = verifyAuditor();
        for (AuditorElector auditorElector : auditorElectors.values()) {
            assertTrue("Auditor elector is not running!", auditorElector
                    .isRunning());
        }
        stopBKCluster();
        stopAuditorElectors();

        startBKCluster(zkUtil.getMetadataServiceUri());
        //startBKCluster(zkUtil.getMetadataServiceUri()) override the base conf metadataServiceUri
        baseConf.setMetadataServiceUri(
                zkUtil.getMetadataServiceUri().replaceAll("zk://", "metadata-store:").replaceAll("/ledgers", ""));
        startAuditorElectors();
        BookieServer newAuditor = waitForNewAuditor(auditor);
        assertNotSame(
                "Auditor re-election is not happened for auditor failure!",
                auditor, newAuditor);
    }

    /**
     * Test the vote is deleting from the ZooKeeper during shutdown.
     */
    @Test
    public void testShutdown() throws Exception {
        BookieServer auditor = verifyAuditor();
        shutdownBookie(auditor);

        // waiting for new auditor
        BookieServer newAuditor = waitForNewAuditor(auditor);
        assertNotSame(
                "Auditor re-election is not happened for auditor failure!",
                auditor, newAuditor);

        List<String> children = zkc.getChildren(electionPath, false);
        for (String child : children) {
            byte[] data = zkc.getData(electionPath + '/' + child, false, null);
            String bookieIP = new String(data);
            String addr = auditor.getBookieId().toString();
            assertFalse("AuditorElection cleanup fails", bookieIP
                    .contains(addr));
        }
    }

    /**
     * Test restart of the previous Auditor bookie shouldn't initiate
     * re-election and should create new vote after restarting.
     */
    @Test
    public void testRestartAuditorBookieAfterCrashing() throws Exception {
        BookieServer auditor = verifyAuditor();

        String addr = auditor.getBookieId().toString();

        // restarting Bookie with same configurations.
        ServerConfiguration serverConfiguration = shutdownBookie(auditor);

        auditorElectors.remove(addr);
        startBookie(serverConfiguration);
        // starting corresponding auditor elector

        if (LOG.isDebugEnabled()) {
            LOG.debug("Performing Auditor Election:" + addr);
        }
        startAuditorElector(addr);

        // waiting for new auditor to come
        BookieServer newAuditor = waitForNewAuditor(auditor);
        assertNotSame(
                "Auditor re-election is not happened for auditor failure!",
                auditor, newAuditor);
        assertFalse("No relection after old auditor rejoins", auditor
                .getBookieId().equals(newAuditor.getBookieId()));
    }

    private void startAuditorElector(String addr) throws Exception {
        AuditorElector auditorElector = new AuditorElector(addr,
                baseConf);
        auditorElectors.put(addr, auditorElector);
        auditorElector.start();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Starting Auditor Elector");
        }
    }

    private void startAuditorElectors() throws Exception {
        for (BookieId addr : bookieAddresses()) {
            startAuditorElector(addr.toString());
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

    private BookieServer verifyAuditor() throws Exception {
        List<BookieServer> auditors = getAuditorBookie();
        assertEquals("Multiple Bookies acting as Auditor!", 1, auditors
                .size());
        if (LOG.isDebugEnabled()) {
            LOG.debug("Bookie running as Auditor:" + auditors.get(0));
        }
        return auditors.get(0);
    }

    private List<BookieServer> getAuditorBookie() throws Exception {
        List<BookieServer> auditors = new LinkedList<BookieServer>();
        byte[] data = zkc.getData(electionPath, false, null);
        assertNotNull("Auditor election failed", data);
        for (int i = 0; i < bookieCount(); i++) {
            BookieServer bks = serverByIndex(i);
            if (new String(data).contains(bks.getBookieId() + "")) {
                auditors.add(bks);
            }
        }
        return auditors;
    }

    private ServerConfiguration shutdownBookie(BookieServer bkServer) throws Exception {
        int index = indexOfServer(bkServer);
        String addr = addressByIndex(index).toString();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Shutting down bookie:" + addr);
        }

        // shutdown bookie which is an auditor
        ServerConfiguration conf = killBookie(index);

        // stopping corresponding auditor elector
        auditorElectors.get(addr).shutdown();
        return conf;
    }

    private BookieServer waitForNewAuditor(BookieServer auditor)
            throws Exception {
        BookieServer newAuditor = null;
        int retryCount = 8;
        while (retryCount > 0) {
            try {
                List<BookieServer> auditors = getAuditorBookie();
                if (auditors.size() > 0) {
                    newAuditor = auditors.get(0);
                    if (auditor != newAuditor) {
                        break;
                    }
                }
            } catch (Exception ignore) {
            }

            Thread.sleep(500);
            retryCount--;
        }
        assertNotNull(
                "New Auditor is not reelected after auditor crashes",
                newAuditor);
        verifyAuditor();
        return newAuditor;
    }
}
