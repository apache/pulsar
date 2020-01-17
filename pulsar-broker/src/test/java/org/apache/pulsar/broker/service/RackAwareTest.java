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
package org.apache.pulsar.broker.service;

import static org.testng.Assert.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.pulsar.common.policies.data.BookieInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class RackAwareTest extends BrokerBkEnsemblesTests {

    private static final int NUM_BOOKIES = 6;
    private final List<BookieServer> bookies = new ArrayList<>();

    public RackAwareTest() {
        // Start bookies manually
        super(0);
    }

    @BeforeClass
    protected void setup() throws Exception {
        super.setup();

        // Start bookies with specific racks
        for (int i = 0; i < NUM_BOOKIES; i++) {
            File bkDataDir = Files.createTempDirectory("bk" + Integer.toString(i) + "test").toFile();
            ServerConfiguration conf = new ServerConfiguration();

            conf.setBookiePort(0);
            conf.setZkServers("127.0.0.1:" + bkEnsemble.getZookeeperPort());
            conf.setJournalDirName(bkDataDir.getPath());
            conf.setLedgerDirNames(new String[] { bkDataDir.getPath() });
            conf.setAllowLoopback(true);

            // Use different advertised addresses for each bookie, so we can place them in different
            // racks.
            // Eg: 1st bookie will be 10.0.0.1, 2nd 10.0.0.2 and so on
            String addr = String.format("10.0.0.%d", i + 1);
            conf.setAdvertisedAddress(addr);

            BookieServer bs = new BookieServer(conf, NullStatsLogger.INSTANCE);

            bs.start();
            bookies.add(bs);
        }

    }

    @AfterClass
    protected void shutdown() throws Exception {
        super.shutdown();

        for (BookieServer bs : bookies) {
            bs.shutdown();
        }

        bookies.clear();
    }

    @Test
    public void testPlacement() throws Exception {
        for (int i = 0; i < NUM_BOOKIES; i++) {
            String bookie = bookies.get(i).getLocalAddress().toString();

            // Place bookie-1 in "rack-1" and the rest in "rack-2"
            int rackId = i == 0 ? 1 : 2;
            BookieInfo bi = new BookieInfo("rack-" + rackId, "bookie-" + (i + 1));
            log.info("setting rack for bookie at {} -- {}", bookie, bi);
            admin.bookies().updateBookieRackInfo(bookie, "default", bi);
        }

        // Make sure the racks cache gets updated through the ZK watch
        Thread.sleep(1000);

        BookKeeper bkc = this.pulsar.getBookKeeperClient();

        // Create few ledgers and verify all of them should have a copy in the first bookie
        BookieSocketAddress fistBookie = bookies.get(0).getLocalAddress();
        for (int i = 0; i < 100; i++) {
            LedgerHandle lh = bkc.createLedger(2, 2, DigestType.DUMMY, new byte[0]);
            log.info("Ledger: {} -- Ensemble: {}", i, lh.getLedgerMetadata().getEnsembleAt(0));
            assertTrue(lh.getLedgerMetadata().getEnsembleAt(0).contains(fistBookie),
                    "first bookie in rack 0 not included in ensemble");
            lh.close();
        }
    }

    @Test(enabled = false)
    public void testCrashBrokerWithoutCursorLedgerLeak() throws Exception {
        // Ignore test
    }

    @Test(enabled = false)
    public void testSkipCorruptDataLedger() throws Exception {
        // Ignore test
    }

    @Test(enabled = false)
    public void testTopicWithWildCardChar() throws Exception {
        // Ignore test
    }

    private static final Logger log = LoggerFactory.getLogger(RackAwareTest.class);
}
