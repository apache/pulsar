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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.google.gson.Gson;
import java.io.File;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.RackawareEnsemblePlacementPolicy;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.net.NetworkTopologyImpl;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.policies.data.BookieInfo;
import org.apache.pulsar.bookie.rackawareness.BookieRackAffinityMapping;
import org.assertj.core.util.Lists;
import org.awaitility.Awaitility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "quarantine")
public class RackAwareTest extends BkEnsemblesTestBase {

    private static final int NUM_BOOKIES = 6;
    private final List<BookieServer> bookies = new ArrayList<>();

    public RackAwareTest() {
        // Start bookies manually
        super(0);
    }

    @DataProvider(name = "forceMinRackNumProvider")
    public Object[][] forceMinRackNumProvider() {
        return new Object[][] { { Boolean.TRUE }, { Boolean.FALSE } };
    }

    @Override
    protected void configurePulsar(ServiceConfiguration config) throws Exception {
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

            BookieServer bs = new BookieServer(conf, NullStatsLogger.INSTANCE, null);

            bs.start();
            bookies.add(bs);
        }

    }

    @Override
    @AfterMethod(alwaysRun = true)
    protected void cleanup() throws Exception {
        super.cleanup();

        for (BookieServer bs : bookies) {
            bs.shutdown();
        }

        bookies.clear();
    }

    @Test
    public void testPlacement() throws Exception {
        final String group = "default";
        for (int i = 0; i < NUM_BOOKIES; i++) {
            String bookie = bookies.get(i).getLocalAddress().toString();

            // Place bookie-1 in "rack-1" and the rest in "rack-2"
            int rackId = i == 0 ? 1 : 2;
            BookieInfo bi = BookieInfo.builder()
                    .rack("rack-" + rackId)
                    .hostname("bookie-" + (i + 1))
                    .build();
            log.info("setting rack for bookie at {} -- {}", bookie, bi);
            admin.bookies().updateBookieRackInfo(bookie, group, bi);
        }

        // Make sure the racks cache gets updated through the ZK watch
        Awaitility.await().untilAsserted(() -> {
            byte[] data = bkEnsemble.getZkClient()
                    .getData(BookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, false, null);
            TreeMap<String, Map<String, Map<String, String>>> rackInfoMap =
                    new Gson().fromJson(new String(data), TreeMap.class);
            assertEquals(rackInfoMap.get(group).size(), NUM_BOOKIES);
            Set<String> racks = rackInfoMap.values().stream()
                    .map(Map::values)
                    .flatMap(bookieId -> bookieId.stream().map(rackInfo -> rackInfo.get("rack")))
                    .collect(Collectors.toSet());
            assertTrue(racks.containsAll(Lists.newArrayList("rack-1", "rack-2")));
        });

        BookKeeper bkc = this.pulsar.getBookKeeperClient();

        // Create few ledgers and verify all of them should have a copy in the first bookie
        BookieId firstBookie = bookies.get(0).getBookieId();
        for (int i = 0; i < 100; i++) {
            LedgerHandle lh = bkc.createLedger(2, 2, DigestType.DUMMY, new byte[0]);
            log.info("Ledger: {} -- Ensemble: {}", i, lh.getLedgerMetadata().getEnsembleAt(0));
            assertTrue(lh.getLedgerMetadata().getEnsembleAt(0).contains(firstBookie),
                    "first bookie in rack 0 not included in ensemble");
            lh.close();
        }
    }

    @Test(dataProvider="forceMinRackNumProvider")
    public void testPlacementMinRackNumsPerWriteQuorum(boolean forceMinRackNums) throws Exception {
        cleanup();
        config = new ServiceConfiguration();
        config.setBookkeeperClientMinNumRacksPerWriteQuorum(2);
        config.setBookkeeperClientEnforceMinNumRacksPerWriteQuorum(forceMinRackNums);
        setup();
        final String group = "default";
        for (int i = 0; i < NUM_BOOKIES; i++) {
            String bookie = bookies.get(i).getLocalAddress().toString();
            // All bookie in one same rack "rack-1"
            int rackId = i == 0 ? 1 : 2;
            BookieInfo bi = BookieInfo.builder()
                    .rack("rack-" + 1)
                    .hostname("bookie-" + (i + 1))
                    .build();
            log.info("setting rack for bookie at {} -- {}", bookie, bi);
            admin.bookies().updateBookieRackInfo(bookie, group, bi);
        }

        // Make sure the racks cache gets updated through the ZK watch
        Awaitility.await().untilAsserted(() -> {
            byte[] data = bkEnsemble.getZkClient()
                    .getData(BookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, false, null);
            TreeMap<String, Map<String, Map<String, String>>> rackInfoMap =
                    new Gson().fromJson(new String(data), TreeMap.class);
            assertEquals(rackInfoMap.get(group).size(), NUM_BOOKIES);

            Set<String> racks = rackInfoMap.values().stream()
                    .map(Map::values)
                    .flatMap(bookieId -> bookieId.stream().map(rackInfo -> rackInfo.get("rack")))
                    .collect(Collectors.toSet());
            assertEquals(racks.size(), 1);
            assertTrue(racks.contains("rack-1"));
        });

        BookKeeper bkc = this.pulsar.getBookKeeperClient();

        if (forceMinRackNums) {
            try {
                bkc.createLedger(2, 2, DigestType.DUMMY, new byte[0]);
                fail("Should be failed due to no enough rack can be found");
            } catch (BKException.BKNotEnoughBookiesException e) {
                // ignore
            }
        } else {
            for (int i = 0; i < 10; i++) {
                LedgerHandle lh = bkc.createLedger(2, 2, DigestType.DUMMY, new byte[0]);
                log.info("Ledger: {} -- Ensemble: {}", i, lh.getLedgerMetadata().getEnsembleAt(0));
                lh.close();
            }
        }
    }

    public void testRackUpdate() throws Exception {
        // 1. reset configurations for rack-aware
        cleanup();
        config = new ServiceConfiguration();
        config.setBookkeeperClientMinNumRacksPerWriteQuorum(2);
        config.setBookkeeperClientEnforceMinNumRacksPerWriteQuorum(true);
        setup();

        // 2. test create ledger(ensemble size = 2) with only one rack
        //   rack-0     bookie-1
        //   rack-0     bookie-2
        //   rack-0     bookie-3

        final String group = "default";
        for (int i = 0; i < NUM_BOOKIES / 2; i++) {
            String bookie = bookies.get(i).getLocalAddress().toString();
            BookieInfo bi = BookieInfo.builder()
                    .rack("rack-0")
                    .hostname("bookie-" + (i + 1))
                    .build();
            log.info("setting rack for bookie at {} -- {}", bookie, bi);
            admin.bookies().updateBookieRackInfo(bookie, group, bi);
        }

        Awaitility.await().untilAsserted(() -> {
            byte[] data = bkEnsemble.getZkClient()
                    .getData(BookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, false, null);
            TreeMap<String, Map<String, Map<String, String>>> rackInfoMap =
                    new Gson().fromJson(new String(data), TreeMap.class);
            assertEquals(rackInfoMap.get(group).size(), NUM_BOOKIES / 2);

            Set<String> racks = rackInfoMap.values().stream()
                    .map(Map::values)
                    .flatMap(bookieId -> bookieId.stream().map(rackInfo -> rackInfo.get("rack")))
                    .collect(Collectors.toSet());
            assertEquals(racks.size(), 1);
            assertTrue(racks.contains("rack-0"));
        });

        BookKeeper bkc = this.pulsar.getBookKeeperClient();
        Field field = bkc.getClass().getDeclaredField("placementPolicy");
        field.setAccessible(true);
        RackawareEnsemblePlacementPolicy ensemblePlacementPolicy = (RackawareEnsemblePlacementPolicy) field.get(bkc);
        Field topoField =
                ensemblePlacementPolicy.getClass().getSuperclass().getSuperclass().getDeclaredField("topology");
        topoField.setAccessible(true);
        NetworkTopologyImpl networkTopology = (NetworkTopologyImpl) topoField.get(ensemblePlacementPolicy);

        // 3. test create ledger
        try {
            bkc.createLedger(2, 2, DigestType.DUMMY, new byte[0]);
            fail("Should be failed due to no enough rack can be found");
        } catch (BKException.BKNotEnoughBookiesException e) {
            // ignore
        }

        // 4. add another rack, rack-1
        //   rack-1     bookie-4
        //   rack-1     bookie-5
        //   rack-1     bookie-6
        for (int i = NUM_BOOKIES / 2; i < NUM_BOOKIES; i++) {
            String bookie = bookies.get(i).getLocalAddress().toString();
            BookieInfo bi = BookieInfo.builder()
                    .rack("rack-1")
                    .hostname("bookie-" + (i + 1))
                    .build();
            log.info("setting rack for bookie at {} -- {}", bookie, bi);
            admin.bookies().updateBookieRackInfo(bookie, group, bi);
        }

        Awaitility.await().untilAsserted(() -> {
            byte[] data = bkEnsemble.getZkClient()
                    .getData(BookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, false, null);
            TreeMap<String, Map<String, Map<String, String>>> rackInfoMap =
                    new Gson().fromJson(new String(data), TreeMap.class);
            assertEquals(rackInfoMap.get(group).size(), NUM_BOOKIES);

            Set<String> racks = rackInfoMap.values().stream()
                    .map(Map::values)
                    .flatMap(bookieId -> bookieId.stream().map(rackInfo -> rackInfo.get("rack")))
                    .collect(Collectors.toSet());
            assertEquals(racks.size(), 2);
            assertTrue(racks.containsAll(Lists.newArrayList("rack-0", "rack-1")));
        });

        Awaitility.await().untilAsserted(() -> {
            assertEquals(networkTopology.getNumOfRacks(), 2);
        });

        // 5. create ledger required for 2 racks
        for (int i = 0; i < 2; i++) {
            LedgerHandle lh = bkc.createLedger(2, 2, DigestType.DUMMY, new byte[0]);
            log.info("Ledger: {} -- Ensemble: {}", i, lh.getLedgerMetadata().getEnsembleAt(0));
            lh.close();
        }

        // 6. remove rack-0
        for (int i = 0; i < NUM_BOOKIES / 2; i++) {
            String bookie = bookies.get(i).getLocalAddress().toString();
            admin.bookies().deleteBookieRackInfo(bookie);
        }

        Awaitility.await().untilAsserted(() -> {
            byte[] data = bkEnsemble.getZkClient()
                    .getData(BookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, false, null);
            TreeMap<String, Map<String, Map<String, String>>> rackInfoMap =
                    new Gson().fromJson(new String(data), TreeMap.class);
            assertEquals(rackInfoMap.get(group).size(), NUM_BOOKIES / 2);

            Set<String> racks = rackInfoMap.values().stream()
                    .map(Map::values)
                    .flatMap(bookieId -> bookieId.stream().map(rackInfo -> rackInfo.get("rack")))
                    .collect(Collectors.toSet());
            assertEquals(racks.size(), 1);
            assertTrue(racks.contains("rack-1"));
        });

        // 7. test create ledger
        try {
            bkc.createLedger(2, 2, DigestType.DUMMY, new byte[0]);
            fail("Should be failed due to no enough rack can be found");
        } catch (BKException.BKNotEnoughBookiesException e) {
            // ignore
        }

    }

    private static final Logger log = LoggerFactory.getLogger(RackAwareTest.class);
}
