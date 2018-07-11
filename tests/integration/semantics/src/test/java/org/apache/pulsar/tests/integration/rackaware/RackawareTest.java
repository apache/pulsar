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
package org.apache.pulsar.tests.integration.rackaware;

import static org.testng.Assert.assertTrue;

import com.google.gson.JsonObject;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;

import org.apache.bookkeeper.client.LedgerMetadata;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.MetadataClientDriver;
import org.apache.bookkeeper.meta.MetadataDrivers;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.policies.data.BookieInfo;
import org.apache.pulsar.tests.topologies.PulsarClusterTestBase;
import org.testng.annotations.Test;

/**
 * Test pulsar rackaware placement integration
 */
@Slf4j
public class RackawareTest extends PulsarClusterTestBase {

    public RackawareTest() {
        NUM_BROKERS = 1;
        NUM_BOOKIES = 6;
    }

    @Test()
    public void testPlacement() throws Exception {
        @Cleanup
        PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(pulsarCluster.getHttpServiceUrl()).build();

        for (int i = 0; i < NUM_BOOKIES; i++) {
            String bookie = "pulsar-bookie-" + i;

            // Place pulsar-bookie-0 in "rack-1" and the rest in "rack-2"
            int rackId = i == 0 ? 1 : 2;
            BookieInfo bi = new BookieInfo("rack-" + rackId, bookie);
            log.info("setting rack for bookie at {} -- {}", bookie, bi);
            admin.bookies().updateBookieRackInfo(bookie + ":3181", "default", bi);
        }

        log.info("Bookies rack map: {}", admin.bookies().getBookiesRackInfo());

        // Give time for broker to get the watch for updated rack infos.
        Thread.sleep(1000);

        // Create many topics. Each topic ledger should have booke-0 in the ensemble, since it's the
        // only bookie in rack-1
        int NUM_TOPICS = 100;

        String baseTopicName = generateTopicName("rackaware", true);
        List<String> topics = new ArrayList<>();
        for (int i = 0; i < NUM_TOPICS; i++) {
            topics.add(baseTopicName + "-" + i);
        }

        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(pulsarCluster.getPlainTextServiceUrl()).build();

        for (String topic : topics) {
            @Cleanup
            Consumer<String> consumer = client.newConsumer(Schema.STRING).topic(topic).subscriptionName("my-sub")
                    .subscribe();
        }

        ClientConfiguration bkConf = new ClientConfiguration();

        @Cleanup
        ZooKeeperClient zk = ZooKeeperClient.newBuilder().connectString(pulsarCluster.getZKConnectionString()).build();

        @Cleanup
        MetadataClientDriver driver = MetadataDrivers
                .getClientDriver(new URI("zk+hierarchical://" + pulsarCluster.getZKConnectionString() + "/ledgers"));

        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        driver.initialize(bkConf, executor, NullStatsLogger.INSTANCE, Optional.of(zk));
        LedgerManager ledgerManager = driver.getLedgerManagerFactory().newLedgerManager();

        for (String topic : topics) {
            JsonObject info = admin.topics().getInternalInfo(topic);
            long ledgerId = info.get("ledgers").getAsJsonArray().get(0).getAsJsonObject().get("ledgerId").getAsLong();
            log.info("Topic {} -- Ledger id: {}", topic, ledgerId);

            CompletableFuture<LedgerMetadata> future = new CompletableFuture<>();
            ledgerManager.readLedgerMetadata(ledgerId, (rc, result) -> {
                future.complete(result);
            });

            LedgerMetadata lm = future.get();
            log.info("Ledger: {} -- Ensemble: {}", ledgerId, lm.getEnsembleAt(0));
            assertTrue(lm.getEnsembleAt(0).contains(new BookieSocketAddress("pulsar-bookie-0", 3181)),
                    "first bookie in rack 0 not included in ensemble");
        }

        executor.shutdown();
    }
}
