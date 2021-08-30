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
package org.apache.pulsar.tests.integration.cli;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.MetadataBookieDriver;
import org.apache.bookkeeper.meta.MetadataDrivers;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.pulsar.PulsarClusterMetadataTeardown;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreFactory;
import org.apache.pulsar.tests.TestRetrySupport;
import org.apache.pulsar.tests.integration.containers.ChaosContainer;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.apache.pulsar.tests.integration.topologies.PulsarClusterSpec;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
public class ClusterMetadataTearDownTest extends TestRetrySupport {

    private final PulsarClusterSpec spec = PulsarClusterSpec.builder()
            .clusterName("ClusterMetadataTearDownTest-" + UUID.randomUUID().toString().substring(0, 8))
            .numProxies(0)
            .numFunctionWorkers(0)
            .enablePrestoWorker(false)
            .build();

    private PulsarCluster pulsarCluster;

    private MetadataStore localMetadataStore;
    private MetadataStore configStore;

    private String metadataServiceUri;
    private MetadataBookieDriver driver;
    private LedgerManager ledgerManager;

    private PulsarClient client;
    private PulsarAdmin admin;

    @Override
    @BeforeClass(alwaysRun = true)
    public final void setup() throws Exception {
        incrementSetupNumber();
        pulsarCluster = PulsarCluster.forSpec(spec);
        pulsarCluster.start();
        metadataServiceUri = "zk+null://" + pulsarCluster.getZKConnString() + "/ledgers";

        localMetadataStore = MetadataStoreFactory.create(pulsarCluster.getZKConnString(),
                MetadataStoreConfig.builder().build());
        configStore = MetadataStoreFactory.create(pulsarCluster.getCSConnString(),
                MetadataStoreConfig.builder().build());

        driver = MetadataDrivers.getBookieDriver(URI.create(metadataServiceUri));
        driver.initialize(new ServerConfiguration().setMetadataServiceUri(metadataServiceUri), () -> {}, NullStatsLogger.INSTANCE);
        ledgerManager = driver.getLedgerManagerFactory().newLedgerManager();

        client = PulsarClient.builder().serviceUrl(pulsarCluster.getPlainTextServiceUrl()).build();
        admin = PulsarAdmin.builder().serviceHttpUrl(pulsarCluster.getHttpServiceUrl()).build();
    }

    @Override
    @AfterClass(alwaysRun = true)
    public final void cleanup() throws PulsarClientException {
        markCurrentSetupNumberCleaned();
        if (client != null) {
            client.close();
        }
        if (admin != null) {
            admin.close();
        }

        try {
            ledgerManager.close();
        } catch (IOException e) {
            log.warn("Failed to close ledger manager: ", e);
        }
        driver.close();
        try {
            configStore.close();
        } catch (Exception ignored) {
        }
        try {
            localMetadataStore.close();
        } catch (Exception ignored) {
        }
        pulsarCluster.stop();
    }

    @Test
    public void testDeleteCluster() throws Exception {
        assertEquals(getNumOfLedgers(), 0);
        final String tenant = "my-tenant";
        final String namespace = tenant + "/my-ns";

        admin.tenants().createTenant(tenant,
                new TenantInfoImpl(new HashSet<>(), Collections.singleton(pulsarCluster.getClusterName())));
        admin.namespaces().createNamespace(namespace);

        String[] topics = { "topic-1", "topic-2", namespace + "/topic-1" };
        for (String topic : topics) {
            try (Producer<String> producer = client.newProducer(Schema.STRING).topic(topic).create()) {
                producer.send("msg");
            }
            String[] subscriptions = { "sub-1", "sub-2" };
            for (String subscription : subscriptions) {
                try (Consumer<String> consumer = client.newConsumer(Schema.STRING)
                        .topic(topic)
                        .subscriptionName(subscription)
                        .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                        .subscribe()) {
                    Message<String> msg = consumer.receive(5, TimeUnit.SECONDS);
                    consumer.acknowledge(msg);
                }
            }
        }

        final String partitionedTopic = namespace + "/par-topic";
        admin.topics().createPartitionedTopic(partitionedTopic, 3);

        // TODO: the schema ledgers of a partitioned topic cannot be deleted completely now,
        //   so we create producers/consumers without schema here
        try (Producer<byte[]> producer = client.newProducer().topic(partitionedTopic).create()) {
            producer.send("msg".getBytes());
            try (Consumer<byte[]> consumer = client.newConsumer()
                    .topic(partitionedTopic)
                    .subscriptionName("my-sub")
                    .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                    .subscribe()) {
                Message<byte[]> msg = consumer.receive(5, TimeUnit.SECONDS);
                consumer.acknowledge(msg);
            }
        }

        pulsarCluster.getBrokers().forEach(ChaosContainer::stop);

        assertTrue(getNumOfLedgers() > 0);
        log.info("Before delete, cluster name: {}, num of ledgers: {}", pulsarCluster.getClusterName(), getNumOfLedgers());

        String[] args = { "-zk", pulsarCluster.getZKConnString(),
                "-cs", pulsarCluster.getCSConnString(),
                "-c", pulsarCluster.getClusterName(),
                "--bookkeeper-metadata-service-uri", metadataServiceUri };
        PulsarClusterMetadataTeardown.main(args);


        // 1. Check Bookie for number of ledgers
        assertEquals(getNumOfLedgers(), 0);

        // 2. Check ZooKeeper for relative nodes
        final int zkOpTimeoutMs = 10000;
        List<String> localNodes = localMetadataStore.getChildren("/").join();
        for (String node : PulsarClusterMetadataTeardown.localZkNodes) {
            assertFalse(localNodes.contains(node));
        }
        List<String> clusterNodes = configStore.getChildren( "/admin/clusters").join();
        assertFalse(clusterNodes.contains(pulsarCluster.getClusterName()));

        // Try delete again, should not fail
        PulsarClusterMetadataTeardown.main(args);
    }

    private long getNumOfLedgers() {
        final AtomicInteger returnCode = new AtomicInteger(BKException.Code.OK);
        final CountDownLatch processDone = new CountDownLatch(1);
        final AtomicLong numOfLedgers = new AtomicLong(0L);

        ledgerManager.asyncProcessLedgers((ledgerId, cb) -> numOfLedgers.incrementAndGet(), (rc, path, ctx) -> {
            returnCode.set(rc);
            processDone.countDown();
        }, null, BKException.Code.OK, BKException.Code.ReadException);

        try {
            processDone.await(5, TimeUnit.SECONDS); // a timeout which is long enough
        } catch (InterruptedException e) {
            fail("asyncProcessLedgers failed", e);
        }
        return numOfLedgers.get();
    }

}
