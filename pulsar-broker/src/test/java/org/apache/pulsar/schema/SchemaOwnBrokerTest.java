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
package org.apache.pulsar.schema;

import static org.apache.pulsar.common.naming.TopicName.PUBLIC_TENANT;
import static org.apache.pulsar.schema.compatibility.SchemaCompatibilityCheckTest.randomName;
import static org.testng.Assert.assertEquals;
import com.google.common.collect.Sets;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.Cleanup;
import org.apache.bookkeeper.client.PulsarMockBookKeeper;
import org.apache.bookkeeper.client.PulsarMockLedgerHandle;
import org.apache.bookkeeper.mledger.impl.LedgerMetadataUtils;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.util.FutureUtil;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Schema tests that count the schema ledgers in the broker's mock BookKeeper, so they run on a broker of their own.
 * {@link SchemaTest} has the tests that share one.
 */
@Test(groups = "schema")
public class SchemaOwnBrokerTest extends MockedPulsarServiceBaseTest {

    private static final String CLUSTER_NAME = "test";

    @BeforeClass
    @Override
    public void setup() throws Exception {
        isTcpLookup = true;
        super.internalSetup();

        // Setup namespaces
        admin.clusters().createCluster(CLUSTER_NAME, ClusterData.builder()
                .serviceUrl(pulsar.getWebServiceAddress()).build());
        TenantInfo tenantInfo = TenantInfo.builder()
                .allowedClusters(Collections.singleton(CLUSTER_NAME))
                .build();
        admin.tenants().createTenant(PUBLIC_TENANT, tenantInfo);
    }

    @AfterClass(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    private String getTopicName(String ns, String baseTopic) {
        return ns + "/" + baseTopic;
    }

    /**
     * Test that when multiple producers concurrently create schema for a brand-new topic,
     * the orphan BookKeeper ledgers created by the losing requests are properly cleaned up.
     */
    @Test
    public void testConcurrentCreateSchemaNoOrphanLedger() throws Exception {
        final String namespace = "test-namespace-" + randomName(16);
        String ns = PUBLIC_TENANT + "/" + namespace;
        admin.namespaces().createNamespace(ns, Sets.newHashSet(CLUSTER_NAME));

        final String topic = getTopicName(ns, "testConcurrentCreateSchemaNoOrphanLedger");
        final String schemaName = TopicName.get(topic).getSchemaName();

        PulsarMockBookKeeper mockBk = (PulsarMockBookKeeper) pulsar.getBookKeeperClient();

        // Concurrently create producers with the same schema on a brand-new topic
        int concurrency = 16;
        List<CompletableFuture<Producer<Schemas.PersonOne>>> producers = createProducersInParallel(
                topic, Schema.AVRO(Schemas.PersonOne.class), concurrency);
        try {
            FutureUtil.waitForAll(producers).join();

            // Verify only 1 schema version exists
            assertEquals(admin.schemas().getAllSchemas(topic).size(), 1);

            int schemaLedgerCount = countSchemaLedgers(mockBk, schemaName);
            assertEquals(schemaLedgerCount, 1,
                    "Expected exactly 1 schema ledger for the topic, but found "
                            + schemaLedgerCount + ". Orphan ledgers were not cleaned up.");
        } finally {
            closeProducers(producers);
        }
    }

    /**
     * Test that concurrent compatible schema updates clean up ledgers created by requests
     * that lose the schema locator CAS race.
     */
    @Test
    public void testConcurrentUpdateSchemaNoOrphanLedger() throws Exception {
        final String namespace = "test-namespace-" + randomName(16);
        String ns = PUBLIC_TENANT + "/" + namespace;
        admin.namespaces().createNamespace(ns, Sets.newHashSet(CLUSTER_NAME));

        final String topic = getTopicName(ns, "testConcurrentUpdateSchemaNoOrphanLedger");
        final String schemaName = TopicName.get(topic).getSchemaName();
        PulsarMockBookKeeper mockBk = (PulsarMockBookKeeper) pulsar.getBookKeeperClient();

        @Cleanup
        Producer<Schemas.PersonOne> initialProducer = pulsarClient
                .newProducer(Schema.AVRO(Schemas.PersonOne.class))
                .topic(topic)
                .create();
        assertEquals(admin.schemas().getAllSchemas(topic).size(), 1);

        int concurrency = 16;
        List<CompletableFuture<Producer<Schemas.PersonThree>>> producers = createProducersInParallel(
                topic, Schema.AVRO(Schemas.PersonThree.class), concurrency);
        try {
            FutureUtil.waitForAll(producers).join();

            assertEquals(admin.schemas().getAllSchemas(topic).size(), 2);
            int schemaLedgerCount = countSchemaLedgers(mockBk, schemaName);
            assertEquals(schemaLedgerCount, 2,
                    "Expected exactly 2 schema ledgers for the topic, but found "
                            + schemaLedgerCount + ". Orphan ledgers were not cleaned up.");
        } finally {
            closeProducers(producers);
        }
    }

    private <T> List<CompletableFuture<Producer<T>>> createProducersInParallel(
            String topic, Schema<T> schema, int concurrency) throws InterruptedException {
        @Cleanup("shutdownNow")
        ExecutorService executor = Executors.newFixedThreadPool(concurrency);
        List<CompletableFuture<Producer<T>>> producers = Collections.synchronizedList(new ArrayList<>(concurrency));
        CountDownLatch latch = new CountDownLatch(concurrency);
        for (int i = 0; i < concurrency; i++) {
            executor.execute(() -> {
                try {
                    producers.add(pulsarClient.newProducer(schema).topic(topic).createAsync());
                } finally {
                    latch.countDown();
                }
            });
        }
        latch.await();
        return producers;
    }

    private int countSchemaLedgers(PulsarMockBookKeeper mockBk, String schemaName) {
        int schemaLedgerCount = 0;
        for (PulsarMockLedgerHandle lh : mockBk.getLedgerMap().values()) {
            Map<String, byte[]> metadata = lh.getLedgerMetadata().getCustomMetadata();
            byte[] schemaIdBytes = metadata.get(LedgerMetadataUtils.METADATA_PROPERTY_SCHEMAID);
            if (schemaIdBytes != null && schemaName.equals(new String(schemaIdBytes, StandardCharsets.UTF_8))) {
                schemaLedgerCount++;
            }
        }
        return schemaLedgerCount;
    }

    private <T> void closeProducers(List<CompletableFuture<Producer<T>>> producers) {
        producers.forEach(p -> {
            try {
                p.join().close();
            } catch (Exception ignore) {
            }
        });
        producers.clear();
    }
}
