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
package org.apache.pulsar.compaction;

import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.swagger.models.auth.In;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.api.OpenBuilder;
import org.apache.bookkeeper.mledger.ManagedLedgerInfo;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.EncryptionKeyInfo;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker")
public class CompactionRetentionTest extends MockedPulsarServiceBaseTest {
    private ScheduledExecutorService compactionScheduler;
    private BookKeeper bk;

    @BeforeMethod
    @Override
    public void setup() throws Exception {
        conf.setManagedLedgerMinLedgerRolloverTimeMinutes(0);
        conf.setManagedLedgerMaxEntriesPerLedger(2);
        conf.setTopicLevelPoliciesEnabled(true);
        conf.setSystemTopicEnabled(true);
        super.internalSetup();

        admin.clusters().createCluster("test", ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        admin.tenants().createTenant("my-tenant",
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("test")));
        admin.namespaces().createNamespace("my-tenant/my-ns", Collections.singleton("test"));

        compactionScheduler = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat("compaction-%d").setDaemon(true).build());
        bk = pulsar.getBookKeeperClientFactory().create(this.conf, null, null, Optional.empty(), null);
    }

    @AfterMethod(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();

        if (compactionScheduler != null) {
            compactionScheduler.shutdownNow();
        }
    }

    /**
     * Compaction should retain expired keys in the compacted view
     */
    @Test
    public void testCompaction() throws Exception {
        String topic = "persistent://my-tenant/my-ns/my-topic-" + System.nanoTime();

        Set<String> keys = Sets.newHashSet("a", "b", "c");
        Set<String> keysToExpire = Sets.newHashSet("x1", "x2");
        Set<String> allKeys = new HashSet<>();
        allKeys.addAll(keys);
        allKeys.addAll(keysToExpire);

        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(SerializationFeature.INDENT_OUTPUT, true);

        @Cleanup
        Producer<Integer> producer = pulsarClient.newProducer(Schema.INT32)
                .topic(topic)
                .create();

        Compactor compactor = new TwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic).join();

        log.info(" ---- X 1: {}", mapper.writeValueAsString(
                admin.topics().getInternalStats(topic, false)));

        int round = 1;

        for (String key : allKeys) {
            producer.newMessage()
                    .key(key)
                    .value(round)
                    .send();
        }

        log.info(" ---- X 2: {}", mapper.writeValueAsString(
                admin.topics().getInternalStats(topic, false)));

        validateMessages(pulsarClient, true, topic, round, allKeys);

        compactor.compact(topic).join();

        log.info(" ---- X 3: {}", mapper.writeValueAsString(
                admin.topics().getInternalStats(topic, false)));

        validateMessages(pulsarClient, true, topic, round, allKeys);

        round = 2;

        for (String key : allKeys) {
            producer.newMessage()
                    .key(key)
                    .value(round)
                    .send();
        }

        compactor.compact(topic).join();

        validateMessages(pulsarClient, true, topic, round, allKeys);

        // Now explicitly remove the expiring keys
        for (String key : keysToExpire) {
            producer.newMessage()
                    .key(key)
                    .send();
        }

        compactor.compact(topic).join();

        log.info(" ---- X 4: {}", mapper.writeValueAsString(
                admin.topics().getInternalStats(topic, false)));

        validateMessages(pulsarClient, true, topic, round, keys);

        // In the raw topic there should be no messages
        validateMessages(pulsarClient, false, topic, round, Collections.emptySet());
    }


    /**
     * When a topic is created, if the compaction threshold are set, the data should be retained in the compacted view,
     * even if the topic is not yet compacted.
     */
    @Test
    public void testCompactionRetentionOnTopicCreationWithNamespacePolicies() throws Exception {
        String namespace = "my-tenant/my-ns";
        String topic = "persistent://my-tenant/my-ns/my-topic-" + System.nanoTime();
        admin.namespaces().setCompactionThreshold(namespace, 10);

        testCompactionCursorRetention(topic);
    }

    @Test
    public void testCompactionRetentionAfterTopicCreationWithNamespacePolicies() throws Exception {
        String namespace = "my-tenant/my-ns";
        String topic = "persistent://my-tenant/my-ns/my-topic-" + System.nanoTime();

        // Pre-create the topic, so that compaction is enabled only after the topic was created
        pulsarClient.newProducer(Schema.INT32).topic(topic).create().close();

        admin.namespaces().setCompactionThreshold(namespace, 10);

        Awaitility.await().untilAsserted(() ->
                testCompactionCursorRetention(topic)
        );
    }

    @Test
    public void testCompactionRetentionOnTopicCreationWithTopicPolicies() throws Exception {
        String topic = "persistent://my-tenant/my-ns/my-topic-" + System.nanoTime();

        // Pre-create the topic, otherwise setting policies will fail
        pulsarClient.newProducer(Schema.INT32).topic(topic).create().close();

        admin.topics().setCompactionThreshold(topic, 10);

        Awaitility.await().untilAsserted(() ->
                testCompactionCursorRetention(topic)
        );
    }

    private void testCompactionCursorRetention(String topic) throws Exception {
        Set<String> keys = Sets.newHashSet("a", "b", "c");
        Set<String> keysToExpire = Sets.newHashSet("x1", "x2");
        Set<String> allKeys = new HashSet<>();
        allKeys.addAll(keys);
        allKeys.addAll(keysToExpire);

        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(SerializationFeature.INDENT_OUTPUT, true);

        @Cleanup
        Producer<Integer> producer = pulsarClient.newProducer(Schema.INT32)
                .topic(topic)
                .create();

        Compactor compactor = new TwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);

        log.info(" ---- X 1: {}", mapper.writeValueAsString(
                admin.topics().getInternalStats(topic, false)));

        int round = 1;

        for (String key : allKeys) {
            producer.newMessage()
                    .key(key)
                    .value(round)
                    .send();
        }

        log.info(" ---- X 2: {}", mapper.writeValueAsString(
                admin.topics().getInternalStats(topic, false)));

        validateMessages(pulsarClient, true, topic, round, allKeys);

        compactor.compact(topic).join();

        log.info(" ---- X 3: {}", mapper.writeValueAsString(
                admin.topics().getInternalStats(topic, false)));

        validateMessages(pulsarClient, true, topic, round, allKeys);
    }

    private void validateMessages(PulsarClient client, boolean readCompacted, String topic, int round, Set<String> expectedKeys)
            throws Exception {
        @Cleanup
        Reader<Integer> reader = client.newReader(Schema.INT32)
                .topic(topic)
                .startMessageId(MessageId.earliest)
                .readCompacted(readCompacted)
                .create();

        Map<String, Integer> receivedValues = new HashMap<>();

        while (true) {
            Message<Integer> msg = reader.readNext(1, TimeUnit.SECONDS);
            if (msg == null) {
                break;
            }

            Integer value = msg.size() > 0 ? msg.getValue() : null;
            log.info("Received: {} -- value: {}", msg.getKey(), value);
            if (value != null) {
                receivedValues.put(msg.getKey(), value);
            }
        }

        Map<String, Integer> expectedReceivedValues = new HashMap<>();
        expectedKeys.forEach(k -> expectedReceivedValues.put(k, round));

        log.info("Received values: {}", receivedValues);
        log.info("Expected values: {}", expectedReceivedValues);
        assertEquals(receivedValues, expectedReceivedValues);
    }
}
