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
package org.apache.pulsar.compaction;

import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.Free;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.Owned;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.Assigned;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.Splitting;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.isValidTransition;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
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
import java.util.stream.Collectors;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.commons.lang.reflect.FieldUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateCompactionStrategy;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateData;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.util.FutureUtil;
import org.awaitility.Awaitility;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker-compaction")
public class ServiceUnitStateCompactionTest extends MockedPulsarServiceBaseTest {
    private ScheduledExecutorService compactionScheduler;
    private BookKeeper bk;
    private Schema<ServiceUnitStateData> schema;
    private ServiceUnitStateCompactionStrategy strategy;

    private ServiceUnitState testState0 = Free;
    private ServiceUnitState testState1 = Free;
    private ServiceUnitState testState2 = Free;
    private ServiceUnitState testState3 = Free;
    private ServiceUnitState testState4 = Free;

    private static Random RANDOM = new Random();


    private ServiceUnitStateData testValue(ServiceUnitState state, String broker) {
        if (state == Free) {
            return null;
        }
        return new ServiceUnitStateData(state, broker);
    }

    private ServiceUnitStateData testValue0(String broker) {
        ServiceUnitState to = nextValidState(testState0);
        testState0 = to;
        return testValue(to, broker);
    }

    private ServiceUnitStateData testValue1(String broker) {
        ServiceUnitState to = nextValidState(testState1);
        testState1 = to;
        return testValue(to, broker);
    }

    private ServiceUnitStateData testValue2(String broker) {
        ServiceUnitState to = nextValidState(testState2);
        testState2 = to;
        return testValue(to, broker);
    }

    private ServiceUnitStateData testValue3(String broker) {
        ServiceUnitState to = nextValidState(testState3);
        testState3 = to;
        return testValue(to, broker);
    }

    private ServiceUnitStateData testValue4(String broker) {
        ServiceUnitState to = nextValidState(testState4);
        testState4 = to;
        return testValue(to, broker);
    }

    private ServiceUnitState nextValidState(ServiceUnitState from) {
        List<ServiceUnitState> candidates = Arrays.stream(ServiceUnitState.values())
                .filter(to -> to != Free && to != Splitting && isValidTransition(from, to))
                .collect(Collectors.toList());
        var state=  candidates.get(RANDOM.nextInt(candidates.size()));
        return state;
    }

    private ServiceUnitState nextInvalidState(ServiceUnitState from) {
        List<ServiceUnitState> candidates = Arrays.stream(ServiceUnitState.values())
                .filter(to -> !isValidTransition(from, to))
                .collect(Collectors.toList());
        if (candidates.size() == 0) {
            return null;
        }
        return candidates.get(RANDOM.nextInt(candidates.size()));
    }

    private List<ServiceUnitState> nextStatesToNull(ServiceUnitState from) {
        if (from == null) {
            return List.of();
        }
        return switch (from) {
            case Assigned -> List.of(Owned);
            case Owned -> List.of();
            case Splitting -> List.of();
            default -> List.of();
        };
    }

    @BeforeMethod
    @Override
    public void setup() throws Exception {
        super.internalSetup();

        admin.clusters().createCluster("use", ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        admin.tenants().createTenant("my-property",
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("use")));
        admin.namespaces().createNamespace("my-property/use/my-ns");

        compactionScheduler = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat("compaction-%d").setDaemon(true).build());
        bk = pulsar.getBookKeeperClientFactory().create(this.conf, null, null, Optional.empty(), null);
        schema = Schema.JSON(ServiceUnitStateData.class);
        strategy = new ServiceUnitStateCompactionStrategy();
        strategy.checkBrokers(false);

    }


    @AfterMethod(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();

        if (compactionScheduler != null) {
            compactionScheduler.shutdownNow();
        }
    }


    public record TestData(
            String topic,
            Map<String, ServiceUnitStateData> expected,
            List<Pair<String, ServiceUnitStateData>> all) {

    }
    TestData generateTestData() throws PulsarAdminException, PulsarClientException {
        String topic = "persistent://my-property/use/my-ns/my-topic1";
        final int numMessages = 20;
        final int maxKeys = 5;

        // Configure retention to ensue data is retained for reader
        admin.namespaces().setRetention("my-property/use/my-ns", new RetentionPolicies(-1, -1));

        Producer<ServiceUnitStateData> producer = pulsarClient.newProducer(schema)
                .topic(topic)
                .enableBatching(true)
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .create();

        Map<String, ServiceUnitStateData> expected = new HashMap<>();
        List<Pair<String, ServiceUnitStateData>> all = new ArrayList<>();
        Random r = new Random(0);

        pulsarClient.newConsumer(schema)
                .topic(topic)
                .subscriptionName("sub1")
                .readCompacted(true)
                .subscribe().close();

        for (int j = 0; j < numMessages; j++) {
            int keyIndex = r.nextInt(maxKeys);
            String key = "key" + keyIndex;
            ServiceUnitStateData prev = expected.get(key);
            ServiceUnitState prevState = prev == null ? Free : prev.state();
            ServiceUnitState state = r.nextBoolean() ? nextInvalidState(prevState) :
                    nextValidState(prevState);
            ServiceUnitStateData value = new ServiceUnitStateData(state, key + ":" + j);
            producer.newMessage().key(key).value(value).send();
            if (!strategy.shouldKeepLeft(prev, value)) {
                expected.put(key, value);
            }
            all.add(Pair.of(key, value));
        }
        return new TestData(topic, expected, all);
    }

    @Test
    public void testCompaction() throws Exception {
        TestData testData = generateTestData();
        var topic = testData.topic;
        var expected = testData.expected;
        var all = testData.all;

        StrategicTwoPhaseCompactor compactor
                = new StrategicTwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic, strategy).get();

        PersistentTopicInternalStats internalStats = admin.topics().getInternalStats(topic, false);
        // Compacted topic ledger should have same number of entry equals to number of unique key.
        //Assert.assertEquals(internalStats.compactedLedger.entries, expected.size());
        Assert.assertTrue(internalStats.compactedLedger.ledgerId > -1);
        Assert.assertFalse(internalStats.compactedLedger.offloaded);

        // consumer with readCompacted enabled only get compacted entries
        try (Consumer<ServiceUnitStateData> consumer = pulsarClient.newConsumer(schema).topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscribe()) {
            while (true) {
                Message<ServiceUnitStateData> m = consumer.receive(2, TimeUnit.SECONDS);
                Assert.assertEquals(expected.remove(m.getKey()), m.getValue());
                if (expected.isEmpty()) {
                    break;
                }
            }
            Assert.assertTrue(expected.isEmpty());
        }

        // can get full backlog if read compacted disabled
        try (Consumer<ServiceUnitStateData> consumer = pulsarClient.newConsumer(schema).topic(topic).subscriptionName("sub1")
                .readCompacted(false).subscribe()) {
            while (true) {
                Message<ServiceUnitStateData> m = consumer.receive(2, TimeUnit.SECONDS);
                Pair<String, ServiceUnitStateData> expectedMessage = all.remove(0);
                Assert.assertEquals(expectedMessage.getLeft(), m.getKey());
                Assert.assertEquals(expectedMessage.getRight(), m.getValue());
                if (all.isEmpty()) {
                    break;
                }
            }
            Assert.assertTrue(all.isEmpty());
        }
    }

    @Test
    public void testCompactionWithReader() throws Exception {
        TestData testData = generateTestData();
        var topic = testData.topic;
        var expected = testData.expected;
        var all = testData.all;

        StrategicTwoPhaseCompactor compactor
                = new StrategicTwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic, strategy).get();

        // consumer with readCompacted enabled only get compacted entries
        try (Reader<ServiceUnitStateData> reader = pulsarClient.newReader(schema).topic(topic).readCompacted(true)
                .startMessageId(MessageId.earliest).create()) {
            while (true) {
                Message<ServiceUnitStateData> m = reader.readNext(2, TimeUnit.SECONDS);
                Assert.assertEquals(expected.remove(m.getKey()), m.getValue());
                if (expected.isEmpty()) {
                    break;
                }
            }
            Assert.assertTrue(expected.isEmpty());
        }

        // can get full backlog if read compacted disabled
        try (Reader<ServiceUnitStateData> reader = pulsarClient.newReader(schema).topic(topic).readCompacted(false)
                .startMessageId(MessageId.earliest).create()) {
            while (true) {
                Message<ServiceUnitStateData> m = reader.readNext(2, TimeUnit.SECONDS);
                Pair<String, ServiceUnitStateData> expectedMessage = all.remove(0);
                Assert.assertEquals(expectedMessage.getLeft(), m.getKey());
                Assert.assertEquals(expectedMessage.getRight(), m.getValue());
                if (all.isEmpty()) {
                    break;
                }
            }
            Assert.assertTrue(all.isEmpty());
        }
    }


    @Test
    public void testCompactionWithTableview() throws Exception {
        var tv = pulsar.getClient().newTableViewBuilder(schema)
                .topic("persistent://my-property/use/my-ns/my-topic1")
                .loadConf(Map.of(
                        "topicCompactionStrategyClassName",
                        ServiceUnitStateCompactionStrategy.class.getName()))
                .create();

        ((ServiceUnitStateCompactionStrategy)
                FieldUtils.readDeclaredField(tv, "compactionStrategy", true))
                .checkBrokers(false);
        TestData testData = generateTestData();
        var topic = testData.topic;
        var expected = testData.expected;
        var expectedCopy = new HashMap<>(expected);

        Awaitility.await()
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> assertEquals(expectedCopy.size(), tv.size()));

        for(var etr : tv.entrySet()){
            Assert.assertEquals(expectedCopy.remove(etr.getKey()), etr.getValue());
            if (expectedCopy.isEmpty()) {
                break;
            }
        }

        Assert.assertTrue(expectedCopy.isEmpty());
        tv.close();;

        StrategicTwoPhaseCompactor compactor
                = new StrategicTwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic, strategy).get();

        // consumer with readCompacted enabled only get compacted entries
        var tableview = pulsar.getClient().newTableViewBuilder(schema)
                .topic(topic)
                .loadConf(Map.of(
                        "topicCompactionStrategyClassName",
                        ServiceUnitStateCompactionStrategy.class.getName()))
                .create();
        for(var etr : tableview.entrySet()){
            Assert.assertEquals(expected.remove(etr.getKey()), etr.getValue());
            if (expected.isEmpty()) {
                break;
            }
        }
        Assert.assertTrue(expected.isEmpty());
        tableview.close();

    }


    @Test
    public void testReadCompactedBeforeCompaction() throws Exception {
        String topic = "persistent://my-property/use/my-ns/my-topic1";

        Producer<ServiceUnitStateData> producer = pulsarClient.newProducer(schema)
                .topic(topic)
                .enableBatching(true)
                .create();

        pulsarClient.newConsumer(schema).topic(topic).subscriptionName("sub1").readCompacted(true).subscribe().close();

        producer.newMessage().key("key0").value(testValue0( "content0")).send();
        producer.newMessage().key("key0").value(testValue0("content1")).send();
        producer.newMessage().key("key0").value(testValue0( "content2")).send();

        try (Consumer<ServiceUnitStateData> consumer = pulsarClient.newConsumer(schema).topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscribe()) {
            Message<ServiceUnitStateData> m = consumer.receive();
            Assert.assertEquals(m.getKey(), "key0");
            Assert.assertEquals(m.getValue().broker(), "content0");

            m = consumer.receive();
            Assert.assertEquals(m.getKey(), "key0");
            Assert.assertEquals(m.getValue().broker(), "content1");

            m = consumer.receive();
            Assert.assertEquals(m.getKey(), "key0");
            Assert.assertEquals(m.getValue().broker(), "content2");
        }

        StrategicTwoPhaseCompactor compactor
                = new StrategicTwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic, strategy).get();

        try (Consumer<ServiceUnitStateData> consumer = pulsarClient.newConsumer(schema).topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscribe()) {
            Message<ServiceUnitStateData> m = consumer.receive();
            Assert.assertEquals(m.getKey(), "key0");
            Assert.assertEquals(m.getValue().broker(), "content2");
        }
    }

    @Test
    public void testReadEntriesAfterCompaction() throws Exception {
        String topic = "persistent://my-property/use/my-ns/my-topic1";

        Producer<ServiceUnitStateData> producer = pulsarClient.newProducer(schema)
                .topic(topic)
                .enableBatching(true)
                .create();

        pulsarClient.newConsumer(schema).topic(topic).subscriptionName("sub1").readCompacted(true).subscribe().close();

        producer.newMessage().key("key0").value(testValue0( "content0")).send();
        producer.newMessage().key("key0").value(testValue0("content1")).send();
        producer.newMessage().key("key0").value(testValue0( "content2")).send();

        StrategicTwoPhaseCompactor compactor
                = new StrategicTwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic, strategy).get();

        producer.newMessage().key("key0").value(testValue0("content3")).send();

        try (Consumer<ServiceUnitStateData> consumer = pulsarClient.newConsumer(schema).topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscribe()) {
            Message<ServiceUnitStateData> m = consumer.receive();
            Assert.assertEquals(m.getKey(), "key0");
            Assert.assertEquals(m.getValue().broker(), "content2");

            m = consumer.receive();
            Assert.assertEquals(m.getKey(), "key0");
            Assert.assertEquals(m.getValue().broker(), "content3");
        }
    }

    @Test
    public void testSeekEarliestAfterCompaction() throws Exception {
        String topic = "persistent://my-property/use/my-ns/my-topic1";

        Producer<ServiceUnitStateData> producer = pulsarClient.newProducer(schema)
                .topic(topic)
                .enableBatching(true)
                .create();

        producer.newMessage().key("key0").value(testValue0( "content0")).send();
        producer.newMessage().key("key0").value(testValue0("content1")).send();
        producer.newMessage().key("key0").value(testValue0( "content2")).send();

        StrategicTwoPhaseCompactor compactor
                = new StrategicTwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic, strategy).get();

        try (Consumer<ServiceUnitStateData> consumer = pulsarClient.newConsumer(schema).topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscribe()) {
            consumer.seek(MessageId.earliest);
            Message<ServiceUnitStateData> m = consumer.receive();
            Assert.assertEquals(m.getKey(), "key0");
            Assert.assertEquals(m.getValue().broker(), "content2");
        }

        try (Consumer<ServiceUnitStateData> consumer = pulsarClient.newConsumer(schema).topic(topic).subscriptionName("sub1")
                .readCompacted(false).subscribe()) {
            consumer.seek(MessageId.earliest);

            Message<ServiceUnitStateData> m = consumer.receive();
            Assert.assertEquals(m.getKey(), "key0");
            Assert.assertEquals(m.getValue().broker(), "content0");

            m = consumer.receive();
            Assert.assertEquals(m.getKey(), "key0");
            Assert.assertEquals(m.getValue().broker(), "content1");

            m = consumer.receive();
            Assert.assertEquals(m.getKey(), "key0");
            Assert.assertEquals(m.getValue().broker(), "content2");
        }
    }

    @Test
    public void testBrokerRestartAfterCompaction() throws Exception {
        String topic = "persistent://my-property/use/my-ns/my-topic1";

        Producer<ServiceUnitStateData> producer = pulsarClient.newProducer(schema)
                .topic(topic)
                .enableBatching(true)
                .create();

        pulsarClient.newConsumer(schema).topic(topic).subscriptionName("sub1").readCompacted(true).subscribe().close();

        producer.newMessage().key("key0").value(testValue0( "content0")).send();
        producer.newMessage().key("key0").value(testValue0("content1")).send();
        producer.newMessage().key("key0").value(testValue0( "content2")).send();

        StrategicTwoPhaseCompactor compactor
                = new StrategicTwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic, strategy).get();

        try (Consumer<ServiceUnitStateData> consumer = pulsarClient.newConsumer(schema).topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscribe()) {
            Message<ServiceUnitStateData> m = consumer.receive();
            Assert.assertEquals(m.getKey(), "key0");
            Assert.assertEquals(m.getValue().broker(), "content2");
        }

        stopBroker();
        try (Consumer<ServiceUnitStateData> consumer = pulsarClient.newConsumer(schema).topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscribe()) {
            consumer.receive();
            Assert.fail("Shouldn't have been able to receive anything");
        } catch (PulsarClientException e) {
            // correct behaviour
        }
        startBroker();

        try (Consumer<ServiceUnitStateData> consumer = pulsarClient.newConsumer(schema).topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscribe()) {
            Message<ServiceUnitStateData> m = consumer.receive();
            Assert.assertEquals(m.getKey(), "key0");
            Assert.assertEquals(m.getValue().broker(), "content2");
        }
    }

    @Test
    public void testCompactEmptyTopic() throws Exception {
        String topic = "persistent://my-property/use/my-ns/my-topic1";

        Producer<ServiceUnitStateData> producer = pulsarClient.newProducer(schema)
                .topic(topic)
                .enableBatching(true)
                .create();

        pulsarClient.newConsumer(schema).topic(topic).subscriptionName("sub1").readCompacted(true).subscribe().close();

        StrategicTwoPhaseCompactor compactor
                = new StrategicTwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic, strategy).get();

        producer.newMessage().key("key0").value(testValue0( "content0")).send();

        try (Consumer<ServiceUnitStateData> consumer = pulsarClient.newConsumer(schema).topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscribe()) {
            Message<ServiceUnitStateData> m = consumer.receive();
            Assert.assertEquals(m.getKey(), "key0");
            Assert.assertEquals(m.getValue().broker(), "content0");
        }
    }

    @Test
    public void testWholeBatchCompactedOut() throws Exception {
        String topic = "persistent://my-property/use/my-ns/my-topic1";

        // subscribe before sending anything, so that we get all messages
        pulsarClient.newConsumer(schema).topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscribe().close();

        try (Producer<ServiceUnitStateData> producerNormal = pulsarClient.newProducer(schema).topic(topic)
                .enableBatching(true)
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .create();
             Producer<ServiceUnitStateData> producerBatch = pulsarClient.newProducer(schema).topic(topic)
                     .maxPendingMessages(3)
                     .enableBatching(true)
                     .batchingMaxMessages(3)
                     .batchingMaxPublishDelay(1, TimeUnit.HOURS)
                     .messageRoutingMode(MessageRoutingMode.SinglePartition)
                     .create()) {
            producerBatch.newMessage().key("key1").value(testValue1("my-message-1")).sendAsync();
            producerBatch.newMessage().key("key1").value(testValue1( "my-message-2")).sendAsync();
            producerBatch.newMessage().key("key1").value(testValue1("my-message-3")).sendAsync();
            producerNormal.newMessage().key("key1").value(testValue1( "my-message-4")).send();
        }

        // compact the topic
        StrategicTwoPhaseCompactor compactor
                = new StrategicTwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic, strategy).get();

        try (Consumer<ServiceUnitStateData> consumer = pulsarClient.newConsumer(schema).topic(topic)
                .subscriptionName("sub1").readCompacted(true).subscribe()) {
            Message<ServiceUnitStateData> message = consumer.receive();
            Assert.assertEquals(message.getKey(), "key1");
            Assert.assertEquals(new String(message.getValue().broker()), "my-message-4");
        }
    }

    public void testCompactionWithLastDeletedKey() throws Exception {
        String topic = "persistent://my-property/use/my-ns/my-topic1";

        Producer<ServiceUnitStateData> producer = pulsarClient.newProducer(schema).topic(topic).enableBatching(true)
                .messageRoutingMode(MessageRoutingMode.SinglePartition).create();

        pulsarClient.newConsumer(schema).topic(topic).subscriptionName("sub1").readCompacted(true).subscribe().close();

        producer.newMessage().key("1").value(testValue(Owned, "1")).send();
        producer.newMessage().key("2").value(testValue(Owned, "3")).send();
        producer.newMessage().key("3").value(testValue(Owned, "5")).send();
        producer.newMessage().key("1").value(null).send();
        producer.newMessage().key("2").value(null).send();

        StrategicTwoPhaseCompactor compactor
                = new StrategicTwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic, strategy).get();

        Set<String> expected = Sets.newHashSet("3");
        // consumer with readCompacted enabled only get compacted entries
        try (Consumer<ServiceUnitStateData> consumer = pulsarClient.newConsumer(schema).topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscribe()) {
            Message<ServiceUnitStateData> m = consumer.receive(2, TimeUnit.SECONDS);
            assertTrue(expected.remove(m.getKey()));
        }
    }

    @Test(timeOut = 20000)
    public void testEmptyCompactionLedger() throws Exception {
        String topic = "persistent://my-property/use/my-ns/my-topic1";

        Producer<ServiceUnitStateData> producer = pulsarClient.newProducer(schema).topic(topic).enableBatching(true)
                .messageRoutingMode(MessageRoutingMode.SinglePartition).create();

        pulsarClient.newConsumer(schema).topic(topic).subscriptionName("sub1").readCompacted(true).subscribe().close();

        producer.newMessage().key("1").value(testValue(Owned, "1")).send();
        producer.newMessage().key("2").value(testValue(Owned, "3")).send();
        producer.newMessage().key("1").value(null).send();
        producer.newMessage().key("2").value(null).send();

        StrategicTwoPhaseCompactor compactor
                = new StrategicTwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic, strategy).get();

        // consumer with readCompacted enabled only get compacted entries
        try (Consumer<ServiceUnitStateData> consumer = pulsarClient.newConsumer(schema).topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscribe()) {
            Message<ServiceUnitStateData> m = consumer.receive(2, TimeUnit.SECONDS);
            assertNull(m);
        }
    }

    @Test(timeOut = 20000)
    public void testAllEmptyCompactionLedger() throws Exception {
        final String topic =
                "persistent://my-property/use/my-ns/testAllEmptyCompactionLedger" + UUID.randomUUID().toString();

        final int messages = 10;

        // 1.create producer and publish message to the topic.
        ProducerBuilder<ServiceUnitStateData> builder = pulsarClient.newProducer(schema).topic(topic);
        builder.batchingMaxMessages(messages / 5);

        Producer<ServiceUnitStateData> producer = builder.create();

        List<CompletableFuture<MessageId>> futures = new ArrayList<>(messages);
        for (int i = 0; i < messages; i++) {
            futures.add(producer.newMessage().key("1").value(null).sendAsync());
        }

        FutureUtil.waitForAll(futures).get();

        // 2.compact the topic.
        StrategicTwoPhaseCompactor compactor
                = new StrategicTwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic, strategy).get();

        // consumer with readCompacted enabled only get compacted entries
        try (Consumer<ServiceUnitStateData> consumer = pulsarClient.newConsumer(schema).topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscriptionInitialPosition(SubscriptionInitialPosition.Earliest).subscribe()) {
            Message<ServiceUnitStateData> m = consumer.receive(2, TimeUnit.SECONDS);
            assertNull(m);
        }
    }

    @Test(timeOut = 20000)
    public void testCompactMultipleTimesWithoutEmptyMessage()
            throws PulsarClientException, ExecutionException, InterruptedException {
        final String topic =
                "persistent://my-property/use/my-ns/testCompactMultipleTimesWithoutEmptyMessage" + UUID.randomUUID()
                        .toString();

        final int messages = 10;
        final String key = "1";

        // 1.create producer and publish message to the topic.
        ProducerBuilder<ServiceUnitStateData> builder = pulsarClient.newProducer(schema).topic(topic);
        builder.enableBatching(true);


        Producer<ServiceUnitStateData> producer = builder.create();

        List<CompletableFuture<MessageId>> futures = new ArrayList<>(messages);
        for (int i = 0; i < messages; i++) {
            futures.add(producer.newMessage().key(key).value(testValue0((i + ""))).sendAsync());
        }

        FutureUtil.waitForAll(futures).get();

        // 2.compact the topic.
        StrategicTwoPhaseCompactor compactor
                = new StrategicTwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic, strategy).get();

        // 3. Send more ten messages
        futures.clear();
        for (int i = 0; i < messages; i++) {
            futures.add(producer.newMessage().key(key).value(testValue0((i + 10 + ""))).sendAsync());
        }
        FutureUtil.waitForAll(futures).get();

        // 4.compact again.
        compactor.compact(topic, strategy).get();

        try (Consumer<ServiceUnitStateData> consumer = pulsarClient.newConsumer(schema).topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscriptionInitialPosition(SubscriptionInitialPosition.Earliest).subscribe()) {
            Message<ServiceUnitStateData> m1 = consumer.receive();
            assertNotNull(m1);
            assertEquals(m1.getKey(), key);
            assertEquals(m1.getValue().broker(), "19");
            Message<ServiceUnitStateData> none = consumer.receive(2, TimeUnit.SECONDS);
            assertNull(none);
        }
    }

    @Test(timeOut = 200000)
    public void testReadUnCompacted()
            throws PulsarClientException, ExecutionException, InterruptedException {
        final String topic = "persistent://my-property/use/my-ns/testReadUnCompacted" + UUID.randomUUID().toString();

        final int messages = 10;
        final String key = "1";

        // 1.create producer and publish message to the topic.
        ProducerBuilder<ServiceUnitStateData> builder = pulsarClient.newProducer(schema).topic(topic);
        builder.batchingMaxMessages(messages / 5);

        Producer<ServiceUnitStateData> producer = builder.create();

        List<CompletableFuture<MessageId>> futures = new ArrayList<>(messages);
        for (int i = 0; i < messages; i++) {
            futures.add(producer.newMessage().key(key).value(testValue0((i + ""))).sendAsync());
        }

        FutureUtil.waitForAll(futures).get();

        // 2.compact the topic.
        StrategicTwoPhaseCompactor compactor
                = new StrategicTwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic, strategy).get();

        // 3. Send more ten messages
        futures.clear();
        for (int i = 0; i < messages; i++) {
            futures.add(producer.newMessage().key(key).value(testValue0((i + 10 + ""))).sendAsync());
        }
        FutureUtil.waitForAll(futures).get();
        try (Consumer<ServiceUnitStateData> consumer = pulsarClient.newConsumer(schema)
                .topic(topic)
                .subscriptionName("sub1")
                .readCompacted(true)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe()) {
            for (int i = 0; i < 11; i++) {
                Message<ServiceUnitStateData> received = consumer.receive();
                assertNotNull(received);
                assertEquals(received.getKey(), key);
                assertEquals(received.getValue().broker(), i + 9 + "");
                consumer.acknowledge(received);
            }
            Message<ServiceUnitStateData> none = consumer.receive(2, TimeUnit.SECONDS);
            assertNull(none);
        }

        // 4.Send empty message to delete the key-value in the compacted topic.
        for (ServiceUnitState state : nextStatesToNull(testState0)) {
            producer.newMessage().key(key).value(new ServiceUnitStateData(state, "xx")).send();
        }
        producer.newMessage().key(key).value(null).send();

        // 5.compact the topic.
        compactor.compact(topic, strategy).get();

        try (Consumer<ServiceUnitStateData> consumer = pulsarClient.newConsumer(schema)
                .topic(topic)
                .subscriptionName("sub2")
                .readCompacted(true)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe()) {
            Message<ServiceUnitStateData> none = consumer.receive(2, TimeUnit.SECONDS);
            assertNull(none);
        }

        for (int i = 0; i < messages; i++) {
            futures.add(producer.newMessage().key(key).value(testValue0((i + 20 + ""))).sendAsync());
        }
        FutureUtil.waitForAll(futures).get();

        try (Consumer<ServiceUnitStateData> consumer = pulsarClient.newConsumer(schema)
                .topic(topic)
                .subscriptionName("sub3")
                .readCompacted(true)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe()) {
            for (int i = 0; i < 10; i++) {
                Message<ServiceUnitStateData> received = consumer.receive();
                assertNotNull(received);
                assertEquals(received.getKey(), key);
                assertEquals(received.getValue().broker(), i + 20 + "");
                consumer.acknowledge(received);
            }
            Message<ServiceUnitStateData> none = consumer.receive(2, TimeUnit.SECONDS);
            assertNull(none);
        }
    }
}
