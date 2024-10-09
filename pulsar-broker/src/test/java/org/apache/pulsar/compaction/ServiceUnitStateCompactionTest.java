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

import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.Deleted;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.Init;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.Owned;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.Assigning;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.Releasing;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.Splitting;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.StorageType.SystemTopic;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.isValidTransition;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateData.state;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateTableViewImpl.MSG_COMPRESSION_TYPE;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
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
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.Cleanup;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.commons.lang.reflect.FieldUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateChannelImpl;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateDataConflictResolver;
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
import org.apache.pulsar.client.impl.ReaderImpl;
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
    private ServiceUnitStateDataConflictResolver strategy;

    private ServiceUnitState testState = Init;

    private ServiceUnitStateData testData = null;

    private static Random RANDOM = new Random();


    private ServiceUnitStateData testValue(ServiceUnitState state, String broker) {
        if (state == Init) {
            testData = null;
        } else {
            testData = new ServiceUnitStateData(state, broker, versionId(testData) + 1);
        }

        return testData;
    }

    private ServiceUnitStateData testValue(String broker) {
        testState = nextValidStateNonSplit(testState);
        return testValue(testState, broker);
    }

    private ServiceUnitState nextValidState(ServiceUnitState from) {
        List<ServiceUnitState> candidates = Arrays.stream(ServiceUnitState.values())
                .filter(to -> isValidTransition(from, to, SystemTopic))
                .collect(Collectors.toList());
        var state=  candidates.get(RANDOM.nextInt(candidates.size()));
        return state;
    }

    private ServiceUnitState nextValidStateNonSplit(ServiceUnitState from) {
        List<ServiceUnitState> candidates = Arrays.stream(ServiceUnitState.values())
                .filter(to -> to != Init && to != Splitting && to != Deleted
                        && isValidTransition(from, to, SystemTopic))
                .collect(Collectors.toList());
        var state=  candidates.get(RANDOM.nextInt(candidates.size()));
        return state;
    }

    private ServiceUnitState nextInvalidState(ServiceUnitState from) {
        List<ServiceUnitState> candidates = Arrays.stream(ServiceUnitState.values())
                .filter(to -> !isValidTransition(from, to, SystemTopic))
                .collect(Collectors.toList());
        if (candidates.size() == 0) {
            return Init;
        }
        return candidates.get(RANDOM.nextInt(candidates.size()));
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
        bk = pulsar.getBookKeeperClientFactory().create(this.conf, null, null, Optional.empty(), null).get();
        schema = Schema.JSON(ServiceUnitStateData.class);
        strategy = new ServiceUnitStateDataConflictResolver();
        strategy.checkBrokers(false);

        testState = Init;
        testData = null;
    }


    @AfterMethod(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
        bk.close();
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
                .compressionType(MSG_COMPRESSION_TYPE)
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
            ServiceUnitState prevState = state(prev);
            boolean invalid =  r.nextBoolean();
            ServiceUnitState state = invalid ? nextInvalidState(prevState) :
                    nextValidState(prevState);
            ServiceUnitStateData value;
            long versionId = versionId(prev) + 1;
            if (invalid) {
                value = new ServiceUnitStateData(state, key + ":" + j, false, versionId);
            } else {
                if (state == Init) {
                    value = new ServiceUnitStateData(state, key + ":" + j, true, versionId);
                } else {
                    value = new ServiceUnitStateData(state, key + ":" + j, false, versionId);
                }
            }

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
                        ServiceUnitStateDataConflictResolver.class.getName()))
                .create();

        ((ServiceUnitStateDataConflictResolver)
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
        var tableview = pulsar.getClient().newTableView(schema)
                .topic(topic)
                .loadConf(Map.of(
                        "topicCompactionStrategyClassName",
                        ServiceUnitStateDataConflictResolver.class.getName()))
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
                .compressionType(MSG_COMPRESSION_TYPE)
                .enableBatching(true)
                .create();

        pulsarClient.newConsumer(schema).topic(topic).subscriptionName("sub1").readCompacted(true).subscribe().close();
        String key = "key0";
        var testValues = Arrays.asList(
                testValue("content0"), testValue("content1"), testValue("content2"));
        for (var val : testValues) {
            producer.newMessage().key(key).value(val).send();
        }

        try (Consumer<ServiceUnitStateData> consumer = pulsarClient.newConsumer(schema).topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscribe()) {
            Message<ServiceUnitStateData> m = consumer.receive();
            Assert.assertEquals(m.getKey(), key);
            Assert.assertEquals(m.getValue(), testValues.get(0));

            m = consumer.receive();
            Assert.assertEquals(m.getKey(), key);
            Assert.assertEquals(m.getValue(), testValues.get(1));

            m = consumer.receive();
            Assert.assertEquals(m.getKey(), key);
            Assert.assertEquals(m.getValue(), testValues.get(2));
        }

        StrategicTwoPhaseCompactor compactor
                = new StrategicTwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic, strategy).get();

        try (Consumer<ServiceUnitStateData> consumer = pulsarClient.newConsumer(schema).topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscribe()) {
            Message<ServiceUnitStateData> m = consumer.receive();
            Assert.assertEquals(m.getKey(), key);
            Assert.assertEquals(m.getValue(), testValues.get(2));
        }
    }

    @Test
    public void testReadEntriesAfterCompaction() throws Exception {
        String topic = "persistent://my-property/use/my-ns/my-topic1";

        Producer<ServiceUnitStateData> producer = pulsarClient.newProducer(schema)
                .topic(topic)
                .compressionType(MSG_COMPRESSION_TYPE)
                .enableBatching(true)
                .create();

        pulsarClient.newConsumer(schema).topic(topic).subscriptionName("sub1").readCompacted(true).subscribe().close();

        String key = "key0";
        var testValues = Arrays.asList(
                testValue( "content0"),
                testValue("content1"),
                testValue( "content2"),
                testValue("content3"));
        producer.newMessage().key(key).value(testValues.get(0)).send();
        producer.newMessage().key(key).value(testValues.get(1)).send();
        producer.newMessage().key(key).value(testValues.get(2)).send();

        StrategicTwoPhaseCompactor compactor
                = new StrategicTwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic, strategy).get();

        producer.newMessage().key(key).value(testValues.get(3)).send();

        try (Consumer<ServiceUnitStateData> consumer = pulsarClient.newConsumer(schema).topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscribe()) {
            Message<ServiceUnitStateData> m = consumer.receive();
            Assert.assertEquals(m.getKey(), key);
            Assert.assertEquals(m.getValue(), testValues.get(2));

            m = consumer.receive();
            Assert.assertEquals(m.getKey(), key);
            Assert.assertEquals(m.getValue(), testValues.get(3));
        }
    }

    @Test
    public void testSeekEarliestAfterCompaction() throws Exception {

        String topic = "persistent://my-property/use/my-ns/my-topic1";

        Producer<ServiceUnitStateData> producer = pulsarClient.newProducer(schema)
                .topic(topic)
                .compressionType(MSG_COMPRESSION_TYPE)
                .enableBatching(true)
                .create();

        String key = "key0";
        var testValues = Arrays.asList(
                testValue("content0"),
                testValue("content1"),
                testValue("content2"));
        for (var val : testValues) {
            producer.newMessage().key(key).value(val).send();
        }

        StrategicTwoPhaseCompactor compactor
                = new StrategicTwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic, strategy).get();

        try (Consumer<ServiceUnitStateData> consumer = pulsarClient.newConsumer(schema).topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscribe()) {
            consumer.seek(MessageId.earliest);
            Message<ServiceUnitStateData> m = consumer.receive();
            Assert.assertEquals(m.getKey(), key);
            Assert.assertEquals(m.getValue(), testValues.get(2));
        }

        try (Consumer<ServiceUnitStateData> consumer = pulsarClient.newConsumer(schema).topic(topic).subscriptionName("sub1")
                .readCompacted(false).subscribe()) {
            consumer.seek(MessageId.earliest);

            Message<ServiceUnitStateData> m = consumer.receive();
            Assert.assertEquals(m.getKey(), key);
            Assert.assertEquals(m.getValue(), testValues.get(0));

            m = consumer.receive();
            Assert.assertEquals(m.getKey(), key);
            Assert.assertEquals(m.getValue(), testValues.get(1));

            m = consumer.receive();
            Assert.assertEquals(m.getKey(), key);
            Assert.assertEquals(m.getValue(), testValues.get(2));
        }
    }

    @Test
    public void testSlowTableviewAfterCompaction() throws Exception {
        String topic = "persistent://my-property/use/my-ns/my-topic1";
        String strategyClassName = "topicCompactionStrategyClassName";
        strategy.checkBrokers(true);

        pulsarClient.newConsumer(schema)
                .topic(topic)
                .subscriptionName("sub1")
                .readCompacted(true)
                .subscribe().close();

        var fastTV = pulsar.getClient().newTableViewBuilder(schema)
                .topic(topic)
                .subscriptionName("fastTV")
                .loadConf(Map.of(
                        strategyClassName,
                        ServiceUnitStateDataConflictResolver.class.getName()))
                .create();

        var defaultConf = getDefaultConf();
        @Cleanup
        var additionalPulsarTestContext = createAdditionalPulsarTestContext(defaultConf);
        var pulsar2 = additionalPulsarTestContext.getPulsarService();

        var slowTV = pulsar2.getClient().newTableViewBuilder(schema)
                .topic(topic)
                .subscriptionName("slowTV")
                .loadConf(Map.of(
                        strategyClassName,
                        ServiceUnitStateDataConflictResolver.class.getName()))
                .create();

        var semaphore = new Semaphore(0);
        AtomicBoolean handledReleased = new AtomicBoolean(false);

        slowTV.listen((k, v) -> {
            if (v.state() == Assigning) {
                try {
                    // Stuck at handling Assigned
                    handledReleased.set(false);
                    semaphore.acquire();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            } else if (v.state() == Releasing) {
                handledReleased.set(true);
            }
        });

        // Configure retention to ensue data is retained for reader
        admin.namespaces().setRetention("my-property/use/my-ns",
                new RetentionPolicies(-1, -1));

        Producer<ServiceUnitStateData> producer = pulsarClient.newProducer(schema)
                .topic(topic)
                .compressionType(MSG_COMPRESSION_TYPE)
                .enableBatching(true)
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .create();

        StrategicTwoPhaseCompactor compactor
                = new StrategicTwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);

        String bundle = "bundle1";
        String src = "broker0";
        String dst = "broker1";
        long versionId = 1;
        producer.newMessage().key(bundle).value(new ServiceUnitStateData(Owned, src, versionId++)).send();
        for (int i = 0; i < 3; i++) {
            var releasedStateData = new ServiceUnitStateData(Releasing, dst, src, versionId++);
            producer.newMessage().key(bundle).value(releasedStateData).send();
            producer.newMessage().key(bundle).value(releasedStateData).send();
            var assignedStateData = new ServiceUnitStateData(Assigning, dst, src, versionId++);
            producer.newMessage().key(bundle).value(assignedStateData).send();
            producer.newMessage().key(bundle).value(assignedStateData).send();
            var ownedStateData = new ServiceUnitStateData(Owned, dst, src, versionId++);
            producer.newMessage().key(bundle).value(ownedStateData).send();
            producer.newMessage().key(bundle).value(ownedStateData).send();
            compactor.compact(topic, strategy).get();

            Awaitility.await()
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertEquals(fastTV.get(bundle), ownedStateData));

            Awaitility.await()
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertEquals(slowTV.get(bundle), assignedStateData));
            assertTrue(!handledReleased.get());
            semaphore.release();

            Awaitility.await()
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertEquals(slowTV.get(bundle), ownedStateData));

            var newTv = pulsar.getClient().newTableView(schema)
                    .topic(topic)
                    .loadConf(Map.of(
                            strategyClassName,
                            ServiceUnitStateDataConflictResolver.class.getName()))
                    .create();
            Awaitility.await()
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertEquals(newTv.get(bundle), ownedStateData));

            src = dst;
            dst = "broker" + (i + 2);
            newTv.close();
        }

        producer.close();
        slowTV.close();
        fastTV.close();
        pulsar2.close();

    }

    @Test
    public void testSlowReceiveTableviewAfterCompaction() throws Exception {
        String topic = "persistent://my-property/use/my-ns/my-topic1";
        String strategyClassName = "topicCompactionStrategyClassName";

        pulsarClient.newConsumer(schema)
                .topic(topic)
                .subscriptionName("sub1")
                .readCompacted(true)
                .subscribe().close();

        var tv = pulsar.getClient().newTableViewBuilder(schema)
                .topic(topic)
                .subscriptionName("slowTV")
                .loadConf(Map.of(
                        strategyClassName,
                        ServiceUnitStateDataConflictResolver.class.getName()))
                .create();

        // Configure retention to ensue data is retained for reader
        admin.namespaces().setRetention("my-property/use/my-ns",
                new RetentionPolicies(-1, -1));

        Producer<ServiceUnitStateData> producer = pulsarClient.newProducer(schema)
                .topic(topic)
                .compressionType(MSG_COMPRESSION_TYPE)
                .enableBatching(true)
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .create();

        StrategicTwoPhaseCompactor compactor
                = new StrategicTwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);

        var reader = ((CompletableFuture<ReaderImpl<ServiceUnitStateData>>) FieldUtils
                .readDeclaredField(tv, "reader", true)).get();
        var consumer = spy(reader.getConsumer());
        FieldUtils.writeDeclaredField(reader, "consumer", consumer, true);
        String bundle = "bundle1";
        final AtomicInteger versionId = new AtomicInteger(0);
        final AtomicInteger cnt = new AtomicInteger(1);
        int msgAddCount = 1000; // has to be big enough to cover compacted cursor fast-forward.
        doAnswer(invocationOnMock -> {
            if (cnt.decrementAndGet() == 0) {
                var msg = consumer.receiveAsync();
                for (int i = 0; i < msgAddCount; i++) {
                    producer.newMessage().key(bundle).value(
                            new ServiceUnitStateData(Owned, "broker" + versionId.incrementAndGet(), true,
                                    versionId.get())).send();
                }
                compactor.compact(topic, strategy).join();
                return msg;
            }
            // Call the real method
            reset(consumer);
            return consumer.receiveAsync();
        }).when(consumer).receiveAsync();
        producer.newMessage().key(bundle).value(
                new ServiceUnitStateData(Owned, "broker", true,
                        versionId.incrementAndGet())).send();
        producer.newMessage().key(bundle).value(
                new ServiceUnitStateData(Owned, "broker" + versionId.incrementAndGet(), true,
                        versionId.get())).send();
        Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(
                () -> {
                    var val = tv.get(bundle);
                    assertNotNull(val);
                    assertEquals(val.dstBroker(), "broker" + versionId.get());
                }
        );

        producer.close();
        tv.close();
    }

    @Test
    public void testBrokerRestartAfterCompaction() throws Exception {
        String topic = "persistent://my-property/use/my-ns/my-topic1";

        Producer<ServiceUnitStateData> producer = pulsarClient.newProducer(schema)
                .topic(topic)
                .compressionType(MSG_COMPRESSION_TYPE)
                .enableBatching(true)
                .create();
        String key = "key0";
        pulsarClient.newConsumer(schema).topic(topic).subscriptionName("sub1").readCompacted(true).subscribe().close();

        var testValues =  Arrays.asList(
                testValue("content0"), testValue("content1"), testValue("content2"));
        for (var val : testValues) {
            producer.newMessage().key(key).value(val).send();
        }
        StrategicTwoPhaseCompactor compactor
                = new StrategicTwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic, strategy).get();

        try (Consumer<ServiceUnitStateData> consumer = pulsarClient.newConsumer(schema).topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscribe()) {
            Message<ServiceUnitStateData> m = consumer.receive();
            Assert.assertEquals(m.getKey(), key);
            Assert.assertEquals(m.getValue(), testValues.get(testValues.size() - 1));
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
            Assert.assertEquals(m.getKey(), key);
            Assert.assertEquals(m.getValue(), testValues.get(testValues.size() - 1));
        }
    }

    @Test
    public void testCompactEmptyTopic() throws Exception {
        String topic = "persistent://my-property/use/my-ns/my-topic1";

        Producer<ServiceUnitStateData> producer = pulsarClient.newProducer(schema)
                .topic(topic)
                .compressionType(MSG_COMPRESSION_TYPE)
                .enableBatching(true)
                .create();

        pulsarClient.newConsumer(schema).topic(topic).subscriptionName("sub1").readCompacted(true).subscribe().close();

        StrategicTwoPhaseCompactor compactor
                = new StrategicTwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic, strategy).get();

        var testValue =  testValue( "content0");
        producer.newMessage().key("key0").value(testValue).send();

        try (Consumer<ServiceUnitStateData> consumer = pulsarClient.newConsumer(schema).topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscribe()) {
            Message<ServiceUnitStateData> m = consumer.receive();
            Assert.assertEquals(m.getKey(), "key0");
            Assert.assertEquals(m.getValue(), testValue);
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
            producerBatch.newMessage().key("key1").value(testValue("my-message-1")).sendAsync();
            producerBatch.newMessage().key("key1").value(testValue( "my-message-2")).sendAsync();
            producerBatch.newMessage().key("key1").value(testValue("my-message-3")).sendAsync();
            producerNormal.newMessage().key("key1").value(testValue( "my-message-4")).send();
        }

        // compact the topic
        StrategicTwoPhaseCompactor compactor
                = new StrategicTwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic, strategy).get();

        try (Consumer<ServiceUnitStateData> consumer = pulsarClient.newConsumer(schema).topic(topic)
                .subscriptionName("sub1").readCompacted(true).subscribe()) {
            Message<ServiceUnitStateData> message = consumer.receive();
            Assert.assertEquals(message.getKey(), "key1");
            Assert.assertEquals(new String(message.getValue().dstBroker()), "my-message-4");
        }
    }

    public void testCompactionWithLastDeletedKey() throws Exception {
        String topic = "persistent://my-property/use/my-ns/my-topic1";

        Producer<ServiceUnitStateData> producer = pulsarClient.newProducer(schema).topic(topic)
                .compressionType(MSG_COMPRESSION_TYPE)
                .enableBatching(true)
                .messageRoutingMode(MessageRoutingMode.SinglePartition).create();

        pulsarClient.newConsumer(schema).topic(topic).subscriptionName("sub1").readCompacted(true).subscribe().close();

        producer.newMessage().key("1").value(testValue("1")).send();
        producer.newMessage().key("2").value(testValue("3")).send();
        producer.newMessage().key("3").value(testValue( "5")).send();
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

        Producer<ServiceUnitStateData> producer = pulsarClient.newProducer(schema).topic(topic)
                .compressionType(MSG_COMPRESSION_TYPE)
                .enableBatching(true)
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
        ProducerBuilder<ServiceUnitStateData> builder = pulsarClient.newProducer(schema)
                .compressionType(MSG_COMPRESSION_TYPE).topic(topic);
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
        builder.compressionType(MSG_COMPRESSION_TYPE);
        builder.enableBatching(true);


        Producer<ServiceUnitStateData> producer = builder.create();

        List<CompletableFuture<MessageId>> futures = new ArrayList<>(messages);
        for (int i = 0; i < messages; i++) {
            futures.add(producer.newMessage().key(key).value(testValue((i + ""))).sendAsync());
        }

        FutureUtil.waitForAll(futures).get();

        // 2.compact the topic.
        StrategicTwoPhaseCompactor compactor
                = new StrategicTwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic, strategy).get();

        // 3. Send more ten messages
        futures.clear();
        for (int i = 0; i < messages; i++) {
            futures.add(producer.newMessage().key(key).value(testValue((i + 10 + ""))).sendAsync());
        }
        FutureUtil.waitForAll(futures).get();

        // 4.compact again.
        compactor.compact(topic, strategy).get();

        try (Consumer<ServiceUnitStateData> consumer = pulsarClient.newConsumer(schema).topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscriptionInitialPosition(SubscriptionInitialPosition.Earliest).subscribe()) {
            Message<ServiceUnitStateData> m1 = consumer.receive();
            assertNotNull(m1);
            assertEquals(m1.getKey(), key);
            assertEquals(m1.getValue().dstBroker(), "19");
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
        builder.compressionType(MSG_COMPRESSION_TYPE);
        builder.batchingMaxMessages(messages / 5);

        Producer<ServiceUnitStateData> producer = builder.create();

        List<CompletableFuture<MessageId>> futures = new ArrayList<>(messages);
        for (int i = 0; i < messages; i++) {
            futures.add(producer.newMessage().key(key).value(testValue((i + ""))).sendAsync());
        }

        FutureUtil.waitForAll(futures).get();

        // 2.compact the topic.
        StrategicTwoPhaseCompactor compactor
                = new StrategicTwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic, strategy).get();

        // 3. Send more ten messages
        futures.clear();
        for (int i = 0; i < messages; i++) {
            futures.add(producer.newMessage().key(key).value(testValue((i + 10 + ""))).sendAsync());
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
                assertEquals(received.getValue().dstBroker(), i + 9 + "");
                consumer.acknowledge(received);
            }
            Message<ServiceUnitStateData> none = consumer.receive(2, TimeUnit.SECONDS);
            assertNull(none);
        }

        // 4.Send empty message to delete the key-value in the compacted topic.
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
            futures.add(producer.newMessage().key(key).value(testValue((i + 20 + ""))).sendAsync());
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
                assertEquals(received.getValue().dstBroker(), i + 20 + "");
                consumer.acknowledge(received);
            }
            Message<ServiceUnitStateData> none = consumer.receive(2, TimeUnit.SECONDS);
            assertNull(none);
        }
    }

    public static long versionId(ServiceUnitStateData data) {
        return data == null ? ServiceUnitStateChannelImpl.VERSION_ID_INIT - 1 : data.versionId();
    }
}
