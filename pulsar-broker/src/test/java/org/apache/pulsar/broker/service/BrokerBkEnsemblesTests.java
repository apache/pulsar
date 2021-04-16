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

import static org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest.retryStrategically;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import java.lang.reflect.Field;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import lombok.Cleanup;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedLedgerInfo.LedgerInfo;
import org.apache.bookkeeper.util.StringUtils;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.zookeeper.ZooKeeper;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class BrokerBkEnsemblesTests extends BkEnsemblesTestBase {

    public BrokerBkEnsemblesTests() {
        this(3);
    }

    public BrokerBkEnsemblesTests(int numberOfBookies) {
        super(numberOfBookies);
    }

    /**
     * It verifies that broker deletes cursor-ledger when broker-crashes without closing topic gracefully
     *
     * <pre>
     * 1. Create topic : publish/consume-ack msgs to update new cursor-ledger
     * 2. Verify cursor-ledger is created and ledger-znode present
     * 3. Broker crashes: remove topic and managed-ledgers without closing
     * 4. Recreate topic: publish/consume-ack msgs to update new cursor-ledger
     * 5. Topic is recovered from old-ledger and broker deletes the old ledger
     * 6. verify znode of old-ledger is deleted
     * </pre>
     *
     * @throws Exception
     */
    @Test
    public void testCrashBrokerWithoutCursorLedgerLeak() throws Exception {

        ZooKeeper zk = bkEnsemble.getZkClient();
        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsar.getWebServiceAddress())
                .statsInterval(0, TimeUnit.SECONDS)
                .build();

        final String ns1 = "prop/usc/crash-broker";

        admin.namespaces().createNamespace(ns1);

        final String topic1 = "persistent://" + ns1 + "/my-topic";

        // (1) create topic
        // publish and ack messages so, cursor can create cursor-ledger and update metadata
        Consumer<byte[]> consumer = client.newConsumer().topic(topic1).subscriptionName("my-subscriber-name")
                .subscribe();
        Producer<byte[]> producer = client.newProducer().topic(topic1).create();
        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }
        Message<byte[]> msg = null;
        for (int i = 0; i < 10; i++) {
            msg = consumer.receive(1, TimeUnit.SECONDS);
            consumer.acknowledge(msg);
        }

        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getOrCreateTopic(topic1).get();
        ManagedCursorImpl cursor = (ManagedCursorImpl) topic.getManagedLedger().getCursors().iterator().next();
        retryStrategically((test) -> cursor.getState().equals("Open"), 5, 100);

        // (2) validate cursor ledger is created and znode is present
        long cursorLedgerId = cursor.getCursorLedger();
        String ledgerPath = "/ledgers" + StringUtils.getHybridHierarchicalLedgerPath(cursorLedgerId);
        Assert.assertNotNull(zk.exists(ledgerPath, false));

        // (3) remove topic and managed-ledger from broker which means topic is not closed gracefully
        consumer.close();
        producer.close();
        pulsar.getBrokerService().removeTopicFromCache(topic1);
        ManagedLedgerFactoryImpl factory = (ManagedLedgerFactoryImpl) pulsar.getManagedLedgerFactory();
        Field field = ManagedLedgerFactoryImpl.class.getDeclaredField("ledgers");
        field.setAccessible(true);
        @SuppressWarnings("unchecked")
        ConcurrentHashMap<String, CompletableFuture<ManagedLedgerImpl>> ledgers = (ConcurrentHashMap<String, CompletableFuture<ManagedLedgerImpl>>) field
                .get(factory);
        ledgers.clear();

        // (4) Recreate topic
        // publish and ack messages so, cursor can create cursor-ledger and update metadata
        consumer = client.newConsumer().topic(topic1).subscriptionName("my-subscriber-name").subscribe();
        producer = client.newProducer().topic(topic1).create();
        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }
        for (int i = 0; i < 10; i++) {
            msg = consumer.receive(1, TimeUnit.SECONDS);
            consumer.acknowledge(msg);
        }

        // (5) Broker should create new cursor-ledger and remove old cursor-ledger
        topic = (PersistentTopic) pulsar.getBrokerService().getOrCreateTopic(topic1).get();
        final ManagedCursorImpl cursor1 = (ManagedCursorImpl) topic.getManagedLedger().getCursors().iterator().next();
        retryStrategically((test) -> cursor1.getState().equals("Open"), 5, 100);
        long newCursorLedgerId = cursor1.getCursorLedger();
        Assert.assertNotEquals(newCursorLedgerId, -1);
        Assert.assertNotEquals(cursorLedgerId, newCursorLedgerId);

        // cursor node must be deleted
        Assert.assertNull(zk.exists(ledgerPath, false));

        producer.close();
        consumer.close();
    }

    /**
     * It verifies broker-configuration using which broker can skip non-recoverable data-ledgers.
     *
     * <pre>
     * 1. publish messages in 5 data-ledgers each with 20 entries under managed-ledger
     * 2. delete first 4 data-ledgers
     * 3. consumer will fail to consume any message as first data-ledger is non-recoverable
     * 4. enable dynamic config to skip non-recoverable data-ledgers
     * 5. consumer will be able to consume 20 messages from last non-deleted ledger
     *
     * </pre>
     *
     * @throws Exception
     */
    @Test
    public void testSkipCorruptDataLedger() throws Exception {
        // Ensure intended state for autoSkipNonRecoverableData
        admin.brokers().updateDynamicConfiguration("autoSkipNonRecoverableData", "false");

        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsar.getWebServiceAddress())
                .statsInterval(0, TimeUnit.SECONDS)
                .build();

        final String ns1 = "prop/usc/crash-broker";
        final int totalMessages = 100;
        final int totalDataLedgers = 5;
        final int entriesPerLedger = totalMessages / totalDataLedgers;

        try {
            admin.namespaces().createNamespace(ns1);
        } catch (Exception e) {

        }

        final String topic1 = "persistent://" + ns1 + "/my-topic-" + System.currentTimeMillis();

        // Create subscription
        Consumer<byte[]> consumer = client.newConsumer().topic(topic1).subscriptionName("my-subscriber-name")
                .receiverQueueSize(5).subscribe();

        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getOrCreateTopic(topic1).get();
        ManagedLedgerImpl ml = (ManagedLedgerImpl) topic.getManagedLedger();
        ManagedCursorImpl cursor = (ManagedCursorImpl) ml.getCursors().iterator().next();
        Field configField = ManagedCursorImpl.class.getDeclaredField("config");
        configField.setAccessible(true);
        // Create multiple data-ledger
        ManagedLedgerConfig config = (ManagedLedgerConfig) configField.get(cursor);
        config.setMaxEntriesPerLedger(entriesPerLedger);
        config.setMinimumRolloverTime(1, TimeUnit.MILLISECONDS);
        // bookkeeper client
        Field bookKeeperField = ManagedLedgerImpl.class.getDeclaredField("bookKeeper");
        bookKeeperField.setAccessible(true);
        // Create multiple data-ledger
        BookKeeper bookKeeper = (BookKeeper) bookKeeperField.get(ml);

        // (1) publish messages in 5 data-ledgers each with 20 entries under managed-ledger
        Producer<byte[]> producer = client.newProducer().topic(topic1).create();
        for (int i = 0; i < totalMessages; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }

        // validate: consumer is able to consume msg and close consumer after reading 1 entry
        Assert.assertNotNull(consumer.receive(1, TimeUnit.SECONDS));
        consumer.close();

        NavigableMap<Long, LedgerInfo> ledgerInfo = ml.getLedgersInfo();
        Assert.assertEquals(ledgerInfo.size(), totalDataLedgers);
        Entry<Long, LedgerInfo> lastLedger = ledgerInfo.lastEntry();

        // (2) delete first 4 data-ledgers
        ledgerInfo.entrySet().forEach(entry -> {
            if (!entry.equals(lastLedger)) {
                assertEquals(entry.getValue().getEntries(), entriesPerLedger);
                try {
                    bookKeeper.deleteLedger(entry.getKey());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        // clean managed-ledger and recreate topic to clean any data from the cache
        producer.close();
        pulsar.getBrokerService().removeTopicFromCache(topic1);
        ManagedLedgerFactoryImpl factory = (ManagedLedgerFactoryImpl) pulsar.getManagedLedgerFactory();
        Field field = ManagedLedgerFactoryImpl.class.getDeclaredField("ledgers");
        field.setAccessible(true);
        @SuppressWarnings("unchecked")
        ConcurrentHashMap<String, CompletableFuture<ManagedLedgerImpl>> ledgers = (ConcurrentHashMap<String, CompletableFuture<ManagedLedgerImpl>>) field
                .get(factory);
        ledgers.clear();

        // (3) consumer will fail to consume any message as first data-ledger is non-recoverable
        Message<byte[]> msg = null;
        // start consuming message
        consumer = client.newConsumer().topic(topic1).subscriptionName("my-subscriber-name").subscribe();
        msg = consumer.receive(1, TimeUnit.SECONDS);
        Assert.assertNull(msg);
        consumer.close();

        // (4) enable dynamic config to skip non-recoverable data-ledgers
        admin.brokers().updateDynamicConfiguration("autoSkipNonRecoverableData", "true");

        retryStrategically((test) -> config.isAutoSkipNonRecoverableData(), 5, 100);

        // (5) consumer will be able to consume 20 messages from last non-deleted ledger
        consumer = client.newConsumer().topic(topic1).subscriptionName("my-subscriber-name").subscribe();
        for (int i = 0; i < entriesPerLedger; i++) {
            msg = consumer.receive();
            System.out.println(i);
            consumer.acknowledge(msg);
        }

        producer.close();
        consumer.close();
    }

    @Test(timeOut = 20000)
    public void testTopicWithWildCardChar() throws Exception {
        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsar.getWebServiceAddress())
                .statsInterval(0, TimeUnit.SECONDS)
                .build();

        final String ns1 = "prop/usc/topicWithSpecialChar";
        try {
            admin.namespaces().createNamespace(ns1);
        } catch (Exception e) {

        }

        final String topic1 = "persistent://"+ns1+"/`~!@#$%^&*()-_+=[]://{}|\\;:'\"<>,./?-30e04524";
        final String subName1 = "c1";
        final byte[] content = "test".getBytes();

        Consumer<byte[]> consumer = client.newConsumer().topic(topic1).subscriptionName(subName1).subscribe();
        org.apache.pulsar.client.api.Producer<byte[]> producer = client.newProducer().topic(topic1).create();

        producer.send(content);
        Message<byte[]> msg = consumer.receive();
        Assert.assertEquals(msg.getData(), content);
        consumer.close();
        producer.close();
    }


    @Test
    public void testDeleteTopicWithMissingData() throws Exception {
        String namespace = BrokerTestUtil.newUniqueName("prop/usc");
        admin.namespaces().createNamespace(namespace);

        String topic = BrokerTestUtil.newUniqueName(namespace + "/my-topic");

        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsar.getBrokerServiceUrl())
                .statsInterval(0, TimeUnit.SECONDS)
                .build();

        @Cleanup
        Consumer<String> consumer = client.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("test").subscribe();

        @Cleanup
        Producer<String> producer = client.newProducer(Schema.STRING)
                .topic(topic)
                .create();

        producer.send("Hello");

        // Stop all bookies, to get all data offline
        bkEnsemble.stopBK();

        // Unload the topic. Since all the bookies are down, the recovery
        // will fail and the topic will not get reloaded.
        admin.topics().unload(topic);

        Thread.sleep(1000);

        CompletableFuture<Optional<Topic>> future = pulsar.getBrokerService().getTopicIfExists(topic);
        try {
            future.get();
            fail("Should have thrown exception");
        } catch (ExecutionException e) {
            // Expected
        }

        // Deletion must succeed
        admin.topics().delete(topic);

        // Topic will not be there after
        assertEquals(pulsar.getBrokerService().getTopicIfExists(topic).join(), Optional.empty());
    }

}
