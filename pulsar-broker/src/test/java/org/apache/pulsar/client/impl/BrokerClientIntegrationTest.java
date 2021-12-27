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
package org.apache.pulsar.client.impl;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.UUID.randomUUID;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Cleanup;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.PulsarMockBookKeeper;
import org.apache.bookkeeper.client.PulsarMockLedgerHandle;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.namespace.OwnershipCache;
import org.apache.pulsar.broker.resources.BaseResources;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.api.schema.SchemaReader;
import org.apache.pulsar.client.api.schema.SchemaWriter;
import org.apache.pulsar.client.impl.HandlerState.State;
import org.apache.pulsar.client.impl.schema.SchemaDefinitionBuilderImpl;
import org.apache.pulsar.client.impl.schema.reader.JacksonJsonReader;
import org.apache.pulsar.client.impl.schema.writer.JacksonJsonWriter;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.protocol.PulsarHandler;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.collections.ConcurrentLongHashMap;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker-impl")
public class BrokerClientIntegrationTest extends ProducerConsumerBase {
    private static final Logger log = LoggerFactory.getLogger(BrokerClientIntegrationTest.class);

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @DataProvider
    public Object[][] subType() {
        return new Object[][] { { SubscriptionType.Shared }, { SubscriptionType.Failover } };
    }

    @DataProvider(name = "booleanFlagProvider")
    public Object[][] booleanFlagProvider() {
        return new Object[][] { { true }, { false } };
    }

    /**
     * Verifies unload namespace-bundle doesn't close shared connection used by other namespace-bundle.
     *
     * <pre>
     * 1. after disabling broker fron loadbalancer
     * 2. unload namespace-bundle "my-ns1" which disconnects client (producer/consumer) connected on that namespacebundle
     * 3. but doesn't close the connection for namesapce-bundle "my-ns2" and clients are still connected
     * 4. verifies unloaded "my-ns1" should not connected again with the broker as broker is disabled
     * 5. unload "my-ns2" which closes the connection as broker doesn't have any more client connected on that connection
     * 6. all namespace-bundles are in "connecting" state and waiting for available broker
     * </pre>
     *
     * @throws Exception
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    public void testDisconnectClientWithoutClosingConnection() throws Exception {

        final String ns1 = "my-property/con-ns1";
        final String ns2 = "my-property/con-ns2";
        admin.namespaces().createNamespace(ns1, Sets.newHashSet("test"));
        admin.namespaces().createNamespace(ns2, Sets.newHashSet("test"));

        final String topic1 = "persistent://" + ns1 + "/my-topic";
        final String topic2 = "persistent://" + ns2 + "/my-topic";
        ConsumerImpl<byte[]> cons1 = (ConsumerImpl<byte[]>) pulsarClient.newConsumer().topic(topic1)
                .subscriptionName("my-subscriber-name").subscribe();

        ProducerImpl<byte[]> prod1 = (ProducerImpl<byte[]>) pulsarClient.newProducer().topic(topic1).create();
        ProducerImpl<byte[]> prod2 = (ProducerImpl<byte[]>) pulsarClient.newProducer().topic(topic2).create();
        ConsumerImpl<byte[]> consumer1 = spy(cons1);
        doAnswer(invocationOnMock -> cons1.getState()).when(consumer1).getState();
        doAnswer(invocationOnMock -> cons1.getClientCnx()).when(consumer1).getClientCnx();
        doAnswer(invocationOnMock -> cons1.cnx()).when(consumer1).cnx();
        doAnswer(invocationOnMock -> {
            cons1.connectionClosed((ClientCnx) invocationOnMock.getArguments()[0]);
            return null;
        }).when(consumer1).connectionClosed(any());
        ProducerImpl<byte[]> producer1 = spy(prod1);
        doAnswer(invocationOnMock -> prod1.getState()).when(producer1).getState();
        doAnswer(invocationOnMock -> prod1.getClientCnx()).when(producer1).getClientCnx();
        doAnswer(invocationOnMock -> prod1.cnx()).when(producer1).cnx();
        doAnswer(invocationOnMock -> {
            prod1.connectionClosed((ClientCnx) invocationOnMock.getArguments()[0]);
            return null;
        }).when(producer1).connectionClosed(any());
        ProducerImpl<byte[]> producer2 = spy(prod2);
        doAnswer(invocationOnMock -> prod2.getState()).when(producer2).getState();
        doAnswer(invocationOnMock -> prod2.getClientCnx()).when(producer2).getClientCnx();
        doAnswer(invocationOnMock -> prod2.cnx()).when(producer2).cnx();
        doAnswer(invocationOnMock -> {
            prod2.connectionClosed((ClientCnx) invocationOnMock.getArguments()[0]);
            return null;
        }).when(producer2).connectionClosed(any());

        ClientCnx clientCnx = producer1.getClientCnx();

        Field pfield = ClientCnx.class.getDeclaredField("producers");
        pfield.setAccessible(true);
        Field cfield = ClientCnx.class.getDeclaredField("consumers");
        cfield.setAccessible(true);

        ConcurrentLongHashMap<ProducerImpl<byte[]>> producers = (ConcurrentLongHashMap) pfield.get(clientCnx);
        ConcurrentLongHashMap<ConsumerImpl<byte[]>> consumers = (ConcurrentLongHashMap) cfield.get(clientCnx);

        producers.put(2, producers.get(0));
        producers.put(3, producers.get(1));
        consumers.put(1, consumers.get(0));

        producers.put(0, producer1);
        producers.put(1, producer2);
        consumers.put(0, consumer1);

        // disable this broker to avoid any new requests
        pulsar.getLoadManager().get().disableBroker();

        NamespaceBundle bundle1 = pulsar.getNamespaceService().getBundle(TopicName.get(topic1));
        NamespaceBundle bundle2 = pulsar.getNamespaceService().getBundle(TopicName.get(topic2));

        // unload ns-bundle:1
        pulsar.getNamespaceService().unloadNamespaceBundle((NamespaceBundle) bundle1).join();
        // let server send signal to close-connection and client close the connection
        Thread.sleep(1000);
        // [1] Verify: producer1 must get connectionClosed signal
        verify(producer1, atLeastOnce()).connectionClosed(any());
        // [2] Verify: consumer1 must get connectionClosed signal
        verify(consumer1, atLeastOnce()).connectionClosed(any());
        // [3] Verify: producer2 should have not received connectionClosed signal
        verify(producer2, never()).connectionClosed(any());

        // sleep for sometime to let other disconnected producer and consumer connect again: but they should not get
        // connected with same broker as that broker is already out from active-broker list
        Thread.sleep(200);

        // producer1 must not be able to connect again
        assertNull(prod1.getClientCnx());
        assertEquals(State.Connecting, prod1.getState());
        // consumer1 must not be able to connect again
        assertNull(cons1.getClientCnx());
        assertEquals(State.Connecting, cons1.getState());
        // producer2 must have live connection
        assertNotNull(prod2.getClientCnx());
        assertEquals(State.Ready, prod2.getState());

        // unload ns-bundle2 as well
        pulsar.getNamespaceService().unloadNamespaceBundle((NamespaceBundle) bundle2).join();
        // let producer2 give some time to get disconnect signal and get disconnected
        Thread.sleep(200);
        verify(producer2, atLeastOnce()).connectionClosed(any());

        // producer1 must not be able to connect again
        assertNull(prod1.getClientCnx());
        assertEquals(State.Connecting, prod1.getState());
        // consumer1 must not be able to connect again
        assertNull(cons1.getClientCnx());
        assertEquals(State.Connecting, cons1.getState());
        // producer2 must not be able to connect again
        assertNull(prod2.getClientCnx());
        assertEquals(State.Connecting, prod2.getState());

        producer1.close();
        producer2.close();
        consumer1.close();
        prod1.close();
        prod2.close();
        cons1.close();
    }

    /**
     * Verifies: 1. Closing of Broker service unloads all bundle gracefully and there must not be any connected bundles
     * after closing broker service
     *
     * @throws Exception
     */
    @Test
    public void testCloseBrokerService() throws Exception {

        final String ns1 = "my-property/brok-ns1";
        final String ns2 = "my-property/brok-ns2";
        admin.namespaces().createNamespace(ns1, Sets.newHashSet("test"));
        admin.namespaces().createNamespace(ns2, Sets.newHashSet("test"));

        final String topic1 = "persistent://" + ns1 + "/my-topic";
        final String topic2 = "persistent://" + ns2 + "/my-topic";

        ConsumerImpl<byte[]> consumer1 = (ConsumerImpl<byte[]>) pulsarClient.newConsumer().topic(topic1)
                .subscriptionName("my-subscriber-name").subscribe();
        ProducerImpl<byte[]> producer1 = (ProducerImpl<byte[]>) pulsarClient.newProducer().topic(topic1).create();
        ProducerImpl<byte[]> producer2 = (ProducerImpl<byte[]>) pulsarClient.newProducer().topic(topic2).create();

        // unload all other namespace
        pulsar.getBrokerService().close();

        // [1] OwnershipCache should not contain any more namespaces
        OwnershipCache ownershipCache = pulsar.getNamespaceService().getOwnershipCache();
        assertTrue(ownershipCache.getOwnedBundles().keySet().isEmpty());
        // Strategical retry
        retryStrategically((test) -> (producer1.getClientCnx() == null && consumer1.getClientCnx() == null
                && producer2.getClientCnx() == null), 5, 100);
        // [2] All clients must be disconnected and in connecting state
        // producer1 must not be able to connect again
        assertNull(producer1.getClientCnx());
        assertEquals(State.Connecting, producer1.getState());
        // consumer1 must not be able to connect again
        assertNull(consumer1.getClientCnx());
        assertEquals(State.Connecting, consumer1.getState());
        // producer2 must not be able to connect again
        assertNull(producer2.getClientCnx());
        assertEquals(State.Connecting, producer2.getState());

        producer1.close();
        producer2.close();
        consumer1.close();

    }

    /**
     * It verifies that consumer which doesn't support batch-message:
     * <p>
     * 1. broker disconnects that consumer
     * <p>
     * 2. redeliver all those messages to other supported consumer under the same subscription
     *
     * @param subType
     * @throws Exception
     */
    @Test(dataProvider = "subType")
    public void testUnsupportedBatchMessageConsumer(SubscriptionType subType) throws Exception {
        log.info("-- Starting {} test --", methodName);

        final String topicName = "persistent://my-property/my-ns/my-topic1";
        final String subscriptionName = "my-subscriber-name" + subType;

        ConsumerImpl<byte[]> consumer1 = (ConsumerImpl<byte[]>) pulsarClient.newConsumer()
                .topic(topicName)
                .subscriptionName(subscriptionName)
                .subscriptionType(subType)
                .subscribe();

        final int numMessagesPerBatch = 10;

        Producer<byte[]> producer = pulsarClient.newProducer()
                    .topic(topicName)
                    .enableBatching(false)
                    .create();
        Producer<byte[]> batchProducer = pulsarClient.newProducer()
                .topic(topicName).enableBatching(true)
                .batchingMaxPublishDelay(Long.MAX_VALUE, TimeUnit.SECONDS)
                .batchingMaxMessages(numMessagesPerBatch)
                .create();

        // update consumer's version to incompatible batch-message version = Version.V3
        Topic topic = pulsar.getBrokerService().getOrCreateTopic(topicName).get();
        org.apache.pulsar.broker.service.Consumer brokerConsumer = topic.getSubscriptions().get(subscriptionName)
                .getConsumers().get(0);
        Field cnxField = org.apache.pulsar.broker.service.Consumer.class.getDeclaredField("cnx");
        cnxField.setAccessible(true);
        PulsarHandler cnx = (PulsarHandler) cnxField.get(brokerConsumer);
        Field versionField = PulsarHandler.class.getDeclaredField("remoteEndpointProtocolVersion");
        versionField.setAccessible(true);
        versionField.set(cnx, 3);

        // (1) send non-batch message: consumer should be able to consume
        MessageId lastNonBatchedMessageId = null;
        for (int i = 0; i < numMessagesPerBatch; i++) {
            String message = "my-message-" + i;
            lastNonBatchedMessageId = producer.send(message.getBytes());
        }
        Set<String> messageSet = Sets.newHashSet();
        Message<byte[]> msg = null;
        for (int i = 0; i < numMessagesPerBatch; i++) {
            msg = consumer1.receive(1, TimeUnit.SECONDS);
            String receivedMessage = new String(msg.getData());
            String expectedMessage = "my-message-" + i;
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
            consumer1.acknowledge(msg);
        }

        // Also set clientCnx of the consumer to null so, it avoid reconnection so, other consumer can consume for
        // verification
        consumer1.setClientCnx(null);
        // (2) send batch-message which should not be able to consume: as broker will disconnect the consumer
        for (int i = 0; i < numMessagesPerBatch; i++) {
            String message = "my-batch-message-" + i;
            batchProducer.sendAsync(message.getBytes());
        }
        batchProducer.flush();

        // consumer should have not received any message as it should have been disconnected
        msg = consumer1.receive(100, TimeUnit.MILLISECONDS);
        assertNull(msg);

        // subscribe consumer2 with supporting batch version
        @Cleanup
        PulsarClient newPulsarClient = newPulsarClient(lookupUrl.toString(), 0); // Creates new client connection
        Consumer<byte[]> consumer2 = newPulsarClient.newConsumer()
                .topic(topicName)
                .subscriptionName(subscriptionName)
                .subscriptionType(subType)
                .subscribe();
        consumer2.seek(lastNonBatchedMessageId);

        messageSet.clear();
        for (int i = 0; i < numMessagesPerBatch; i++) {
            msg = consumer2.receive();
            String receivedMessage = new String(msg.getData());
            log.debug("Received message: [{}]", receivedMessage);
            String expectedMessage = "my-batch-message-" + i;
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
            consumer2.acknowledge(msg);
        }

        consumer2.close();
        producer.close();
        batchProducer.close();
        log.info("-- Exiting {} test --", methodName);
    }

    @Test(dataProvider = "subType")
    public void testResetCursor(SubscriptionType subType) throws Exception {
        final RetentionPolicies policy = new RetentionPolicies(60, 52 * 1024);
        final TopicName topicName = TopicName.get("persistent://my-property/my-ns/unacked-topic");
        final int warmup = 20;
        final int testSize = 150;
        final List<Message<byte[]>> received = new ArrayList<>();
        final String subsId = "sub";

        final NavigableMap<Long, TimestampEntryCount> publishTimeIdMap = new ConcurrentSkipListMap<>();

        // set delay time to start dispatching messages to active consumer in order to avoid message duplication
        conf.setActiveConsumerFailoverDelayTimeMillis(500);
        restartBroker();

        admin.namespaces().setRetention(topicName.getNamespace(), policy);

        ConsumerBuilder<byte[]> consumerBuilder = pulsarClient.newConsumer().topic(topicName.toString())
                .startMessageIdInclusive()
                .subscriptionName(subsId).subscriptionType(subType).messageListener((consumer, msg) -> {
                    try {
                        synchronized (received) {
                            received.add(msg);
                        }
                        consumer.acknowledge(msg);
                        long publishTime = msg.getPublishTime();
                        log.info(" publish time is " + publishTime + "," + msg.getMessageId());
                        TimestampEntryCount timestampEntryCount = publishTimeIdMap.computeIfAbsent(publishTime,
                                (k) -> new TimestampEntryCount(publishTime));
                        timestampEntryCount.incrementAndGet();
                    } catch (final PulsarClientException e) {
                        log.warn("Failed to ack!");
                    }
                });
        Consumer<byte[]> consumer1 = consumerBuilder.subscribe();
        Consumer<byte[]> consumer2 = consumerBuilder.subscribe();
        final Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName.toString()).create();

        log.info("warm up started for " + topicName.toString());
        // send warmup msgs
        byte[] msgBytes = new byte[1000];
        for (int i = 0; i < warmup; i++) {
            producer.send(msgBytes);
        }
        log.info("warm up finished.");

        // sleep to ensure receiving of msgs
        for (int n = 0; n < 10 && received.size() < warmup; n++) {
            Thread.sleep(200);
        }

        // validate received msgs
        Assert.assertEquals(received.size(), warmup);
        received.clear();

        // publish testSize num of msgs
        log.info("Sending more messages.");
        for (int n = 0; n < testSize; n++) {
            producer.send(msgBytes);
            Thread.sleep(1);
        }
        log.info("Sending more messages done.");

        Thread.sleep(3000);

        long begints = publishTimeIdMap.firstEntry().getKey();
        long endts = publishTimeIdMap.lastEntry().getKey();
        // find reset timestamp
        long timestamp = (endts - begints) / 2 + begints;
        timestamp = publishTimeIdMap.floorKey(timestamp);

        NavigableMap<Long, TimestampEntryCount> expectedMessages = new ConcurrentSkipListMap<>();
        expectedMessages.putAll(publishTimeIdMap.tailMap(timestamp, true));

        received.clear();

        log.info("reset cursor to " + timestamp + " for topic " + topicName.toString() + " for subs " + subsId);
        log.info("issuing admin operation on " + admin.getServiceUrl());
        List<String> subList = admin.topics().getSubscriptions(topicName.toString());
        for (String subs : subList) {
            log.info("got sub " + subs);
        }
        publishTimeIdMap.clear();
        // reset the cursor to this timestamp
        Assert.assertTrue(subList.contains(subsId));
        admin.topics().resetCursor(topicName.toString(), subsId, timestamp);

        Thread.sleep(3000);
        int totalExpected = 0;
        for (TimestampEntryCount tec : expectedMessages.values()) {
            totalExpected += tec.numMessages;
        }
        // validate that replay happens after the timestamp
        Assert.assertTrue(publishTimeIdMap.firstEntry().getKey() >= timestamp);
        consumer1.close();
        consumer2.close();
        producer.close();
        // validate that expected and received counts match
        int totalReceived = 0;
        for (TimestampEntryCount tec : publishTimeIdMap.values()) {
            totalReceived += tec.numMessages;
        }
        Assert.assertEquals(totalReceived, totalExpected, "did not receive all messages on replay after reset");

        resetConfig();
        restartBroker();
    }

    /**
     * It verifies that broker throttles down configured concurrent topic loading requests
     *
     * <pre>
     * 1. Start broker with N maxConcurrentTopicLoadRequest
     * 2. create concurrent producers on different topics which makes broker to load topics concurrently
     * 3. Producer operationtimeout = 1 ms so, if producers creation will fail for throttled topics
     * 4. verify all producers should have connected
     * </pre>
     *
     * @throws Exception
     */
    @Test
    public void testMaxConcurrentTopicLoading() throws Exception {
        final String topicName = "persistent://prop/usw/my-ns/cocurrentLoadingTopic";
        int concurrentTopic = pulsar.getConfiguration().getMaxConcurrentTopicLoadRequest();
        final int concurrentLookupRequests = 20;
        @Cleanup("shutdownNow")
        ExecutorService executor = Executors.newFixedThreadPool(concurrentLookupRequests);

        try {
            pulsar.getConfiguration().setAuthorizationEnabled(false);
            stopBroker();
            startBroker();
            pulsar.getConfiguration().setMaxConcurrentTopicLoadRequest(1);
            String lookupUrl = pulsar.getBrokerServiceUrl();

            try (PulsarClientImpl pulsarClient = (PulsarClientImpl) PulsarClient.builder().serviceUrl(lookupUrl)
                    .statsInterval(0, TimeUnit.SECONDS).maxNumberOfRejectedRequestPerConnection(0).build();
                 PulsarClientImpl pulsarClient2 = (PulsarClientImpl) PulsarClient.builder().serviceUrl(lookupUrl)
                    .statsInterval(0, TimeUnit.SECONDS).ioThreads(concurrentLookupRequests)
                         .connectionsPerBroker(20).build()) {

                ProducerImpl<byte[]> producer =
                        (ProducerImpl<byte[]>) pulsarClient.newProducer().topic(topicName).create();
                ClientCnx cnx = producer.cnx();
                assertTrue(cnx.channel().isActive());

                final List<CompletableFuture<Producer<byte[]>>> futures = Lists.newArrayList();
                final int totalProducers = 10;
                CountDownLatch latch = new CountDownLatch(totalProducers);
                for (int i = 0; i < totalProducers; i++) {
                    executor.submit(() -> {
                        final String randomTopicName1 = topicName + randomUUID().toString();
                        final String randomTopicName2 = topicName + randomUUID().toString();
                        // pass producer-name to avoid exception: producer is already connected to topic
                        synchronized (futures) {
                            futures.add(pulsarClient2.newProducer().topic(randomTopicName1).createAsync());
                            futures.add(pulsarClient.newProducer().topic(randomTopicName2).createAsync());
                        }
                        latch.countDown();
                    });
                }

                latch.await();
                synchronized (futures) {
                    FutureUtil.waitForAll(futures).get();
                }
            }
        } finally {
            // revert back to original value
            pulsar.getConfiguration().setMaxConcurrentTopicLoadRequest(concurrentTopic);
        }
    }

    /**
     * It verifies that client closes the connection on internalSerevrError which is "ServiceNotReady" from Broker-side
     *
     * @throws Exception
     */
    @Test
    public void testCloseConnectionOnInternalServerError() throws Exception {

        final String topicName = "persistent://prop/usw/my-ns/newTopic";

        @Cleanup
        final PulsarClient pulsarClient = PulsarClient.builder()
                .serviceUrl(pulsar.getBrokerServiceUrl())
                .statsInterval(0, TimeUnit.SECONDS)
                .operationTimeout(1000, TimeUnit.MILLISECONDS)
                .build();

        ProducerImpl<byte[]> producer = (ProducerImpl<byte[]>) pulsarClient.newProducer().topic(topicName).create();
        ClientCnx cnx = producer.cnx();
        assertTrue(cnx.channel().isActive());

        Field cacheField = BaseResources.class.getDeclaredField("cache");
        cacheField.setAccessible(true);
        cacheField.set(pulsar.getPulsarResources().getNamespaceResources().getPartitionedTopicResources(), null);

        try {
            pulsarClient.newProducer().topic(topicName).create();
            fail("it should have fail with lookup-exception:");
        } catch (Exception e) {
            // ok
        }
        // connection must be closed
        assertFalse(cnx.channel().isActive());
    }

    @Test
    public void testInvalidDynamicConfiguration() throws Exception {

        // (1) try to update invalid loadManagerClass name
        try {
            admin.brokers().updateDynamicConfiguration("loadManagerClassName", "org.apache.pulsar.invalid.loadmanager");
            fail("it should have failed due to invalid argument");
        } catch (PulsarAdminException e) {
            // Ok: should have failed due to invalid config value
        }

        // (2) try to update with valid loadManagerClass name
        try {
            admin.brokers().updateDynamicConfiguration("loadManagerClassName",
                    "org.apache.pulsar.broker.loadbalance.ModularLoadManager");
        } catch (PulsarAdminException e) {
            fail("it should have failed due to invalid argument", e);
        }

        // (3) restart broker with invalid config value
        pulsar.getPulsarResources().getDynamicConfigResources()
                .setDynamicConfiguration(m -> {
                    m.put("loadManagerClassName", "org.apache.pulsar.invalid.loadmanager");
                    return m;
                });
    }

    static class TimestampEntryCount {
        private final long timestamp;
        private int numMessages;

        public TimestampEntryCount(long ts) {
            this.numMessages = 0;
            this.timestamp = ts;
        }

        public int incrementAndGet() {
            return ++numMessages;
        }

        public long getTimestamp() {
            return timestamp;
        }
    }

    @Test
    public void testCleanProducer() throws Exception {
        log.info("-- Starting {} test --", methodName);

        admin.clusters().createCluster("global", ClusterData.builder().build());
        admin.namespaces().createNamespace("my-property/global/lookup");

        final int operationTimeOut = 500;
        @Cleanup
        PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(lookupUrl.toString())
                .statsInterval(0, TimeUnit.SECONDS).operationTimeout(operationTimeOut, TimeUnit.MILLISECONDS).build();
        CountDownLatch latch = new CountDownLatch(1);
        pulsarClient.newProducer().topic("persistent://my-property/global/lookup/my-topic1").createAsync()
                .handle((producer, e) -> {
                    latch.countDown();
                    return null;
                });

        latch.await(operationTimeOut + 1000, TimeUnit.MILLISECONDS);
        Field prodField = PulsarClientImpl.class.getDeclaredField("producers");
        prodField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Set<ProducerBase<byte[]>> producers = (Set<ProducerBase<byte[]>>) prodField
                .get(pulsarClient);
        assertTrue(producers.isEmpty());
        log.info("-- Exiting {} test --", methodName);
    }

    /**
     * It verifies that if broker fails to complete producer/consumer operation then client times out rather waiting
     * forever.
     *
     * @throws PulsarClientException
     */
    @Test(expectedExceptions = PulsarClientException.TimeoutException.class)
    public void testOperationTimeout() throws PulsarClientException {
        final String topicName = "persistent://my-property/my-ns/my-topic1";
        ConcurrentOpenHashMap<String, CompletableFuture<Optional<Topic>>> topics = pulsar.getBrokerService()
                .getTopics();
        // non-complete topic future so, create topic should timeout
        topics.put(topicName, new CompletableFuture<>());
        try (PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(lookupUrl.toString())
                .operationTimeout(2, TimeUnit.SECONDS).statsInterval(0, TimeUnit.SECONDS).build()) {

            Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create();
        } finally {
            topics.clear();
        }
    }

    @Test
    public void testAddEntryOperationTimeout() throws Exception {

        log.info("-- Starting {} test --", methodName);

        conf.setManagedLedgerAddEntryTimeoutSeconds(1);

        final String topicName = "persistent://my-property/my-ns/addEntryTimeoutTopic";

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName).create();
        ConsumerImpl<byte[]> consumer = (ConsumerImpl<byte[]>) pulsarClient.newConsumer().topic(topicName)
                .subscriptionName("my-subscriber-name").subscribe();

        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getOrCreateTopic(topicName).get();
        ManagedLedgerImpl ml = (ManagedLedgerImpl) topic.getManagedLedger();

        class MockLedgerHandle extends PulsarMockLedgerHandle {
            public MockLedgerHandle(PulsarMockBookKeeper bk, long id, DigestType digest, byte[] passwd)
                    throws GeneralSecurityException {
                super(bk, id, digest, passwd);
            }

            @Override
            public void asyncAddEntry(final byte[] data, final AddCallback cb, final Object ctx) {
                // do nothing
            }

            @Override
            public void asyncClose(org.apache.bookkeeper.client.AsyncCallback.CloseCallback cb, Object ctx) {
                cb.closeComplete(BKException.Code.OK, this, ctx);
            }
        }
        MockLedgerHandle ledgerHandle = mock(MockLedgerHandle.class);
        final byte[] data = "data".getBytes();
        // this will make first entry to be timed out but then managed-ledger will create a new ledger and next time add
        // entry should be successful.
        doNothing().when(ledgerHandle).asyncAddEntry(data, null, null);

        MockedPulsarServiceBaseTest.setFieldValue(ManagedLedgerImpl.class, ml, "currentLedger", ledgerHandle);
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean addedSuccessfully = new AtomicBoolean(false);
        producer.sendAsync(data).handle((res, ex) -> {
            if (ex == null) {
                addedSuccessfully.set(true);
            } else {
                log.error("add-entry failed for {}", methodName, ex);
            }
            latch.countDown();
            return null;
        });
        latch.await();

        // broker should be resilient enough to add-entry timeout and add entry successfully.
        assertTrue(addedSuccessfully.get());

        byte[] receivedData = consumer.receive().getData();
        assertEquals(receivedData, data);

        producer.close();
        consumer.close();
    }


    @Test
    public void testAvroSchemaProducerConsumerWithSpecifiedReaderAndWriter() throws PulsarClientException {
        final String topicName = "persistent://my-property/my-ns/my-topic1";
        TestMessageObject object = new TestMessageObject();
        SchemaReader<TestMessageObject> reader = Mockito.mock(SchemaReader.class);
        SchemaWriter<TestMessageObject> writer = Mockito.mock(SchemaWriter.class);
        Mockito.when(reader.read(Mockito.any(byte[].class), Mockito.any(byte[].class))).thenReturn(object);
        Mockito.when(writer.write(Mockito.any(TestMessageObject.class))).thenReturn("fake data".getBytes(StandardCharsets.UTF_8));
        SchemaDefinition<TestMessageObject> schemaDefinition = new SchemaDefinitionBuilderImpl<TestMessageObject>()
                .withPojo(TestMessageObject.class)
                .withSchemaReader(reader)
                .withSchemaWriter(writer)
                .build();
        Schema<TestMessageObject> schema = Schema.AVRO(schemaDefinition);

        try (PulsarClient client = PulsarClient.builder()
                .serviceUrl(lookupUrl.toString())
                .build(); Producer<TestMessageObject> producer = client.newProducer(schema).topic(topicName).create();
             Consumer<TestMessageObject> consumer =
                     client.newConsumer(schema).topic(topicName).subscriptionName("my-subscriber-name").subscribe()) {

            assertNotNull(producer);
            assertNotNull(consumer);
            producer.newMessage().value(object).send();
            TestMessageObject testObject = consumer.receive().getValue();
            Assert.assertEquals(object.getValue(), testObject.getValue());
            Mockito.verify(writer, Mockito.times(1)).write(Mockito.any());
            Mockito.verify(reader, Mockito.times(1)).read(Mockito.any(byte[].class), Mockito.any(byte[].class));
        }
    }

    @Test
    public void testJsonSchemaProducerConsumerWithSpecifiedReaderAndWriter() throws PulsarClientException {
        final String topicName = "persistent://my-property/my-ns/my-topic1";
        ObjectMapper mapper = new ObjectMapper();
        SchemaReader<TestMessageObject> reader = Mockito.spy(new JacksonJsonReader<>(mapper, TestMessageObject.class));
        SchemaWriter<TestMessageObject> writer = Mockito.spy(new JacksonJsonWriter<>(mapper));

        SchemaDefinition<TestMessageObject> schemaDefinition = new SchemaDefinitionBuilderImpl<TestMessageObject>()
                .withPojo(TestMessageObject.class)
                .withSchemaReader(reader)
                .withSchemaWriter(writer)
                .build();
        Schema<TestMessageObject> schema = Schema.JSON(schemaDefinition);

        try (PulsarClient client = PulsarClient.builder()
                .serviceUrl(lookupUrl.toString())
                .build(); Producer<TestMessageObject> producer = client.newProducer(schema).topic(topicName).create();
             Consumer<TestMessageObject> consumer = client.newConsumer(schema).topic(topicName).subscriptionName("my-subscriber-name").subscribe()) {

            assertNotNull(producer);
            assertNotNull(consumer);

            TestMessageObject object = new TestMessageObject();
            object.setValue("fooooo");
            producer.newMessage().value(object).send();

            TestMessageObject testObject = consumer.receive().getValue();
            Assert.assertEquals(object.getValue(), testObject.getValue());

            Mockito.verify(writer, Mockito.times(1)).write(Mockito.any());
            Mockito.verify(reader, Mockito.times(1)).read(Mockito.any(byte[].class));
        }
    }

    @Getter
    @Setter
    @EqualsAndHashCode
    private static final class TestMessageObject{
        private String value;
    }
    
    /**
     * It validates pooled message consumption for batch and non-batch messages.
     * 
     * @throws Exception
     */
    @Test(dataProvider = "booleanFlagProvider")
    public void testConsumerWithPooledMessages(boolean isBatchingEnabled) throws Exception {
        log.info("-- Starting {} test --", methodName);

        @Cleanup
        PulsarClient newPulsarClient = PulsarClient.builder().serviceUrl(lookupUrl.toString()).build();

        final String topic = "persistent://my-property/my-ns/testConsumerWithPooledMessages" + isBatchingEnabled;

        @Cleanup
        Consumer<ByteBuffer> consumer = newPulsarClient.newConsumer(Schema.BYTEBUFFER).topic(topic)
                .subscriptionName("my-sub").poolMessages(true).subscribe();

        @Cleanup
        Producer<byte[]> producer = newPulsarClient.newProducer().topic(topic).enableBatching(isBatchingEnabled).create();

        final int numMessages = 100;
        for (int i = 0; i < numMessages; i++) {
            producer.newMessage().value(("value-" + i).getBytes(UTF_8))
            .eventTime((i + 1) * 100L).sendAsync();
        }
        producer.flush();

        // Reuse pre-allocated pooled buffer to process every message
        byte[] val = null;
        int size = 0;
        for (int i = 0; i < numMessages; i++) {
            Message<ByteBuffer> msg = consumer.receive();
            ByteBuffer value;
            try {
                value = msg.getValue();
                int capacity = value.remaining();
                // expand the size of buffer if needed
                if (capacity > size) {
                    val = new byte[capacity];
                    size = capacity;
                }
                // read message into pooled buffer
                value.get(val, 0, capacity);
                // process the message
                assertEquals(("value-" + i), new String(val, 0, capacity));
            } finally {
                msg.release();
            }
        }
        consumer.close();
        producer.close();
    }
    
    /**
     * It verifies that expiry/redelivery of messages relesaes the messages without leak.
     * 
     * @param isBatchingEnabled
     * @throws Exception
     */
    @Test(dataProvider = "booleanFlagProvider")
    public void testPooledMessageWithAckTimeout(boolean isBatchingEnabled) throws Exception {
        log.info("-- Starting {} test --", methodName);

        @Cleanup
        PulsarClient newPulsarClient = PulsarClient.builder().serviceUrl(lookupUrl.toString()).build();

        final String topic = "persistent://my-property/my-ns/testPooledMessageWithAckTimeout" + isBatchingEnabled;

        @Cleanup
        ConsumerImpl<ByteBuffer> consumer = (ConsumerImpl<ByteBuffer>) newPulsarClient.newConsumer(Schema.BYTEBUFFER)
                .topic(topic).subscriptionName("my-sub").poolMessages(true).subscribe();

        @Cleanup
        Producer<byte[]> producer = newPulsarClient.newProducer().topic(topic).enableBatching(isBatchingEnabled)
                .create();

        final int numMessages = 100;
        for (int i = 0; i < numMessages; i++) {
            producer.newMessage().value(("value-" + i).getBytes(UTF_8)).eventTime((i + 1) * 100L).sendAsync();
        }
        producer.flush();

        retryStrategically((test) -> consumer.incomingMessages.peek() != null, 5, 500);
        MessageImpl<ByteBuffer> msg = (MessageImpl) consumer.incomingMessages.peek();
        assertNotNull(msg);
        ByteBuf payload = ((MessageImpl) msg).getPayload();
        assertNotEquals(payload.refCnt(), 0);
        consumer.redeliverUnacknowledgedMessages();
        assertEquals(payload.refCnt(), 0);
        consumer.close();
        producer.close();
    }

    /**
     * It validates pooled message consumption for batch and non-batch messages.
     * 
     * @throws Exception
     */
    @Test(dataProvider = "booleanFlagProvider")
    public void testConsumerWithPooledMessagesWithReader(boolean isBatchingEnabled) throws Exception {
        log.info("-- Starting {} test --", methodName);

        @Cleanup
        PulsarClient newPulsarClient = PulsarClient.builder().serviceUrl(lookupUrl.toString()).build();

        final String topic = "persistent://my-property/my-ns/testConsumerWithPooledMessages" + isBatchingEnabled;

        @Cleanup
        Reader<ByteBuffer> reader = newPulsarClient.newReader(Schema.BYTEBUFFER).topic(topic).poolMessages(true)
                .startMessageId(MessageId.latest).create();

        @Cleanup
        Producer<byte[]> producer = newPulsarClient.newProducer().topic(topic).enableBatching(isBatchingEnabled).create();

        final int numMessages = 100;
        for (int i = 0; i < numMessages; i++) {
            producer.newMessage().value(("value-" + i).getBytes(UTF_8))
            .eventTime((i + 1) * 100L).sendAsync();
        }
        producer.flush();

        // Reuse pre-allocated pooled buffer to process every message
        byte[] val = null;
        int size = 0;
        for (int i = 0; i < numMessages; i++) {
            Message<ByteBuffer> msg = reader.readNext();
            ByteBuffer value;
            try {
                value = msg.getValue();
                int capacity = value.remaining();
                // expand the size of buffer if needed
                if (capacity > size) {
                    val = new byte[capacity];
                    size = capacity;
                }
                // read message into pooled buffer
                value.get(val, 0, capacity);
                // process the message
                assertEquals(("value-" + i), new String(val, 0, capacity));
                assertTrue(value.isDirect());
            } finally {
                msg.release();
            }
        }
        reader.close();
        producer.close();
    }
}
