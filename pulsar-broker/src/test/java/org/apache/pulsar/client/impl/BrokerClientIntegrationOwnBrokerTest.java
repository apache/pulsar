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
package org.apache.pulsar.client.impl;

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
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.google.common.collect.Sets;
import java.lang.reflect.Field;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Cleanup;
import lombok.CustomLog;
import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.PulsarMockBookKeeper;
import org.apache.bookkeeper.client.PulsarMockLedgerHandle;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.namespace.OwnershipCache;
import org.apache.pulsar.broker.resources.BaseResources;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.HandlerState.State;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.collections.ConcurrentLongHashMap;
import org.mockito.invocation.InvocationOnMock;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Broker-client tests that disable, close, restart or stub parts of their broker, so they run on a broker of their
 * own. {@link BrokerClientIntegrationTest} has the tests that share one.
 */
@Test(groups = "broker-impl")
@CustomLog
public class BrokerClientIntegrationOwnBrokerTest extends ProducerConsumerBase {

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

    /**
     * Verifies unload namespace-bundle doesn't close shared connection used by other namespace-bundle.
     *
     * <pre>
     * 1. after disabling broker fron loadbalancer
     * 2. unload namespace-bundle "my-ns1" which disconnects client (producer/consumer)
     *    connected on that namespacebundle
     * 3. but doesn't close the connection for namesapce-bundle "my-ns2" and clients are still connected
     * 4. verifies unloaded "my-ns1" should not connected again with the broker as broker is disabled
     * 5. unload "my-ns2" which closes the connection as broker doesn't have any more client
     *    connected on that connection
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
        doAnswer(InvocationOnMock::callRealMethod).when(consumer1).connectionClosed(any(), any(), any());
        ProducerImpl<byte[]> producer1 = spy(prod1);
        doAnswer(invocationOnMock -> prod1.getState()).when(producer1).getState();
        doAnswer(invocationOnMock -> prod1.getClientCnx()).when(producer1).getClientCnx();
        doAnswer(invocationOnMock -> prod1.cnx()).when(producer1).cnx();
        ProducerImpl<byte[]> producer2 = spy(prod2);
        doAnswer(invocationOnMock -> prod2.getState()).when(producer2).getState();
        doAnswer(invocationOnMock -> prod2.getClientCnx()).when(producer2).getClientCnx();
        doAnswer(invocationOnMock -> prod2.cnx()).when(producer2).cnx();

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
        verify(producer1, atLeastOnce()).connectionClosed(any(), any(), any());
        // [2] Verify: consumer1 must get connectionClosed signal
        verify(consumer1, atLeastOnce()).connectionClosed(any(), any(), any());
        // [3] Verify: producer2 should have not received connectionClosed signal
        verify(producer2, never()).connectionClosed(any(), any(), any());

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
        verify(producer2, atLeastOnce()).connectionClosed(any(), any(), any());

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
     * It verifies that broker throttles down configured concurrent topic loading requests.
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
    @SuppressWarnings("deprecation")
    @Test
    public void testMaxConcurrentTopicLoading() throws Exception {
        final String topicName = "persistent://my-property/my-ns/cocurrentLoadingTopic";
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

                final List<CompletableFuture<Producer<byte[]>>> futures = new ArrayList<>();
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
     * It verifies that client closes the connection on internalServerError which is "ServiceNotReady" from Broker-side.
     *
     * @throws Exception
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testCloseConnectionOnInternalServerError() throws Exception {

        final String topicName = "persistent://my-property/my-ns/newTopic";

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

    /**
     * It verifies that if broker fails to complete producer/consumer operation then client times out rather waiting
     * forever.
     *
     * @throws PulsarClientException
     */
    @SuppressWarnings("deprecation")
    @Test(expectedExceptions = PulsarClientException.TimeoutException.class)
    public void testOperationTimeout() throws PulsarClientException {
        final String topicName = "persistent://my-property/my-ns/my-topic1";
        final var topics = pulsar.getBrokerService().getTopics();
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

        log.info().attr("method", methodName).log("Starting test");

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
        doAnswer(invocation -> {
            org.apache.bookkeeper.client.AsyncCallback.CloseCallback cb = invocation.getArgument(0);
            cb.closeComplete(BKException.Code.OK, ledgerHandle, invocation.getArgument(1));
            return null;
        }).when(ledgerHandle).asyncClose(any(), any());

        MockedPulsarServiceBaseTest.setFieldValue(ManagedLedgerImpl.class, ml, "currentLedger", ledgerHandle);
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean addedSuccessfully = new AtomicBoolean(false);
        producer.sendAsync(data).handle((res, ex) -> {
            if (ex == null) {
                addedSuccessfully.set(true);
            } else {
                log.error().attr("entry", methodName).exception(ex).log("add-entry failed for");
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
}
