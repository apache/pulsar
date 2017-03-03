/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.broker.service;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.matches;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.bookkeeper.mledger.AsyncCallbacks.AddEntryCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.CloseCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteCursorCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteLedgerCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OpenCursorCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OpenLedgerCallback;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.zookeeper.ZooKeeper;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.mockito.verification.VerificationMode;
import org.powermock.api.mockito.PowerMockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.yahoo.pulsar.broker.PulsarService;
import com.yahoo.pulsar.broker.ServiceConfiguration;
import com.yahoo.pulsar.broker.admin.AdminResource;
import com.yahoo.pulsar.broker.cache.ConfigurationCacheService;
import com.yahoo.pulsar.broker.namespace.NamespaceService;
import com.yahoo.pulsar.broker.service.persistent.PersistentDispatcherMultipleConsumers;
import com.yahoo.pulsar.broker.service.persistent.PersistentDispatcherSingleActiveConsumer;
import com.yahoo.pulsar.broker.service.persistent.PersistentReplicator;
import com.yahoo.pulsar.broker.service.persistent.PersistentSubscription;
import com.yahoo.pulsar.broker.service.persistent.PersistentTopic;
import com.yahoo.pulsar.client.api.ProducerConfiguration;
import com.yahoo.pulsar.client.api.PulsarClient;
import com.yahoo.pulsar.client.impl.PulsarClientImpl;
import com.yahoo.pulsar.common.api.proto.PulsarApi.CommandSubscribe;
import com.yahoo.pulsar.common.api.proto.PulsarApi.CommandSubscribe.SubType;
import com.yahoo.pulsar.common.api.proto.PulsarApi.MessageMetadata;
import com.yahoo.pulsar.common.naming.DestinationName;
import com.yahoo.pulsar.common.naming.NamespaceBundle;
import com.yahoo.pulsar.common.policies.data.Policies;
import com.yahoo.pulsar.common.util.collections.ConcurrentOpenHashMap;
import com.yahoo.pulsar.zookeeper.ZooKeeperDataCache;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 */
public class PersistentTopicTest {
    private PulsarService pulsar;
    private BrokerService brokerService;
    private ManagedLedgerFactory mlFactoryMock;
    private ServerCnx serverCnx;
    private ManagedLedger ledgerMock;
    private ManagedCursor cursorMock;
    private ConfigurationCacheService configCacheService;

    final String successTopicName = "persistent://prop/use/ns-abc/successTopic";
    final String successPartitionTopicName = "persistent://prop/use/ns-abc/successTopic-partition-0";
    final String failTopicName = "persistent://prop/use/ns-abc/failTopic";
    final String successSubName = "successSub";
    final String successSubName2 = "successSub2";
    final String successSubName3 = "successSub3";
    private static final Logger log = LoggerFactory.getLogger(PersistentTopicTest.class);

    @BeforeMethod
    public void setup() throws Exception {
        ServiceConfiguration svcConfig = spy(new ServiceConfiguration());
        pulsar = spy(new PulsarService(svcConfig));
        doReturn(svcConfig).when(pulsar).getConfiguration();

        mlFactoryMock = mock(ManagedLedgerFactory.class);
        doReturn(mlFactoryMock).when(pulsar).getManagedLedgerFactory();

        ZooKeeper mockZk = mock(ZooKeeper.class);
        doReturn(mockZk).when(pulsar).getZkClient();

        configCacheService = mock(ConfigurationCacheService.class);
        @SuppressWarnings("unchecked")
        ZooKeeperDataCache<Policies> zkDataCache = mock(ZooKeeperDataCache.class);
        doReturn(zkDataCache).when(configCacheService).policiesCache();
        doReturn(configCacheService).when(pulsar).getConfigurationCache();
        doReturn(Optional.empty()).when(zkDataCache).get(anyString());

        brokerService = spy(new BrokerService(pulsar));
        doReturn(brokerService).when(pulsar).getBrokerService();

        serverCnx = spy(new ServerCnx(brokerService));
        doReturn(true).when(serverCnx).isActive();
        doReturn(true).when(serverCnx).isWritable();
        doReturn(new InetSocketAddress("localhost", 1234)).when(serverCnx).clientAddress();

        NamespaceService nsSvc = mock(NamespaceService.class);
        doReturn(nsSvc).when(pulsar).getNamespaceService();
        doReturn(true).when(nsSvc).isServiceUnitOwned(any(NamespaceBundle.class));
        doReturn(true).when(nsSvc).isServiceUnitActive(any(DestinationName.class));

        setupMLAsyncCallbackMocks();
    }

    @AfterMethod
    public void teardown() throws Exception {
        brokerService.getTopics().clear();
        brokerService.close(); //to clear pulsarStats
        pulsar.close();
    }

    @Test
    public void testCreateTopic() throws Exception {
        final ManagedLedger ledgerMock = mock(ManagedLedger.class);
        doReturn(new ArrayList<Object>()).when(ledgerMock).getCursors();

        final String topicName = "persistent://prop/use/ns-abc/topic1";
        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                ((OpenLedgerCallback) invocationOnMock.getArguments()[2]).openLedgerComplete(ledgerMock, null);
                return null;
            }
        }).when(mlFactoryMock).asyncOpen(anyString(), any(ManagedLedgerConfig.class), any(OpenLedgerCallback.class),
                anyObject());

        CompletableFuture<Void> future = brokerService.getTopic(topicName).thenAccept(topic -> {
            assertTrue(topic.toString().contains(topicName));
        }).exceptionally((t) -> {
            fail("should not fail");
            return null;
        });

        // wait for completion
        try {
            future.get(1, TimeUnit.SECONDS);
        } catch (Exception e) {
            fail("Should not fail or time out");
        }
    }

    @Test
    public void testCreateTopicMLFailure() throws Exception {
        final String jinxedTopicName = "persistent://prop/use/ns-abc/topic3";
        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                new Thread(() -> {
                    ((OpenLedgerCallback) invocationOnMock.getArguments()[2])
                            .openLedgerFailed(new ManagedLedgerException("Managed ledger failure"), null);
                }).start();

                return null;
            }
        }).when(mlFactoryMock).asyncOpen(anyString(), any(ManagedLedgerConfig.class), any(OpenLedgerCallback.class),
                anyObject());

        CompletableFuture<Topic> future = brokerService.getTopic(jinxedTopicName);

        // wait for completion
        try {
            future.get(1, TimeUnit.SECONDS);
            fail("should have failed");
        } catch (TimeoutException e) {
            fail("Should not time out");
        } catch (Exception e) {
            // OK
        }
    }

    @Test
    public void testPublishMessage() throws Exception {

        PersistentTopic topic = new PersistentTopic(successTopicName, ledgerMock, brokerService);
        /*
         * MessageMetadata.Builder messageMetadata = MessageMetadata.newBuilder();
         * messageMetadata.setPublishTime(System.currentTimeMillis()); messageMetadata.setProducerName("producer-name");
         * messageMetadata.setSequenceId(1);
         */
        ByteBuf payload = Unpooled.wrappedBuffer("content".getBytes());

        final CountDownLatch latch = new CountDownLatch(1);

        topic.publishMessage(payload, (exception, ledgerId, entryId) -> {
            latch.countDown();
        });

        assertTrue(latch.await(1, TimeUnit.SECONDS));
    }

    @Test
    public void testDispatcherMultiConsumerReadFailed() throws Exception {
        PersistentTopic topic = spy(new PersistentTopic(successTopicName, ledgerMock, brokerService));
        ManagedCursor cursor = mock(ManagedCursor.class);
        when(cursor.getName()).thenReturn("cursor");
        PersistentDispatcherMultipleConsumers dispatcher = new PersistentDispatcherMultipleConsumers(topic, cursor);
        dispatcher.readEntriesFailed(new ManagedLedgerException.InvalidCursorPositionException("failed"), null);
        verify(topic, atLeast(1)).getBrokerService();
    }

    @Test
    public void testDispatcherSingleConsumerReadFailed() throws Exception {
        PersistentTopic topic = spy(new PersistentTopic(successTopicName, ledgerMock, brokerService));
        ManagedCursor cursor = mock(ManagedCursor.class);
        when(cursor.getName()).thenReturn("cursor");
        PersistentDispatcherSingleActiveConsumer dispatcher = new PersistentDispatcherSingleActiveConsumer(cursor,
                SubType.Exclusive, 1, topic);
        Consumer consumer = mock(Consumer.class);
        dispatcher.readEntriesFailed(new ManagedLedgerException.InvalidCursorPositionException("failed"), consumer);
        verify(topic, atLeast(1)).getBrokerService();
    }

    @Test
    public void testPublishMessageMLFailure() throws Exception {
        final String successTopicName = "persistent://prop/use/ns-abc/successTopic";

        final ManagedLedger ledgerMock = mock(ManagedLedger.class);
        doReturn(new ArrayList<Object>()).when(ledgerMock).getCursors();

        PersistentTopic topic = new PersistentTopic(successTopicName, ledgerMock, brokerService);

        MessageMetadata.Builder messageMetadata = MessageMetadata.newBuilder();
        messageMetadata.setPublishTime(System.currentTimeMillis());
        messageMetadata.setProducerName("prod-name");
        messageMetadata.setSequenceId(1);

        ByteBuf payload = Unpooled.wrappedBuffer("content".getBytes());
        final CountDownLatch latch = new CountDownLatch(1);

        // override asyncAddEntry callback to return error
        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                ((AddEntryCallback) invocationOnMock.getArguments()[1]).addFailed(
                        new ManagedLedgerException("Managed ledger failure"), invocationOnMock.getArguments()[2]);
                return null;
            }
        }).when(ledgerMock).asyncAddEntry(any(ByteBuf.class), any(AddEntryCallback.class), anyObject());

        topic.publishMessage(payload, (exception, ledgerId, entryId) -> {
            if (exception == null) {
                fail("publish should have failed");
            } else {
                latch.countDown();
            }
        });

        assertTrue(latch.await(1, TimeUnit.SECONDS));
    }

    @Test
    public void testAddRemoveProducer() throws Exception {
        PersistentTopic topic = new PersistentTopic(successTopicName, ledgerMock, brokerService);

        String role = "appid1";
        // 1. simple add producer
        Producer producer = new Producer(topic, serverCnx, 1 /* producer id */, "prod-name", role);
        topic.addProducer(producer);
        assertEquals(topic.getProducers().size(), 1);

        // 2. duplicate add
        try {
            topic.addProducer(producer);
            fail("Should have failed with naming exception because producer 'null' is already connected to the topic");
        } catch (BrokerServiceException e) {
            assertTrue(e instanceof BrokerServiceException.NamingException);
        }
        assertEquals(topic.getProducers().size(), 1);

        // 3. add producer for a different topic
        PersistentTopic failTopic = new PersistentTopic(failTopicName, ledgerMock, brokerService);
        Producer failProducer = new Producer(failTopic, serverCnx, 2 /* producer id */, "prod-name", role);
        try {
            topic.addProducer(failProducer);
            fail("should have failed");
        } catch (IllegalArgumentException e) {
            // OK
        }

        // 4. simple remove producer
        topic.removeProducer(producer);
        assertEquals(topic.getProducers().size(), 0);

        // 5. duplicate remove
        topic.removeProducer(producer); /* noop */
    }

    @Test
    public void testSubscribeUnsubscribe() throws Exception {
        PersistentTopic topic = new PersistentTopic(successTopicName, ledgerMock, brokerService);

        CommandSubscribe cmd = CommandSubscribe.newBuilder().setConsumerId(1).setTopic(successTopicName)
                .setSubscription(successSubName).setRequestId(1).setSubType(SubType.Exclusive).build();

        // 1. simple subscribe
        Future<Consumer> f1 = topic.subscribe(serverCnx, cmd.getSubscription(), cmd.getConsumerId(), cmd.getSubType(),
                0, cmd.getConsumerName());
        f1.get();

        // 2. duplicate subscribe
        Future<Consumer> f2 = topic.subscribe(serverCnx, cmd.getSubscription(), cmd.getConsumerId(), cmd.getSubType(),
                0, cmd.getConsumerName());

        try {
            f2.get();
            fail("should fail with exception");
        } catch (ExecutionException ee) {
            // Expected
            assertTrue(ee.getCause() instanceof BrokerServiceException.ConsumerBusyException);
        }

        // 3. simple unsubscribe
        Future<Void> f3 = topic.unsubscribe(successSubName);
        f3.get();

        assertNull(topic.getPersistentSubscription(successSubName));
    }

    @Test
    public void testAddRemoveConsumer() throws Exception {
        PersistentTopic topic = new PersistentTopic(successTopicName, ledgerMock, brokerService);
        PersistentSubscription sub = new PersistentSubscription(topic, cursorMock);

        // 1. simple add consumer
        Consumer consumer = new Consumer(sub, SubType.Exclusive, 1 /* consumer id */, 0, "Cons1"/* consumer name */,
                50000, serverCnx, "myrole-1");
        sub.addConsumer(consumer);
        assertTrue(sub.getDispatcher().isConsumerConnected());

        // 2. duplicate add consumer
        try {
            sub.addConsumer(consumer);
            fail("Should fail with ConsumerBusyException");
        } catch (BrokerServiceException e) {
            assertTrue(e instanceof BrokerServiceException.ConsumerBusyException);
        }

        // 3. simple remove consumer
        sub.removeConsumer(consumer);
        assertFalse(sub.getDispatcher().isConsumerConnected());

        // 4. duplicate remove consumer
        try {
            sub.removeConsumer(consumer);
            fail("Should fail with ServerMetadataException");
        } catch (BrokerServiceException e) {
            assertTrue(e instanceof BrokerServiceException.ServerMetadataException);
        }
    }

    @Test
    public void testUbsubscribeRaceConditions() throws Exception {
        PersistentTopic topic = new PersistentTopic(successTopicName, ledgerMock, brokerService);
        PersistentSubscription sub = new PersistentSubscription(topic, cursorMock);
        Consumer consumer1 = new Consumer(sub, SubType.Exclusive, 1 /* consumer id */, 0, "Cons1"/* consumer name */,
                50000, serverCnx, "myrole-1");
        sub.addConsumer(consumer1);

        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                ((DeleteCursorCallback) invocationOnMock.getArguments()[1]).deleteCursorComplete(null);
                Thread.sleep(1000);
                return null;
            }
        }).when(ledgerMock).asyncDeleteCursor(matches(".*success.*"), any(DeleteCursorCallback.class), anyObject());

        ExecutorService executor = Executors.newCachedThreadPool();

        executor.submit(() -> {
            sub.doUnsubscribe(consumer1);
            return null;
        }).get();

        try {
            Thread.sleep(10); /* delay to ensure that the ubsubscribe gets executed first */
            Consumer consumer2 = new Consumer(sub, SubType.Exclusive, 2 /* consumer id */, 0, "Cons2"/* consumer name */,
                    50000, serverCnx, "myrole-1");
        } catch (BrokerServiceException e) {
            assertTrue(e instanceof BrokerServiceException.SubscriptionFencedException);
        }
    }

    @Test
    public void testDeleteTopic() throws Exception {
        // create topic
        PersistentTopic topic = (PersistentTopic) brokerService.getTopic(successTopicName).get();

        String role = "appid1";
        // 1. delete inactive topic
        topic.delete().get();
        assertNull(brokerService.getTopicReference(successTopicName));

        // 2. delete topic with producer
        topic = (PersistentTopic) brokerService.getTopic(successTopicName).get();
        Producer producer = new Producer(topic, serverCnx, 1 /* producer id */, "prod-name", role);
        topic.addProducer(producer);

        assertTrue(topic.delete().isCompletedExceptionally());
        topic.removeProducer(producer);

        // 3. delete topic with subscriber
        CommandSubscribe cmd = CommandSubscribe.newBuilder().setConsumerId(1).setTopic(successTopicName)
                .setSubscription(successSubName).setRequestId(1).setSubType(SubType.Exclusive).build();

        Future<Consumer> f1 = topic.subscribe(serverCnx, cmd.getSubscription(), cmd.getConsumerId(), cmd.getSubType(),
                0, cmd.getConsumerName());
        f1.get();

        assertTrue(topic.delete().isCompletedExceptionally());
        topic.unsubscribe(successSubName);
    }

    @Test
    public void testDeleteAndUnsubscribeTopic() throws Exception {
        // create topic
        final PersistentTopic topic = (PersistentTopic) brokerService.getTopic(successTopicName).get();
        CommandSubscribe cmd = CommandSubscribe.newBuilder().setConsumerId(1).setTopic(successTopicName)
                .setSubscription(successSubName).setRequestId(1).setSubType(SubType.Exclusive).build();

        Future<Consumer> f1 = topic.subscribe(serverCnx, cmd.getSubscription(), cmd.getConsumerId(), cmd.getSubType(),
                0, cmd.getConsumerName());
        f1.get();

        final CyclicBarrier barrier = new CyclicBarrier(2);
        final CountDownLatch counter = new CountDownLatch(2);
        final AtomicBoolean gotException = new AtomicBoolean(false);

        Thread deleter = new Thread() {
            @Override
            public void run() {
                try {
                    barrier.await();
                    assertFalse(topic.delete().isCompletedExceptionally());
                } catch (Exception e) {
                    e.printStackTrace();
                    gotException.set(true);
                } finally {
                    counter.countDown();
                }
            }
        };

        Thread unsubscriber = new Thread() {
            @Override
            public void run() {
                try {
                    barrier.await();

                    // do topic unsubscribe
                    topic.unsubscribe(successSubName);
                } catch (Exception e) {
                    e.printStackTrace();
                    gotException.set(true);
                } finally {
                    counter.countDown();
                }
            }
        };

        deleter.start();
        unsubscriber.start();

        counter.await();
        assertEquals(gotException.get(), false);
    }

    // @Test
    public void testConcurrentTopicAndSubscriptionDelete() throws Exception {
        // create topic
        final PersistentTopic topic = (PersistentTopic) brokerService.getTopic(successTopicName).get();
        CommandSubscribe cmd = CommandSubscribe.newBuilder().setConsumerId(1).setTopic(successTopicName)
                .setSubscription(successSubName).setRequestId(1).setSubType(SubType.Exclusive).build();

        Future<Consumer> f1 = topic.subscribe(serverCnx, cmd.getSubscription(), cmd.getConsumerId(), cmd.getSubType(),
                0, cmd.getConsumerName());
        f1.get();

        final CyclicBarrier barrier = new CyclicBarrier(2);
        final CountDownLatch counter = new CountDownLatch(2);
        final AtomicBoolean gotException = new AtomicBoolean(false);

        Thread deleter = new Thread() {
            @Override
            public void run() {
                try {
                    barrier.await();
                    // assertTrue(topic.unsubscribe(successSubName).isDone());
                    Thread.sleep(5, 0);
                    log.info("deleter outcome is {}", topic.delete().get());
                } catch (Exception e) {
                    e.printStackTrace();
                    gotException.set(true);
                } finally {
                    counter.countDown();
                }
            }
        };

        Thread unsubscriber = new Thread() {
            @Override
            public void run() {
                try {
                    barrier.await();
                    // do subscription delete
                    ConcurrentOpenHashMap<String, PersistentSubscription> subscriptions = topic.getSubscriptions();
                    PersistentSubscription ps = subscriptions.get(successSubName);
                    // Thread.sleep(5,0);
                    log.info("unsubscriber outcome is {}", ps.doUnsubscribe(ps.getConsumers().get(0)).get());
                    // assertFalse(ps.delete().isCompletedExceptionally());
                } catch (Exception e) {
                    e.printStackTrace();
                    gotException.set(true);
                } finally {
                    counter.countDown();
                }
            }
        };

        deleter.start();
        unsubscriber.start();

        counter.await();
        assertEquals(gotException.get(), false);
    }

    @Test
    public void testDeleteTopicRaceConditions() throws Exception {
        PersistentTopic topic = (PersistentTopic) brokerService.getTopic(successTopicName).get();

        // override ledger deletion callback to slow down deletion
        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Thread.sleep(1000);
                ((DeleteLedgerCallback) invocationOnMock.getArguments()[0]).deleteLedgerComplete(null);
                return null;
            }
        }).when(ledgerMock).asyncDelete(any(DeleteLedgerCallback.class), anyObject());

        ExecutorService executor = Executors.newCachedThreadPool();

        executor.submit(() -> {
            topic.delete();
            return null;
        }).get();

        try {
            String role = "appid1";
            Thread.sleep(10); /* delay to ensure that the delete gets executed first */
            Producer producer = new Producer(topic, serverCnx, 1 /* producer id */, "prod-name", role);
            topic.addProducer(producer);
            fail("Should have failed");
        } catch (BrokerServiceException e) {
            assertTrue(e instanceof BrokerServiceException.TopicFencedException);
        }

        CommandSubscribe cmd = CommandSubscribe.newBuilder().setConsumerId(1).setTopic(successTopicName)
                .setSubscription(successSubName).setRequestId(1).setSubType(SubType.Exclusive).build();

        Future<Consumer> f = topic.subscribe(serverCnx, cmd.getSubscription(), cmd.getConsumerId(), cmd.getSubType(),
                0, cmd.getConsumerName());

        try {
            f.get();
            fail("should have failed");
        } catch (ExecutionException ee) {
            assertTrue(ee.getCause() instanceof BrokerServiceException.TopicFencedException);
            // Expected
        }
    }

    void setupMLAsyncCallbackMocks() {
        ledgerMock = mock(ManagedLedger.class);
        cursorMock = mock(ManagedCursor.class);
        final CompletableFuture<Void> closeFuture = new CompletableFuture<>();

        doReturn(new ArrayList<Object>()).when(ledgerMock).getCursors();
        doReturn("mockCursor").when(cursorMock).getName();
        // doNothing().when(cursorMock).asyncClose(new CloseCallback() {
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                // return closeFuture.get();
                return closeFuture.complete(null);
            }
        })

                .when(cursorMock).asyncClose(new CloseCallback() {

                    @Override
                    public void closeComplete(Object ctx) {
                        log.info("[{}] Successfully closed cursor ledger", "mockCursor");
                        closeFuture.complete(null);
                    }

                    @Override
                    public void closeFailed(ManagedLedgerException exception, Object ctx) {
                        // isFenced.set(false);

                        log.error("Error closing cursor for subscription", exception);
                        closeFuture.completeExceptionally(new BrokerServiceException.PersistenceException(exception));
                    }
                }, null);

        // call openLedgerComplete with ledgerMock on ML factory asyncOpen
        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                ((OpenLedgerCallback) invocationOnMock.getArguments()[2]).openLedgerComplete(ledgerMock, null);
                return null;
            }
        }).when(mlFactoryMock).asyncOpen(matches(".*success.*"), any(ManagedLedgerConfig.class),
                any(OpenLedgerCallback.class), anyObject());

        // call openLedgerFailed on ML factory asyncOpen
        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                ((OpenLedgerCallback) invocationOnMock.getArguments()[2])
                        .openLedgerFailed(new ManagedLedgerException("Managed ledger failure"), null);
                return null;
            }
        }).when(mlFactoryMock).asyncOpen(matches(".*fail.*"), any(ManagedLedgerConfig.class),
                any(OpenLedgerCallback.class), anyObject());

        // call addComplete on ledger asyncAddEntry
        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                ((AddEntryCallback) invocationOnMock.getArguments()[1]).addComplete(new PositionImpl(1, 1),
                        invocationOnMock.getArguments()[2]);
                return null;
            }
        }).when(ledgerMock).asyncAddEntry(any(ByteBuf.class), any(AddEntryCallback.class), anyObject());

        // call openCursorComplete on cursor asyncOpen
        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                ((OpenCursorCallback) invocationOnMock.getArguments()[1]).openCursorComplete(cursorMock, null);
                return null;
            }
        }).when(ledgerMock).asyncOpenCursor(matches(".*success.*"), any(OpenCursorCallback.class), anyObject());

        // call deleteLedgerComplete on ledger asyncDelete
        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                ((DeleteLedgerCallback) invocationOnMock.getArguments()[0]).deleteLedgerComplete(null);
                return null;
            }
        }).when(ledgerMock).asyncDelete(any(DeleteLedgerCallback.class), anyObject());

        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                ((DeleteCursorCallback) invocationOnMock.getArguments()[1]).deleteCursorComplete(null);
                return null;
            }
        }).when(ledgerMock).asyncDeleteCursor(matches(".*success.*"), any(DeleteCursorCallback.class), anyObject());
    }

    @Test
    public void testFailoverSubscription() throws Exception {
        PersistentTopic topic1 = new PersistentTopic(successTopicName, ledgerMock, brokerService);

        CommandSubscribe cmd1 = CommandSubscribe.newBuilder().setConsumerId(1).setTopic(successTopicName)
                .setSubscription(successSubName).setRequestId(1).setSubType(SubType.Failover).build();

        // 1. Subscribe with non partition topic
        Future<Consumer> f1 = topic1.subscribe(serverCnx, cmd1.getSubscription(), cmd1.getConsumerId(),
                cmd1.getSubType(), 0, cmd1.getConsumerName());
        f1.get();

        // 2. Subscribe with partition topic
        PersistentTopic topic2 = new PersistentTopic(successPartitionTopicName, ledgerMock, brokerService);

        CommandSubscribe cmd2 = CommandSubscribe.newBuilder().setConsumerId(1).setConsumerName("C1")
                .setTopic(successPartitionTopicName).setSubscription(successSubName).setRequestId(1)
                .setSubType(SubType.Failover).build();

        Future<Consumer> f2 = topic2.subscribe(serverCnx, cmd2.getSubscription(), cmd2.getConsumerId(),
                cmd2.getSubType(), 0, cmd2.getConsumerName());
        f2.get();

        // 3. Subscribe and create second consumer
        CommandSubscribe cmd3 = CommandSubscribe.newBuilder().setConsumerId(2).setConsumerName("C2")
                .setTopic(successPartitionTopicName).setSubscription(successSubName).setRequestId(1)
                .setSubType(SubType.Failover).build();

        Future<Consumer> f3 = topic2.subscribe(serverCnx, cmd3.getSubscription(), cmd3.getConsumerId(),
                cmd3.getSubType(), 0, cmd3.getConsumerName());
        f3.get();

        assertEquals(
                topic2.getPersistentSubscription(successSubName).getDispatcher().getConsumers().get(0).consumerId(), 1);
        assertEquals(
                topic2.getPersistentSubscription(successSubName).getDispatcher().getConsumers().get(0).consumerName(),
                "C1");
        assertEquals(
                topic2.getPersistentSubscription(successSubName).getDispatcher().getConsumers().get(1).consumerId(), 2);
        assertEquals(
                topic2.getPersistentSubscription(successSubName).getDispatcher().getConsumers().get(1).consumerName(),
                "C2");

        // 4. Subscribe and create third duplicate consumer
        CommandSubscribe cmd4 = CommandSubscribe.newBuilder().setConsumerId(3).setConsumerName("C1")
                .setTopic(successPartitionTopicName).setSubscription(successSubName).setRequestId(1)
                .setSubType(SubType.Failover).build();

        Future<Consumer> f4 = topic2.subscribe(serverCnx, cmd4.getSubscription(), cmd4.getConsumerId(),
                cmd4.getSubType(), 0, cmd4.getConsumerName());
        f4.get();

        assertEquals(
                topic2.getPersistentSubscription(successSubName).getDispatcher().getConsumers().get(0).consumerId(), 1);
        assertEquals(
                topic2.getPersistentSubscription(successSubName).getDispatcher().getConsumers().get(0).consumerName(),
                "C1");
        assertEquals(
                topic2.getPersistentSubscription(successSubName).getDispatcher().getConsumers().get(1).consumerId(), 3);
        assertEquals(
                topic2.getPersistentSubscription(successSubName).getDispatcher().getConsumers().get(1).consumerName(),
                "C1");
        assertEquals(
                topic2.getPersistentSubscription(successSubName).getDispatcher().getConsumers().get(2).consumerId(), 2);
        assertEquals(
                topic2.getPersistentSubscription(successSubName).getDispatcher().getConsumers().get(2).consumerName(),
                "C2");

        // 5. Subscribe on partition topic with existing consumer id and different sub type
        CommandSubscribe cmd5 = CommandSubscribe.newBuilder().setConsumerId(2).setConsumerName("C1")
                .setTopic(successPartitionTopicName).setSubscription(successSubName).setRequestId(1)
                .setSubType(SubType.Exclusive).build();

        Future<Consumer> f5 = topic2.subscribe(serverCnx, cmd5.getSubscription(), cmd5.getConsumerId(),
                cmd5.getSubType(), 0, cmd5.getConsumerName());

        try {
            f5.get();
            fail("should fail with exception");
        } catch (ExecutionException ee) {
            // Expected
            assertTrue(ee.getCause() instanceof BrokerServiceException.SubscriptionBusyException);
        }

        // 6. Subscribe on partition topic with different sub name, type and different consumer id
        CommandSubscribe cmd6 = CommandSubscribe.newBuilder().setConsumerId(4).setConsumerName("C3")
                .setTopic(successPartitionTopicName).setSubscription(successSubName2).setRequestId(1)
                .setSubType(SubType.Exclusive).build();

        Future<Consumer> f6 = topic2.subscribe(serverCnx, cmd6.getSubscription(), cmd6.getConsumerId(),
                cmd6.getSubType(), 0, cmd6.getConsumerName());
        f6.get();

        // 7. unsubscribe exclusive sub
        Future<Void> f7 = topic2.unsubscribe(successSubName2);
        f7.get();

        assertNull(topic2.getPersistentSubscription(successSubName2));

        // 8. unsubscribe active consumer from shared sub.
        PersistentSubscription sub = topic2.getPersistentSubscription(successSubName);
        Consumer cons = sub.getDispatcher().getConsumers().get(0);
        sub.removeConsumer(cons);

        // Verify second consumer become active
        assertEquals(
                topic2.getPersistentSubscription(successSubName).getDispatcher().getConsumers().get(0).consumerId(), 3);
        assertEquals(
                topic2.getPersistentSubscription(successSubName).getDispatcher().getConsumers().get(0).consumerName(),
                "C1");

        // 9. unsubscribe active consumer from shared sub.
        cons = sub.getDispatcher().getConsumers().get(0);
        sub.removeConsumer(cons);

        // Verify second consumer become active
        assertEquals(
                topic2.getPersistentSubscription(successSubName).getDispatcher().getConsumers().get(0).consumerId(), 2);
        assertEquals(
                topic2.getPersistentSubscription(successSubName).getDispatcher().getConsumers().get(0).consumerName(),
                "C2");

        // 10. unsubscribe shared sub
        Future<Void> f8 = topic2.unsubscribe(successSubName);
        f8.get();

        assertNull(topic2.getPersistentSubscription(successSubName));
    }

    /**
     * {@link PersistentReplicator.removeReplicator} doesn't remove replicator in atomic way and does in multiple step:
     * 1. disconnect replicator producer
     * <p>
     * 2. close cursor
     * <p>
     * 3. remove from replicator-list.
     * <p>
     * 
     * If we try to startReplicationProducer before step-c finish then it should not avoid restarting repl-producer.
     * 
     * @throws Exception
     */
    @Test
    public void testAtomicReplicationRemoval() throws Exception {
        final String globalTopicName = "persistent://prop/global/ns-abc/successTopic";
        String localCluster = "local";
        String remoteCluster = "remote";
        final ManagedLedger ledgerMock = mock(ManagedLedger.class);
        doNothing().when(ledgerMock).asyncDeleteCursor(anyObject(), anyObject(), anyObject());
        doReturn(new ArrayList<Object>()).when(ledgerMock).getCursors();

        PersistentTopic topic = new PersistentTopic(globalTopicName, ledgerMock, brokerService);
        String remoteReplicatorName = topic.replicatorPrefix + "." + remoteCluster;
        ConcurrentOpenHashMap<String, PersistentReplicator> replicatorMap = topic.getReplicators();
        
        final URL brokerUrl = new URL(
                "http://" + pulsar.getAdvertisedAddress() + ":" + pulsar.getConfiguration().getBrokerServicePort());
        PulsarClient client = PulsarClient.create(brokerUrl.toString());
        ManagedCursor cursor = mock(ManagedCursorImpl.class);
        doReturn(remoteCluster).when(cursor).getName();
        brokerService.getReplicationClients().put(remoteCluster, client);
        PersistentReplicator replicator = spy(
                new PersistentReplicator(topic, cursor, localCluster, remoteCluster, brokerService));
        replicatorMap.put(remoteReplicatorName, replicator);

        // step-1 remove replicator : it will disconnect the producer but it will wait for callback to be completed
        Method removeMethod = PersistentTopic.class.getDeclaredMethod("removeReplicator", String.class);
        removeMethod.setAccessible(true);
        removeMethod.invoke(topic, remoteReplicatorName);

        // step-2 now, policies doesn't have removed replication cluster so, it should not invoke "startProducer" of the
        // replicator
        when(pulsar.getConfigurationCache().policiesCache()
                .get(AdminResource.path("policies", DestinationName.get(globalTopicName).getNamespace())))
                        .thenReturn(Optional.of(new Policies()));
        // try to start replicator again
        topic.startReplProducers();
        // verify: replicator.startProducer is not invoked
        verify(replicator, Mockito.times(0)).startProducer();

        // step-3 : complete the callback to remove replicator from the list
        ArgumentCaptor<DeleteCursorCallback> captor = ArgumentCaptor.forClass(DeleteCursorCallback.class);
        Mockito.verify(ledgerMock).asyncDeleteCursor(anyObject(), captor.capture(), anyObject());
        DeleteCursorCallback callback = captor.getValue();
        callback.deleteCursorComplete(null);
    }

    @Test
    public void testClosingReplicationProducerTwice() throws Exception {
        final String globalTopicName = "persistent://prop/global/ns/testClosingReplicationProducerTwice";
        String localCluster = "local";
        String remoteCluster = "remote";
        final ManagedLedger ledgerMock = mock(ManagedLedger.class);
        doNothing().when(ledgerMock).asyncDeleteCursor(anyObject(), anyObject(), anyObject());
        doReturn(new ArrayList<Object>()).when(ledgerMock).getCursors();

        PersistentTopic topic = new PersistentTopic(globalTopicName, ledgerMock, brokerService);
        String remoteReplicatorName = topic.replicatorPrefix + "." + localCluster;
        
        final URL brokerUrl = new URL(
                "http://" + pulsar.getAdvertisedAddress() + ":" + pulsar.getConfiguration().getBrokerServicePort());
        PulsarClient client =  spy( PulsarClient.create(brokerUrl.toString()) );
        PulsarClientImpl clientImpl = (PulsarClientImpl) client;
        Field conf = PersistentReplicator.class.getDeclaredField("producerConfiguration");
        conf.setAccessible(true);
        
        ManagedCursor cursor = mock(ManagedCursorImpl.class);
        doReturn(remoteCluster).when(cursor).getName();
        brokerService.getReplicationClients().put(remoteCluster, client);
        PersistentReplicator replicator = new PersistentReplicator(topic, cursor, localCluster, remoteCluster, brokerService);

        doReturn(new CompletableFuture<Producer>()).when(clientImpl).createProducerAsync(globalTopicName, (ProducerConfiguration) conf.get(replicator), remoteReplicatorName);
    
        replicator.startProducer();
        verify(clientImpl).createProducerAsync(globalTopicName, (ProducerConfiguration) conf.get(replicator), remoteReplicatorName);
        
        replicator.disconnect(false);
        replicator.disconnect(false);
        
        replicator.startProducer();

        verify(clientImpl, Mockito.times(2)).createProducerAsync(globalTopicName, (ProducerConfiguration) conf.get(replicator), remoteReplicatorName);       
    }

}
