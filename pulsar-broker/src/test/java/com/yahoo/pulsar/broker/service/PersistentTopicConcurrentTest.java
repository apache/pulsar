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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;

import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;

import com.google.common.collect.Lists;
import com.yahoo.pulsar.common.api.proto.PulsarApi;
import com.yahoo.pulsar.common.naming.DestinationName;
import com.yahoo.pulsar.common.naming.NamespaceBundle;
import com.yahoo.pulsar.common.util.collections.ConcurrentOpenHashMap;
import com.yahoo.pulsar.broker.PulsarService;
import com.yahoo.pulsar.broker.ServiceConfiguration;
import com.yahoo.pulsar.broker.namespace.NamespaceService;
import com.yahoo.pulsar.broker.service.Consumer;
import com.yahoo.pulsar.broker.service.BrokerService;
import com.yahoo.pulsar.broker.service.ServerCnx;
import com.yahoo.pulsar.broker.service.persistent.PersistentSubscription;
import com.yahoo.pulsar.broker.service.persistent.PersistentTopic;

/**
 */
public class PersistentTopicConcurrentTest extends MockedBookKeeperTestCase {
    private BrokerService brokerService;
    private ManagedLedgerFactory mlFactoryMock;
    private ServerCnx serverCnx;
    private ManagedLedger ledgerMock;
    private ManagedCursor cursorMock;

    final String successTopicName = "persistent://prop/use/ns-abc/successTopic";
    final String failTopicName = "persistent://prop/use/ns-abc/failTopic";
    final String successSubName = "successSub";
    private static final Logger log = LoggerFactory.getLogger(PersistentTopicTest.class);

    @BeforeMethod
    public void setup(Method m) throws Exception {
        super.setUp(m);
        ServiceConfiguration svcConfig = spy(new ServiceConfiguration());
        PulsarService pulsar = spy(new PulsarService(svcConfig));
        doReturn(svcConfig).when(pulsar).getConfiguration();

        mlFactoryMock = mock(ManagedLedgerFactory.class);
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(bkc, bkc.getZkHandle());
        ManagedLedger ledger = factory.open("my_test_ledger", new ManagedLedgerConfig().setMaxEntriesPerLedger(2));
        final ManagedCursor cursor = ledger.openCursor("c1");
        cursorMock = cursor;
        ledgerMock = ledger;
        mlFactoryMock = factory;
        doReturn(mlFactoryMock).when(pulsar).getManagedLedgerFactory();

        ZooKeeper mockZk = mock(ZooKeeper.class);
        doReturn(mockZk).when(pulsar).getZkClient();

        brokerService = spy(new BrokerService(pulsar));
        doReturn(brokerService).when(pulsar).getBrokerService();

        serverCnx = spy(new ServerCnx(brokerService));
        doReturn(true).when(serverCnx).isActive();

        NamespaceService nsSvc = mock(NamespaceService.class);
        doReturn(nsSvc).when(pulsar).getNamespaceService();
        doReturn(true).when(nsSvc).isServiceUnitOwned(any(NamespaceBundle.class));
        doReturn(true).when(nsSvc).isServiceUnitActive(any(DestinationName.class));

        final List<Position> addedEntries = Lists.newArrayList();

        for (int i = 0; i < 100; i++) {
            Position pos = ledger.addEntry("entry".getBytes());
            addedEntries.add(pos);
        }
    }

    // @Test
    public void testConcurrentTopicAndSubscriptionDelete() throws Exception {
        // create topic
        final PersistentTopic topic = (PersistentTopic) brokerService.getTopic(successTopicName).get();
        PulsarApi.CommandSubscribe cmd = PulsarApi.CommandSubscribe.newBuilder().setConsumerId(1)
                .setTopic(successTopicName).setSubscription(successSubName).setRequestId(1)
                .setSubType(PulsarApi.CommandSubscribe.SubType.Exclusive).build();

        Future<Consumer> f1 = topic.subscribe(serverCnx, cmd.getSubscription(), cmd.getConsumerId(), cmd.getSubType(),
                0, cmd.getConsumerName());
        f1.get();

        final CyclicBarrier barrier = new CyclicBarrier(2);
        final CountDownLatch counter = new CountDownLatch(2);
        final AtomicBoolean gotException = new AtomicBoolean(false);

        Thread deleter = new Thread() {
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
            public void run() {
                try {
                    barrier.await();
                    // do subscription delete
                    ConcurrentOpenHashMap<String, PersistentSubscription> subscriptions = topic.getSubscriptions();
                    PersistentSubscription ps = subscriptions.get(successSubName);
                    // Thread.sleep(2,0);
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

    // @Test
    public void testConcurrentTopicGCAndSubscriptionDelete() throws Exception {
        // create topic
        final PersistentTopic topic = (PersistentTopic) brokerService.getTopic(successTopicName).get();
        PulsarApi.CommandSubscribe cmd = PulsarApi.CommandSubscribe.newBuilder().setConsumerId(1)
                .setTopic(successTopicName).setSubscription(successSubName).setRequestId(1)
                .setSubType(PulsarApi.CommandSubscribe.SubType.Exclusive).build();

        Future<Consumer> f1 = topic.subscribe(serverCnx, cmd.getSubscription(), cmd.getConsumerId(), cmd.getSubType(),
                0, cmd.getConsumerName());
        f1.get();

        final CyclicBarrier barrier = new CyclicBarrier(2);
        final CountDownLatch counter = new CountDownLatch(2);
        final AtomicBoolean gotException = new AtomicBoolean(false);

        Thread deleter = new Thread() {
            public void run() {
                try {
                    barrier.await();
                    // assertTrue(topic.unsubscribe(successSubName).isDone());
                    // Thread.sleep(5,0);
                    log.info("{} forcing topic GC ", Thread.currentThread());
                    for (int i = 0; i < 2000; i++) {
                        topic.checkGC(0);
                    }
                    log.info("GC done..");
                } catch (Exception e) {
                    e.printStackTrace();
                    gotException.set(true);
                } finally {
                    counter.countDown();
                }
            }
        };

        Thread unsubscriber = new Thread() {
            public void run() {
                try {
                    barrier.await();
                    // do subscription delete
                    ConcurrentOpenHashMap<String, PersistentSubscription> subscriptions = topic.getSubscriptions();
                    PersistentSubscription ps = subscriptions.get(successSubName);
                    // Thread.sleep(2,0);
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

    // @Test
    public void testConcurrentTopicDeleteAndUnsubscribe() throws Exception {
        // create topic
        final PersistentTopic topic = (PersistentTopic) brokerService.getTopic(successTopicName).get();
        PulsarApi.CommandSubscribe cmd = PulsarApi.CommandSubscribe.newBuilder().setConsumerId(1)
                .setTopic(successTopicName).setSubscription(successSubName).setRequestId(1)
                .setSubType(PulsarApi.CommandSubscribe.SubType.Exclusive).build();

        Future<Consumer> f1 = topic.subscribe(serverCnx, cmd.getSubscription(), cmd.getConsumerId(), cmd.getSubType(),
                0, cmd.getConsumerName());
        f1.get();

        final CyclicBarrier barrier = new CyclicBarrier(2);
        final CountDownLatch counter = new CountDownLatch(2);
        final AtomicBoolean gotException = new AtomicBoolean(false);

        Thread deleter = new Thread() {
            public void run() {
                try {
                    barrier.await();
                    Thread.sleep(4, 700);
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
            public void run() {
                try {
                    barrier.await();
                    // Thread.sleep(2,0);
                    // assertTrue(topic.unsubscribe(successSubName).isDone());
                    ConcurrentOpenHashMap<String, PersistentSubscription> subscriptions = topic.getSubscriptions();
                    PersistentSubscription ps = subscriptions.get(successSubName);
                    log.info("unsubscribe result : {}", topic.unsubscribe(successSubName).get());
                    log.info("closing consumer..");
                    ps.getConsumers().get(0).close();
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
    public void testConcurrentTopicDeleteAndSubsUnsubscribe() throws Exception {
        // create topic
        final PersistentTopic topic = (PersistentTopic) brokerService.getTopic(successTopicName).get();
        PulsarApi.CommandSubscribe cmd = PulsarApi.CommandSubscribe.newBuilder().setConsumerId(1)
                .setTopic(successTopicName).setSubscription(successSubName).setRequestId(1)
                .setSubType(PulsarApi.CommandSubscribe.SubType.Exclusive).build();

        Future<Consumer> f1 = topic.subscribe(serverCnx, cmd.getSubscription(), cmd.getConsumerId(), cmd.getSubType(),
                0, cmd.getConsumerName());
        f1.get();

        final CyclicBarrier barrier = new CyclicBarrier(2);
        final CountDownLatch counter = new CountDownLatch(2);
        final AtomicBoolean gotException = new AtomicBoolean(false);

        Thread deleter = new Thread() {
            public void run() {
                try {
                    barrier.await();
                    Thread.sleep(4, 730);
                    log.info("@@@@@@@@ DELETER TH");
                    log.info("deleter outcome is " + topic.delete().get());
                } catch (Exception e) {
                    e.printStackTrace();
                    gotException.set(true);
                } finally {
                    counter.countDown();
                }
            }
        };

        Thread unsubscriber = new Thread() {
            public void run() {
                try {
                    barrier.await();
                    log.info("&&&&&&&&& UNSUBSCRIBER TH");
                    // Thread.sleep(2,0);
                    // assertTrue(topic.unsubscribe(successSubName).isDone());
                    ConcurrentOpenHashMap<String, PersistentSubscription> subscriptions = topic.getSubscriptions();
                    PersistentSubscription ps = subscriptions.get(successSubName);
                    log.info("unsubscribe result : " + ps.doUnsubscribe(ps.getConsumers().get(0)).get());
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
}
