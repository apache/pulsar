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

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertFalse;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import lombok.Cleanup;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.transaction.TransactionTestBase;
import org.apache.pulsar.common.api.proto.CommandSubscribe;
import org.apache.pulsar.common.api.proto.CommandSubscribe.InitialPosition;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.policies.data.InactiveTopicDeleteMode;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;

import com.google.common.collect.Lists;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class PersistentTopicConcurrentTest extends MockedBookKeeperTestCase {

    private BrokerService brokerService;
    private ManagedLedgerFactory mlFactoryMock;
    private ServerCnx serverCnx;
    @SuppressWarnings("unused")
    private ManagedLedger ledgerMock;
    @SuppressWarnings("unused")
    private ManagedCursor cursorMock;

    final String successTopicName = "persistent://prop/use/ns-abc/successTopic";
    final String successSubName = "successSub";
    private static final Logger log = LoggerFactory.getLogger(PersistentTopicTest.class);

    @BeforeMethod
    public void setup(Method m) throws Exception {
        super.setUp(m);
        ServiceConfiguration svcConfig = spy(new ServiceConfiguration());
        svcConfig.setBrokerShutdownTimeoutMs(0L);
        @Cleanup
        PulsarService pulsar = spy(new PulsarService(svcConfig));
        doReturn(svcConfig).when(pulsar).getConfiguration();

        @Cleanup(value = "shutdownGracefully")
        EventLoopGroup eventLoopGroup = new NioEventLoopGroup();

        mlFactoryMock = mock(ManagedLedgerFactory.class);
        ManagedLedgerFactory factory = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ManagedLedger ledger = factory.open("my_test_ledger", new ManagedLedgerConfig().setMaxEntriesPerLedger(2));
        cursorMock = ledger.openCursor("c1");
        ledgerMock = ledger;
        mlFactoryMock = factory;
        doReturn(mlFactoryMock).when(pulsar).getManagedLedgerFactory();

        brokerService = spy(new BrokerService(pulsar, eventLoopGroup));
        doReturn(brokerService).when(pulsar).getBrokerService();

        serverCnx = spy(new ServerCnx(pulsar));
        doReturn(true).when(serverCnx).isActive();

        NamespaceService nsSvc = mock(NamespaceService.class);
        doReturn(nsSvc).when(pulsar).getNamespaceService();
        doReturn(true).when(nsSvc).isServiceUnitOwned(any(NamespaceBundle.class));
        doReturn(true).when(nsSvc).isServiceUnitActive(any(TopicName.class));
        doReturn(CompletableFuture.completedFuture(true)).when(nsSvc).checkTopicOwnership(any(TopicName.class));

        final List<Position> addedEntries = Lists.newArrayList();

        for (int i = 0; i < 100; i++) {
            Position pos = ledger.addEntry("entry".getBytes());
            addedEntries.add(pos);
        }
    }

    @Test(enabled = false)
    public void testConcurrentTopicAndSubscriptionDelete() throws Exception {
        // create topic
        final PersistentTopic topic = (PersistentTopic) brokerService.getOrCreateTopic(successTopicName).get();
        CommandSubscribe cmd = new CommandSubscribe()
                .setConsumerId(1)
                .setTopic(successTopicName)
                .setSubscription(successSubName)
                .setRequestId(1)
                .setSubType(CommandSubscribe.SubType.Exclusive);

        Future<Consumer> f1 = topic.subscribe(serverCnx, cmd.getSubscription(), cmd.getConsumerId(), cmd.getSubType(),
                0, cmd.getConsumerName(), cmd.isDurable(), null, Collections.emptyMap(), cmd.isReadCompacted(), InitialPosition.Latest,
                0 /*avoid reseting cursor*/, false /* replicated */, null);
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
        assertFalse(gotException.get());
    }

    @Test(enabled = false)
    public void testConcurrentTopicGCAndSubscriptionDelete() throws Exception {
        // create topic
        final PersistentTopic topic = (PersistentTopic) brokerService.getOrCreateTopic(successTopicName).get();
        CommandSubscribe cmd = new CommandSubscribe()
                .setConsumerId(1)
                .setTopic(successTopicName)
                .setSubscription(successSubName)
                .setRequestId(1)
                .setSubType(CommandSubscribe.SubType.Exclusive);

        Future<Consumer> f1 = topic.subscribe(serverCnx, cmd.getSubscription(), cmd.getConsumerId(), cmd.getSubType(),
                0, cmd.getConsumerName(), cmd.isDurable(), null, Collections.emptyMap(), cmd.isReadCompacted(), InitialPosition.Latest,
                0 /*avoid reseting cursor*/, false /* replicated */, null);
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
                        topic.getInactiveTopicPolicies().setMaxInactiveDurationSeconds(0);
                        topic.getInactiveTopicPolicies().setInactiveTopicDeleteMode(InactiveTopicDeleteMode.delete_when_no_subscriptions);
                        topic.checkGC();
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
        assertFalse(gotException.get());
    }

    @Test(enabled = false)
    public void testConcurrentTopicDeleteAndUnsubscribe() throws Exception {
        // create topic
        final PersistentTopic topic = (PersistentTopic) brokerService.getOrCreateTopic(successTopicName).get();
        CommandSubscribe cmd = new CommandSubscribe()
                .setConsumerId(1)
                .setTopic(successTopicName)
                .setSubscription(successSubName)
                .setRequestId(1)
                .setSubType(CommandSubscribe.SubType.Exclusive);

        Future<Consumer> f1 = topic.subscribe(serverCnx, cmd.getSubscription(), cmd.getConsumerId(), cmd.getSubType(),
                0, cmd.getConsumerName(), cmd.isDurable(), null, Collections.emptyMap(), cmd.isReadCompacted(), InitialPosition.Latest,
                0 /*avoid reseting cursor*/, false /* replicated */, null);
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
        assertFalse(gotException.get());
    }

    @Test(enabled = false)
    public void testConcurrentTopicDeleteAndSubsUnsubscribe() throws Exception {
        // create topic
        final PersistentTopic topic = (PersistentTopic) brokerService.getOrCreateTopic(successTopicName).get();
        CommandSubscribe cmd = new CommandSubscribe()
                .setConsumerId(1)
                .setTopic(successTopicName)
                .setSubscription(successSubName)
                .setRequestId(1)
                .setSubType(CommandSubscribe.SubType.Exclusive);

        Future<Consumer> f1 = topic.subscribe(serverCnx, cmd.getSubscription(), cmd.getConsumerId(), cmd.getSubType(),
                0, cmd.getConsumerName(), cmd.isDurable(), null, Collections.emptyMap(), cmd.isReadCompacted(), InitialPosition.Latest,
                0 /*avoid reseting cursor*/, false /* replicated */, null);
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
        assertFalse(gotException.get());
    }
}
