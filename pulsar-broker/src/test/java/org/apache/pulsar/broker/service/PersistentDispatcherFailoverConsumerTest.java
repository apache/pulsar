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

import static org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest.createMockZooKeeper;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.matches;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertFalse;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.apache.bookkeeper.mledger.AsyncCallbacks.AddEntryCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteCursorCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteLedgerCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OpenCursorCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OpenLedgerCallback;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.cache.ConfigurationCacheService;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.broker.service.persistent.PersistentDispatcherMultipleConsumers;
import org.apache.pulsar.broker.service.persistent.PersistentDispatcherSingleActiveConsumer;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSubscribe.SubType;
import org.apache.pulsar.common.naming.DestinationName;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.zookeeper.ZooKeeperDataCache;
import org.apache.zookeeper.ZooKeeper;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PersistentDispatcherFailoverConsumerTest {

    private BrokerService brokerService;
    private ManagedLedgerFactory mlFactoryMock;
    private ServerCnx serverCnx;
    private ManagedLedger ledgerMock;
    private ManagedCursor cursorMock;
    private ConfigurationCacheService configCacheService;

    final String successTopicName = "persistent://part-perf/global/perf.t1/ptopic";
    final String failTopicName = "persistent://part-perf/global/perf.t1/pfailTopic";

    @BeforeMethod
    public void setup() throws Exception {
        ServiceConfiguration svcConfig = spy(new ServiceConfiguration());
        PulsarService pulsar = spy(new PulsarService(svcConfig));
        doReturn(svcConfig).when(pulsar).getConfiguration();

        mlFactoryMock = mock(ManagedLedgerFactory.class);
        doReturn(mlFactoryMock).when(pulsar).getManagedLedgerFactory();

        ZooKeeper mockZk = createMockZooKeeper();
        doReturn(mockZk).when(pulsar).getZkClient();

        configCacheService = mock(ConfigurationCacheService.class);
        @SuppressWarnings("unchecked")
        ZooKeeperDataCache<Policies> zkDataCache = mock(ZooKeeperDataCache.class);
        doReturn(zkDataCache).when(configCacheService).policiesCache();
        doReturn(configCacheService).when(pulsar).getConfigurationCache();

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

    void setupMLAsyncCallbackMocks() {
        ledgerMock = mock(ManagedLedger.class);
        cursorMock = mock(ManagedCursor.class);

        doReturn(new ArrayList<Object>()).when(ledgerMock).getCursors();
        doReturn("mockCursor").when(cursorMock).getName();

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
                ((AddEntryCallback) invocationOnMock.getArguments()[1]).addComplete(new PositionImpl(1, 1), null);
                return null;
            }
        }).when(ledgerMock).asyncAddEntry(any(byte[].class), any(AddEntryCallback.class), anyObject());

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
    public void testAddRemoveConsumer() throws Exception {
        log.info("--- Starting PersistentDispatcherFailoverConsumerTest::testAddConsumer ---");

        PersistentTopic topic = new PersistentTopic(successTopicName, ledgerMock, brokerService);
        PersistentSubscription sub = new PersistentSubscription(topic, "sub-1", cursorMock);

        int partitionIndex = 0;
        PersistentDispatcherSingleActiveConsumer pdfc = new PersistentDispatcherSingleActiveConsumer(cursorMock,
                SubType.Failover, partitionIndex, topic);

        // 1. Verify no consumers connected
        assertFalse(pdfc.isConsumerConnected());

        // 2. Add consumer
        Consumer consumer1 = new Consumer(sub, SubType.Exclusive, 1 /* consumer id */, 0, "Cons1"/* consumer name */,
                50000, serverCnx, "myrole-1");
        pdfc.addConsumer(consumer1);
        List<Consumer> consumers = pdfc.getConsumers();
        assertTrue(consumers.get(0).consumerName() == consumer1.consumerName());
        assertEquals(1, consumers.size());

        // 3. Add again, duplicate allowed
        pdfc.addConsumer(consumer1);
        consumers = pdfc.getConsumers();
        assertTrue(consumers.get(0).consumerName() == consumer1.consumerName());
        assertEquals(2, consumers.size());

        // 4. Verify active consumer
        assertTrue(pdfc.getActiveConsumer().consumerName() == consumer1.consumerName());

        // 5. Add another consumer which does not change active consumer
        Consumer consumer2 = new Consumer(sub, SubType.Exclusive, 2 /* consumer id */, 0, "Cons2"/* consumer name */,
                50000, serverCnx, "myrole-1");
        pdfc.addConsumer(consumer2);
        consumers = pdfc.getConsumers();
        assertTrue(pdfc.getActiveConsumer().consumerName() == consumer1.consumerName());
        assertEquals(3, consumers.size());

        // 6. Add a consumer which changes active consumer
        Consumer consumer0 = new Consumer(sub, SubType.Exclusive, 0 /* consumer id */, 0, "Cons0"/* consumer name */,
                50000, serverCnx, "myrole-1");
        pdfc.addConsumer(consumer0);
        consumers = pdfc.getConsumers();
        assertTrue(pdfc.getActiveConsumer().consumerName() == consumer0.consumerName());
        assertEquals(4, consumers.size());

        // 7. Remove last consumer
        pdfc.removeConsumer(consumer2);
        consumers = pdfc.getConsumers();
        assertTrue(pdfc.getActiveConsumer().consumerName() == consumer0.consumerName());
        assertEquals(3, consumers.size());

        // 8. Verify if we can unsubscribe when more than one consumer is connected
        assertFalse(pdfc.canUnsubscribe(consumer0));

        // 9. Remove active consumer
        pdfc.removeConsumer(consumer0);
        consumers = pdfc.getConsumers();
        assertTrue(pdfc.getActiveConsumer().consumerName() == consumer1.consumerName());
        assertEquals(2, consumers.size());

        // 10. Attempt to remove already removed consumer
        String cause = "";
        try {
            pdfc.removeConsumer(consumer0);
        } catch (Exception e) {
            cause = e.getMessage();
        }
        assertEquals(cause, "Consumer was not connected");

        // 11. Remove active consumer
        pdfc.removeConsumer(consumer1);
        consumers = pdfc.getConsumers();
        assertTrue(pdfc.getActiveConsumer().consumerName() == consumer1.consumerName());
        assertEquals(1, consumers.size());

        // 11. With only one consumer, unsubscribe is allowed
        assertTrue(pdfc.canUnsubscribe(consumer1));

    }
    
    @Test
    public void testMultipleDispatcherGetNextConsumerWithDifferentPriorityLevel() throws Exception {

        PersistentTopic topic = new PersistentTopic(successTopicName, ledgerMock, brokerService);
        PersistentDispatcherMultipleConsumers dispatcher = new PersistentDispatcherMultipleConsumers(topic, cursorMock);
        Consumer consumer1 = createConsumer(0, 2, false, 1);
        Consumer consumer2 = createConsumer(0, 2, false, 2);
        Consumer consumer3 = createConsumer(0, 2, false, 3);
        Consumer consumer4 = createConsumer(1, 2, false, 4);
        Consumer consumer5 = createConsumer(1, 1, false, 5);
        Consumer consumer6 = createConsumer(1, 2, false, 6);
        Consumer consumer7 = createConsumer(2, 1, false, 7);
        Consumer consumer8 = createConsumer(2, 1, false, 8);
        Consumer consumer9 = createConsumer(2, 1, false, 9);
        dispatcher.addConsumer(consumer1);
        dispatcher.addConsumer(consumer2);
        dispatcher.addConsumer(consumer3);
        dispatcher.addConsumer(consumer4);
        dispatcher.addConsumer(consumer5);
        dispatcher.addConsumer(consumer6);
        dispatcher.addConsumer(consumer7);
        dispatcher.addConsumer(consumer8);
        dispatcher.addConsumer(consumer9);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer1);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer2);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer3);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer1);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer2);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer3);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer4);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer5);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer6);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer4);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer6);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer7);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer8);
        // in between add upper priority consumer with more permits
        Consumer consumer10 = createConsumer(0, 2, false, 10);
        dispatcher.addConsumer(consumer10);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer10);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer10);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer9);

    }

    @Test
    public void testFewBlockedConsumerSamePriority() throws Exception{
        PersistentTopic topic = new PersistentTopic(successTopicName, ledgerMock, brokerService);
        PersistentDispatcherMultipleConsumers dispatcher = new PersistentDispatcherMultipleConsumers(topic, cursorMock);
        Consumer consumer1 = createConsumer(0, 2, false, 1);
        Consumer consumer2 = createConsumer(0, 2, false, 2);
        Consumer consumer3 = createConsumer(0, 2, false, 3);
        Consumer consumer4 = createConsumer(0, 2, false, 4);
        Consumer consumer5 = createConsumer(0, 1, true, 5);
        Consumer consumer6 = createConsumer(0, 2, true, 6);
        dispatcher.addConsumer(consumer1);
        dispatcher.addConsumer(consumer2);
        dispatcher.addConsumer(consumer3);
        dispatcher.addConsumer(consumer4);
        dispatcher.addConsumer(consumer5);
        dispatcher.addConsumer(consumer6);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer1);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer2);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer3);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer4);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer1);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer2);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer3);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer4);
        Assert.assertEquals(getNextConsumer(dispatcher), null);
    }

    @Test
    public void testFewBlockedConsumerDifferentPriority() throws Exception {
        PersistentTopic topic = new PersistentTopic(successTopicName, ledgerMock, brokerService);
        PersistentDispatcherMultipleConsumers dispatcher = new PersistentDispatcherMultipleConsumers(topic, cursorMock);
        Consumer consumer1 = createConsumer(0, 2, false, 1);
        Consumer consumer2 = createConsumer(0, 2, false, 2);
        Consumer consumer3 = createConsumer(0, 2, false, 3);
        Consumer consumer4 = createConsumer(0, 2, false, 4);
        Consumer consumer5 = createConsumer(0, 1, true, 5);
        Consumer consumer6 = createConsumer(0, 2, true, 6);
        Consumer consumer7 = createConsumer(1, 2, false, 7);
        Consumer consumer8 = createConsumer(1, 10, true, 8);
        Consumer consumer9 = createConsumer(1, 2, false, 9);
        Consumer consumer10 = createConsumer(2, 2, false, 10);
        Consumer consumer11 = createConsumer(2, 10, true, 11);
        Consumer consumer12 = createConsumer(2, 2, false, 12);
        dispatcher.addConsumer(consumer1);
        dispatcher.addConsumer(consumer2);
        dispatcher.addConsumer(consumer3);
        dispatcher.addConsumer(consumer4);
        dispatcher.addConsumer(consumer5);
        dispatcher.addConsumer(consumer6);
        dispatcher.addConsumer(consumer7);
        dispatcher.addConsumer(consumer8);
        dispatcher.addConsumer(consumer9);
        dispatcher.addConsumer(consumer10);
        dispatcher.addConsumer(consumer11);
        dispatcher.addConsumer(consumer12);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer1);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer2);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer3);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer4);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer1);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer2);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer3);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer4);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer7);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer9);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer7);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer9);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer10);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer12);
        // add consumer with lower priority again
        Consumer consumer13 = createConsumer(0, 2, false, 13);
        Consumer consumer14 = createConsumer(0, 2, true, 14);
        dispatcher.addConsumer(consumer13);
        dispatcher.addConsumer(consumer14);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer13);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer13);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer10);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer12);
        Assert.assertEquals(getNextConsumer(dispatcher), null);
    }

    @Test
    public void testFewBlockedConsumerDifferentPriority2() throws Exception {
        PersistentTopic topic = new PersistentTopic(successTopicName, ledgerMock, brokerService);
        PersistentDispatcherMultipleConsumers dispatcher = new PersistentDispatcherMultipleConsumers(topic, cursorMock);
        Consumer consumer1 = createConsumer(0, 2, true, 1);
        Consumer consumer2 = createConsumer(0, 2, true, 2);
        Consumer consumer3 = createConsumer(0, 2, true, 3);
        Consumer consumer4 = createConsumer(1, 2, false, 4);
        Consumer consumer5 = createConsumer(1, 1, false, 5);
        Consumer consumer6 = createConsumer(2, 1, false, 6);
        Consumer consumer7 = createConsumer(2, 2, true, 7);
        dispatcher.addConsumer(consumer1);
        dispatcher.addConsumer(consumer2);
        dispatcher.addConsumer(consumer3);
        dispatcher.addConsumer(consumer4);
        dispatcher.addConsumer(consumer5);
        dispatcher.addConsumer(consumer6);
        dispatcher.addConsumer(consumer7);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer4);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer5);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer4);
        Assert.assertEquals(getNextConsumer(dispatcher), consumer6);
        Assert.assertEquals(getNextConsumer(dispatcher), null);
    }

    private Consumer getNextConsumer(PersistentDispatcherMultipleConsumers dispatcher) throws Exception {
        
        Consumer consumer = dispatcher.getNextConsumer();
        
        if (consumer != null) {
            Field field = Consumer.class.getDeclaredField("MESSAGE_PERMITS_UPDATER");
            field.setAccessible(true);
            AtomicIntegerFieldUpdater<Consumer> messagePermits = (AtomicIntegerFieldUpdater) field.get(consumer);
            messagePermits.decrementAndGet(consumer);
            return consumer;
        }
        return null;
    }

    private Consumer createConsumer(int priority, int permit, boolean blocked, int id) throws Exception {
        Consumer consumer = new Consumer(null, SubType.Shared, id, priority, ""+id, 5000, serverCnx, "appId");
        try {
            consumer.flowPermits(permit);
        } catch (Exception e) {
        }
        // set consumer blocked flag
        Field blockField = Consumer.class.getDeclaredField("blockedConsumerOnUnackedMsgs");
        blockField.setAccessible(true);
        blockField.set(consumer, blocked);
        return consumer;
    }

    private static final Logger log = LoggerFactory.getLogger(PersistentDispatcherFailoverConsumerTest.class);

}
