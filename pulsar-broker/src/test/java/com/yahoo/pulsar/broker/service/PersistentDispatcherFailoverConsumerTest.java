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
import static org.mockito.Matchers.matches;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertFalse;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

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
import org.apache.zookeeper.ZooKeeper;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.yahoo.pulsar.common.api.proto.PulsarApi.CommandSubscribe.SubType;
import com.yahoo.pulsar.common.naming.DestinationName;
import com.yahoo.pulsar.common.naming.ServiceUnitId;
import com.yahoo.pulsar.common.policies.data.Policies;
import com.yahoo.pulsar.broker.PulsarService;
import com.yahoo.pulsar.broker.ServiceConfiguration;
import com.yahoo.pulsar.broker.cache.ConfigurationCacheService;
import com.yahoo.pulsar.broker.namespace.NamespaceService;
import com.yahoo.pulsar.broker.service.Consumer;
import com.yahoo.pulsar.broker.service.BrokerService;
import com.yahoo.pulsar.broker.service.ServerCnx;
import com.yahoo.pulsar.broker.service.persistent.PersistentDispatcherSingleActiveConsumer;
import com.yahoo.pulsar.broker.service.persistent.PersistentSubscription;
import com.yahoo.pulsar.broker.service.persistent.PersistentTopic;
import com.yahoo.pulsar.zookeeper.ZooKeeperDataCache;

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

        ZooKeeper mockZk = mock(ZooKeeper.class);
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
        doReturn(true).when(nsSvc).isServiceUnitOwned(any(ServiceUnitId.class));
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
        PersistentSubscription sub = new PersistentSubscription(topic, cursorMock);

        int partitionIndex = 0;
        PersistentDispatcherSingleActiveConsumer pdfc = new PersistentDispatcherSingleActiveConsumer(cursorMock,
                SubType.Failover, partitionIndex, topic);

        // 1. Verify no consumers connected
        assertFalse(pdfc.isConsumerConnected());

        // 2. Add consumer
        Consumer consumer1 = new Consumer(sub, SubType.Exclusive, 1 /* consumer id */, "Cons1"/* consumer name */,
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
        Consumer consumer2 = new Consumer(sub, SubType.Exclusive, 2 /* consumer id */, "Cons2"/* consumer name */,
                50000, serverCnx, "myrole-1");
        pdfc.addConsumer(consumer2);
        consumers = pdfc.getConsumers();
        assertTrue(pdfc.getActiveConsumer().consumerName() == consumer1.consumerName());
        assertEquals(3, consumers.size());

        // 6. Add a consumer which changes active consumer
        Consumer consumer0 = new Consumer(sub, SubType.Exclusive, 0 /* consumer id */, "Cons0"/* consumer name */,
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

    private static final Logger log = LoggerFactory.getLogger(PersistentDispatcherFailoverConsumerTest.class);

}
