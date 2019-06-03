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
package org.apache.pulsar.broker.service.persistent;

import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.cache.ConfigurationCacheService;
import org.apache.pulsar.broker.cache.LocalZooKeeperCacheService;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.PersistentTopicTest;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.compaction.Compactor;
import org.apache.pulsar.transaction.common.exception.TransactionConflictException;
import org.apache.pulsar.transaction.impl.common.TxnID;
import org.apache.pulsar.zookeeper.ZooKeeperCache;
import org.apache.pulsar.zookeeper.ZooKeeperDataCache;
import org.apache.zookeeper.ZooKeeper;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest.createMockBookKeeper;
import static org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest.createMockZooKeeper;
import static org.apache.pulsar.common.api.proto.PulsarApi.CommandAck.AckType;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@PrepareForTest({ ZooKeeperDataCache.class, BrokerService.class })
public class PersistentSubscriptionTest {

    private PulsarService pulsarMock;
    private BrokerService brokerMock;
    private ManagedLedgerFactory mlFactoryMock;
    private ManagedLedger ledgerMock;
    private ManagedCursor cursorMock;
    private ConfigurationCacheService configCacheServiceMock;
    private PersistentTopic topic;
    private PersistentSubscription persistentSubscription;

    final String successTopicName = "persistent://prop/use/ns-abc/successTopic";
    final String subName = "subscriptionName";

    final TxnID txnID1 = new TxnID(1,1);
    final TxnID txnID2 = new TxnID(1,2);

    private static final Logger log = LoggerFactory.getLogger(PersistentTopicTest.class);

    @BeforeMethod
    public void setup() throws Exception {
        ServiceConfiguration svcConfig = spy(new ServiceConfiguration());
        pulsarMock = spy(new PulsarService(svcConfig));
        doReturn(svcConfig).when(pulsarMock).getConfiguration();
        doReturn(mock(Compactor.class)).when(pulsarMock).getCompactor();

        mlFactoryMock = mock(ManagedLedgerFactory.class);
        doReturn(mlFactoryMock).when(pulsarMock).getManagedLedgerFactory();

        ZooKeeper zkMock = createMockZooKeeper();
        doReturn(zkMock).when(pulsarMock).getZkClient();
        doReturn(createMockBookKeeper(zkMock, pulsarMock.getOrderedExecutor().chooseThread(0)))
                .when(pulsarMock).getBookKeeperClient();

        ZooKeeperCache cache = mock(ZooKeeperCache.class);
        doReturn(30).when(cache).getZkOperationTimeoutSeconds();
        CompletableFuture getDataFuture = new CompletableFuture();
        getDataFuture.complete(Optional.empty());
        doReturn(getDataFuture).when(cache).getDataAsync(anyString(), any(), any());
        doReturn(cache).when(pulsarMock).getLocalZkCache();

        configCacheServiceMock = mock(ConfigurationCacheService.class);
        @SuppressWarnings("unchecked")
        ZooKeeperDataCache<Policies> zkPoliciesDataCacheMock = mock(ZooKeeperDataCache.class);
        doReturn(zkPoliciesDataCacheMock).when(configCacheServiceMock).policiesCache();
        doReturn(configCacheServiceMock).when(pulsarMock).getConfigurationCache();
        doReturn(Optional.empty()).when(zkPoliciesDataCacheMock).get(anyString());

        LocalZooKeeperCacheService zkCacheMock = mock(LocalZooKeeperCacheService.class);
        doReturn(CompletableFuture.completedFuture(Optional.empty())).when(zkPoliciesDataCacheMock).getAsync(any());
        doReturn(zkPoliciesDataCacheMock).when(zkCacheMock).policiesCache();
        doReturn(zkCacheMock).when(pulsarMock).getLocalZkCacheService();

        brokerMock = spy(new BrokerService(pulsarMock));
        doNothing().when(brokerMock).unloadNamespaceBundlesGracefully();
        doReturn(brokerMock).when(pulsarMock).getBrokerService();

        ledgerMock = mock(ManagedLedger.class);
        cursorMock = mock(ManagedCursor.class);
        doReturn(new ArrayList<Object>()).when(ledgerMock).getCursors();
        doReturn("mockCursor").when(cursorMock).getName();

        topic = new PersistentTopic(successTopicName, ledgerMock, brokerMock);

        persistentSubscription = new PersistentSubscription(topic, subName, cursorMock);
    }

    @AfterMethod
    public void teardown() throws Exception {
        brokerMock.close(); //to clear pulsarStats
        try {
            pulsarMock.close();
        } catch (Exception e) {
            log.warn("Failed to close pulsar service", e);
            throw e;
        }
    }

    @Test
    public void testCanAcknowledgeAndCommitForTransaction() throws TransactionConflictException {
        List<Position> expectedSinglePositions = new ArrayList<>();
        expectedSinglePositions.add(new PositionImpl(1, 1));
        expectedSinglePositions.add(new PositionImpl(1, 3));
        expectedSinglePositions.add(new PositionImpl(1, 5));

        doAnswer((invocationOnMock) -> {
            assertTrue(((List)invocationOnMock.getArguments()[0]).containsAll(expectedSinglePositions));
            ((AsyncCallbacks.DeleteCallback) invocationOnMock.getArguments()[1])
                    .deleteComplete(invocationOnMock.getArguments()[2]);
            return null;
        }).when(cursorMock).asyncDelete(anyList(), any(AsyncCallbacks.DeleteCallback.class), anyObject());

        doAnswer((invocationOnMock) -> {
            assertTrue(((PositionImpl)invocationOnMock.getArguments()[0]).compareTo(new PositionImpl(3, 100)) == 0);
            ((AsyncCallbacks.MarkDeleteCallback) invocationOnMock.getArguments()[2])
                    .markDeleteComplete(invocationOnMock.getArguments()[3]);
            return null;
        }).when(cursorMock).asyncMarkDelete(anyObject(), anyObject(), any(AsyncCallbacks.MarkDeleteCallback.class), anyObject());

        List<Position> positions = new ArrayList<>();
        positions.add(new PositionImpl(1, 1));
        positions.add(new PositionImpl(1, 3));
        positions.add(new PositionImpl(1, 5));

        // Single ack for txn
        persistentSubscription.acknowledgeMessage(txnID1, positions, AckType.Individual);

        positions.clear();
        positions.add(new PositionImpl(3, 100));

        // Cumulative ack for txn
        persistentSubscription.acknowledgeMessage(txnID1, positions, AckType.Cumulative);

        // Commit txn
        persistentSubscription.commitTxn(txnID1, Collections.emptyMap());

        // Verify corresponding ledger method was called with expected args.
        verify(cursorMock, times(1)).asyncDelete(anyList(), any(), any());
        verify(cursorMock, times(1)).asyncMarkDelete(any(), anyMap(), anyObject(), any());
    }

    @Test
    public void testCanAcknowledgeAndAbortForTransaction() throws TransactionConflictException {
        List<Position> positions = new ArrayList<>();
        positions.add(new PositionImpl(1, 1));
        positions.add(new PositionImpl(1, 3));
        positions.add(new PositionImpl(1, 5));

        // Single ack for txn1
        persistentSubscription.acknowledgeMessage(txnID1, positions, AckType.Individual);

        positions.clear();
        positions.add(new PositionImpl(3, 100));

        // Cumulative ack for txn1
        persistentSubscription.acknowledgeMessage(txnID1, positions, AckType.Cumulative);

        positions.clear();
        positions.add(new PositionImpl(1, 1));

        // Can not single ack message already acked.
        try {
            persistentSubscription.acknowledgeMessage(txnID2, positions, AckType.Individual);
            fail("Single acknowledge for transaction2 should fail. ");
        } catch (TransactionConflictException e) {
            assertEquals(e.getMessage(),"[persistent://prop/use/ns-abc/successTopic][subscriptionName] " +
                    "Transaction:(1,2) try to ack message:1:1 already acked before.");
        }

        positions.clear();
        positions.add(new PositionImpl(3, 100));

        // Can not cumulative ack message for another txn.
        try {
            persistentSubscription.acknowledgeMessage(txnID2, positions, AckType.Cumulative);
            fail("Cumulative acknowledge for transaction2 should fail. ");
        } catch (TransactionConflictException e) {
            System.out.println(e.getMessage());
            assertEquals(e.getMessage(),"[persistent://prop/use/ns-abc/successTopic][subscriptionName] " +
                "Transaction:(1,2) try to cumulative ack message while transaction:(1,1) already cumulative acked messages.");
        }

        //Abort txn.
        persistentSubscription.abortTxn(txnID1);

        // Retry above ack, will succeed. As abort has clear pending_ack for those messages.
        persistentSubscription.acknowledgeMessage(txnID2, positions, AckType.Cumulative);

        positions.clear();
        positions.add(new PositionImpl(1, 1));

        persistentSubscription.acknowledgeMessage(txnID2, positions, AckType.Individual);
    }
}
