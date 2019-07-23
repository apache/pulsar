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
package org.apache.pulsar.transaction.buffer.impl;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.matches;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.MoreExecutors;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.client.PulsarMockBookKeeper;
import org.apache.bookkeeper.mledger.AsyncCallbacks.AddEntryCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.CloseCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteCursorCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteLedgerCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.MarkDeleteCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OpenCursorCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OpenLedgerCallback;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.cache.ConfigurationCacheService;
import org.apache.pulsar.broker.cache.LocalZooKeeperCacheService;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSubscribe.InitialPosition;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.compaction.Compactor;
import org.apache.pulsar.transaction.buffer.TransactionBufferReader;
import org.apache.pulsar.transaction.buffer.TransactionEntry;
import org.apache.pulsar.transaction.buffer.TransactionMeta;
import org.apache.pulsar.transaction.buffer.exceptions.TransactionNotFoundException;
import org.apache.pulsar.transaction.buffer.exceptions.TransactionNotSealedException;
import org.apache.pulsar.transaction.buffer.exceptions.TransactionSealedException;
import org.apache.pulsar.transaction.buffer.exceptions.UnexpectedTxnStatusException;
import org.apache.pulsar.transaction.impl.common.TxnID;
import org.apache.pulsar.transaction.impl.common.TxnStatus;
import org.apache.pulsar.zookeeper.ZooKeeperCache;
import org.apache.pulsar.zookeeper.ZooKeeperDataCache;
import org.apache.pulsar.zookeeper.ZookeeperClientFactoryImpl;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.MockZooKeeper;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PersistentTransactionBufferTest extends MockedBookKeeperTestCase {
    private PulsarService pulsar;
    private BrokerService brokerService;
    private ManagedLedgerFactory mlFactoryMock;
    private ServerCnx serverCnx;
    private ManagedLedger ledgerMock;
    private ManagedCursor cursorMock;
    private ConfigurationCacheService configCacheService;

    final String successTopicName = "persistent://prop/use/ns-abc/successTopic_txn";
    private static final Logger log = LoggerFactory.getLogger(PersistentTransactionBufferTest.class);

    @BeforeMethod
    public void setup() throws Exception {
        ServiceConfiguration svcConfig = spy(new ServiceConfiguration());
        pulsar = spy(new PulsarService(svcConfig));
        doReturn(svcConfig).when(pulsar).getConfiguration();
        doReturn(mock(Compactor.class)).when(pulsar).getCompactor();

        mlFactoryMock = mock(ManagedLedgerFactory.class);
        doReturn(mlFactoryMock).when(pulsar).getManagedLedgerFactory();

        ZooKeeper mockZk = createMockZooKeeper();
        doReturn(mockZk).when(pulsar).getZkClient();
        doReturn(createMockBookKeeper(mockZk, pulsar.getOrderedExecutor().chooseThread(0)))
            .when(pulsar).getBookKeeperClient();

        ZooKeeperCache cache = mock(ZooKeeperCache.class);
        doReturn(30).when(cache).getZkOperationTimeoutSeconds();
        doReturn(cache).when(pulsar).getLocalZkCache();

        configCacheService = mock(ConfigurationCacheService.class);
        @SuppressWarnings("unchecked")
        ZooKeeperDataCache<Policies> zkDataCache = mock(ZooKeeperDataCache.class);
        doReturn(zkDataCache).when(configCacheService).policiesCache();
        doReturn(configCacheService).when(pulsar).getConfigurationCache();
        doReturn(Optional.empty()).when(zkDataCache).get(anyString());

        LocalZooKeeperCacheService zkCache = mock(LocalZooKeeperCacheService.class);
        doReturn(CompletableFuture.completedFuture(Optional.empty())).when(zkDataCache).getAsync(any());
        doReturn(zkDataCache).when(zkCache).policiesCache();
        doReturn(configCacheService).when(pulsar).getConfigurationCache();
        doReturn(zkCache).when(pulsar).getLocalZkCacheService();

        brokerService = spy(new BrokerService(pulsar));
        doReturn(brokerService).when(pulsar).getBrokerService();

        serverCnx = spy(new ServerCnx(pulsar));
        doReturn(true).when(serverCnx).isActive();
        doReturn(true).when(serverCnx).isWritable();
        doReturn(new InetSocketAddress("localhost", 1234)).when(serverCnx).clientAddress();

        NamespaceService nsSvc = mock(NamespaceService.class);
        doReturn(nsSvc).when(pulsar).getNamespaceService();
        doReturn(true).when(nsSvc).isServiceUnitOwned(any(NamespaceBundle.class));
        doReturn(true).when(nsSvc).isServiceUnitActive(any(TopicName.class));

        setupMLAsyncCallbackMocks();
    }

    public static MockZooKeeper createMockZooKeeper() throws Exception {
        MockZooKeeper zk = MockZooKeeper.newInstance(MoreExecutors.newDirectExecutorService());
        List<ACL> dummyAclList = new ArrayList<>(0);

        ZkUtils.createFullPathOptimistic(zk, "/ledgers/available/192.168.1.1:" + 5000,
                                         "".getBytes(ZookeeperClientFactoryImpl.ENCODING_SCHEME), dummyAclList, CreateMode.PERSISTENT);

        zk.create("/ledgers/LAYOUT", "1\nflat:1".getBytes(ZookeeperClientFactoryImpl.ENCODING_SCHEME), dummyAclList,
                  CreateMode.PERSISTENT);
        return zk;
    }

    public static NonClosableMockBookKeeper createMockBookKeeper(ZooKeeper zookeeper,
                                                                 ExecutorService executor) throws Exception {
        return spy(new NonClosableMockBookKeeper(zookeeper, executor));
    }

    public static class NonClosableMockBookKeeper extends PulsarMockBookKeeper {

        public NonClosableMockBookKeeper(ZooKeeper zk, ExecutorService executor) throws Exception {
            super(zk, executor);
        }

        @Override
        public void close() {
            // no-op
        }

        @Override
        public void shutdown() {
            // no-op
        }

        public void reallyShutdown() {
            super.shutdown();
        }
    }

    @SuppressWarnings("unchecked")
    void setupMLAsyncCallbackMocks()
        throws BrokerServiceException.NamingException, ManagedLedgerException, InterruptedException {
//        ledgerMock = factory.open("hello");
        ledgerMock = mock(ManagedLedger.class);
        cursorMock = mock(ManagedCursor.class);
        final CompletableFuture<Void> closeFuture = new CompletableFuture<>();

        doReturn(new ArrayList<Object>()).when(ledgerMock).getCursors();
        doReturn("mockCursor").when(cursorMock).getName();
        // doNothing().when(cursorMock).asyncClose(new CloseCallback() {
        doAnswer(new Answer<Object>() {
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
        }).when(mlFactoryMock)
          .asyncOpen(matches(".*success.*"), any(ManagedLedgerConfig.class), any(OpenLedgerCallback.class), anyObject());

        // call openLedgerFailed on ML factory asyncOpen
        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                ((OpenLedgerCallback) invocationOnMock.getArguments()[2])
                    .openLedgerFailed(new ManagedLedgerException("Managed ledger failure"), null);
                return null;
            }
        }).when(mlFactoryMock)
          .asyncOpen(matches(".*fail.*"), any(ManagedLedgerConfig.class), any(OpenLedgerCallback.class), anyObject());

        // call addComplete on ledger asyncAddEntry
        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                ((AddEntryCallback) invocationOnMock.getArguments()[1])
                    .addComplete(new PositionImpl(1, 1), invocationOnMock.getArguments()[2]);
                return null;
            }
        }).when(ledgerMock).asyncAddEntry(any(ByteBuf.class), any(AddEntryCallback.class), anyObject());

        // call openCursorComplete on cursor asyncOpen
        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                ((OpenCursorCallback) invocationOnMock.getArguments()[2]).openCursorComplete(cursorMock, null);
                return null;
            }
        }).when(ledgerMock)
          .asyncOpenCursor(matches(".*success.*"), any(InitialPosition.class), any(OpenCursorCallback.class), anyObject());

        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                ((OpenCursorCallback) invocationOnMock.getArguments()[3]).openCursorComplete(cursorMock, null);
                return null;
            }
        }).when(ledgerMock).asyncOpenCursor(matches(".*success.*"), any(InitialPosition.class), any(Map.class),
                                            any(OpenCursorCallback.class), anyObject());

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

        doAnswer((invokactionOnMock) -> {
            ((MarkDeleteCallback) invokactionOnMock.getArguments()[2])
                .markDeleteComplete(invokactionOnMock.getArguments()[3]);
            return null;
        }).when(cursorMock).asyncMarkDelete(anyObject(), anyObject(), any(MarkDeleteCallback.class), anyObject());

        this.buffer = new PersistentTransactionBuffer(successTopicName, factory.open("hello"), brokerService);
    }

        @AfterMethod
    public void teardown() throws Exception {
        brokerService.getTopics().clear();
        brokerService.close(); //to clear pulsarStats
        try {
            pulsar.close();
        } catch (Exception e) {
            log.warn("Failed to close pulsar service", e);
            throw e;
        }
    }

    private final TxnID txnID = new TxnID(1234L, 5678L);
    private PersistentTransactionBuffer buffer;

    @Test
    public void testGetANonExistTxn() throws BrokerServiceException.NamingException {
        buffer.getTransactionMeta(txnID).whenComplete(((meta, throwable) -> {
            assertTrue(throwable instanceof TransactionNotFoundException);
        }));
    }

    @Test
    public void testOpenReaderOnNonExistentTxn() throws Exception {
        buffer.openTransactionBufferReader(txnID, 0L).whenComplete((transactionBufferReader, throwable) -> {
            assertTrue(throwable instanceof TransactionNotFoundException);
        });
    }

    @Test
    public void testOpenReadOnAnOpenTxn() throws ExecutionException, InterruptedException {
        final int numEntries = 10;
        appendEntries(txnID, numEntries, 0L);
        TransactionMeta meta = buffer.getTransactionMeta(txnID).get();
        assertEquals(txnID, meta.id());
        assertEquals(TxnStatus.OPEN, meta.status());

        buffer.openTransactionBufferReader(txnID, 0L).whenComplete((transactionBufferReader, throwable) -> {
            assertTrue(throwable instanceof TransactionNotSealedException);
        });
    }

    @Test
    public void testOpenReaderOnCommittedTxn() throws ExecutionException, InterruptedException {
        final int numEntries = 10;
        appendEntries(txnID, numEntries, 0L);
        TransactionMeta meta = buffer.getTransactionMeta(txnID).get();
        assertEquals(txnID, meta.id());
        assertEquals(TxnStatus.OPEN, meta.status());

        CountDownLatch latch = new CountDownLatch(1);
        buffer.commitTxn(txnID, 22L, 33L).get();

        meta = buffer.getTransactionMeta(txnID).get();
        assertEquals(txnID, meta.id());
        assertEquals(TxnStatus.COMMITTED, meta.status());

        try (TransactionBufferReader reader = buffer.openTransactionBufferReader(txnID, 0L).get()) {
            reader.readNext(numEntries).whenComplete((transactionEntries, throwable) -> {
                if (null != throwable) {
                    fail("Should not fail to read entries");
                } else {
                    verifyAndReleaseEntries(transactionEntries, txnID, 0L, numEntries);
                }
            });
        }
    }

    @Test
    public void testCommitNonExistentTxn() throws ExecutionException, InterruptedException {
        try {
            buffer.commitTxn(txnID, 22L, 33L).get();
        } catch (ExecutionException ee) {
            assertTrue(ee.getCause() instanceof TransactionNotFoundException);
        }
    }

    @Test
    public void testCommitTxn() throws Exception {
        final int numEntries = 10;
        appendEntries(txnID, numEntries, 0L);
        TransactionMeta meta = buffer.getTransactionMeta(txnID).get();

        assertEquals(txnID, meta.id());
        assertEquals(meta.status(), TxnStatus.OPEN);

        buffer.commitTxn(txnID, 22L, 33L).get();
        meta = buffer.getTransactionMeta(txnID).get();

        assertEquals(txnID, meta.id());
        assertEquals(meta.status(), TxnStatus.COMMITTED);
    }

    @Test
    public void testAbortNonExistentTxn() throws Exception {
        try {
            buffer.abortTxn(txnID).get();
            fail("Should fail to abort a transaction if it doesn't exist");
        } catch (ExecutionException ee) {
            assertTrue(ee.getCause() instanceof TransactionNotFoundException);
        }
    }

    @Test
    public void testAbortCommittedTxn() throws Exception {
        final int numEntries = 10;
        appendEntries(txnID, numEntries, 0L);
        TransactionMeta meta = buffer.getTransactionMeta(txnID).get();
        assertEquals(txnID, meta.id());
        assertEquals(TxnStatus.OPEN, meta.status());

        buffer.commitTxn(txnID, 22L, 33L).get();
        meta = buffer.getTransactionMeta(txnID).get();
        assertEquals(txnID, meta.id());
        assertEquals(TxnStatus.COMMITTED, meta.status());

        try {
            buffer.abortTxn(txnID).get();
            fail("Should fail to abort a committed transaction");
        } catch (ExecutionException ee) {
            assertTrue(ee.getCause() instanceof UnexpectedTxnStatusException);
        }

        meta = buffer.getTransactionMeta(txnID).get();
        assertEquals(txnID, meta.id());
        assertEquals(TxnStatus.COMMITTED, meta.status());
    }

    @Test
    public void testAbortTxn() throws Exception {
        final int numEntries = 10;
        appendEntries(txnID, numEntries, 0L);
        TransactionMeta meta = buffer.getTransactionMeta(txnID).get();
        assertEquals(txnID, meta.id());
        assertEquals(TxnStatus.OPEN, meta.status());

        buffer.abortTxn(txnID).get();
        verifyTxnNotExist(txnID);
    }

    @Test
    public void testPurgeTxns() throws Exception {
        final int numEntries = 10;
        TxnID txnId1 = new TxnID(1234L, 2345L);
        appendEntries(txnId1, numEntries, 0L);
        TransactionMeta meta1 = buffer.getTransactionMeta(txnId1).get();
        assertEquals(txnId1, meta1.id());
        assertEquals(TxnStatus.OPEN, meta1.status());

        TxnID txnId2 = new TxnID(1234L, 3456L);
        appendEntries(txnId2, numEntries, 0L);
        buffer.commitTxn(txnId2, 22L, 0L).get();
        TransactionMeta meta2 = buffer.getTransactionMeta(txnId2).get();
        assertEquals(txnId2, meta2.id());
        assertEquals(TxnStatus.COMMITTED, meta2.status());

        TxnID txnId3 = new TxnID(1234L, 4567L);
        appendEntries(txnId3, numEntries, 0L);
        buffer.commitTxn(txnId3, 23L, 0L).get();
        TransactionMeta meta3 = buffer.getTransactionMeta(txnId3).get();
        assertEquals(txnId3, meta3.id());
        assertEquals(TxnStatus.COMMITTED, meta3.status());

        buffer.purgeTxns(Lists.newArrayList(Long.valueOf(22L))).get();

        verifyTxnNotExist(txnId2);

        meta1 = buffer.getTransactionMeta(txnId1).get();
        assertEquals(txnId1, meta1.id());
        assertEquals(TxnStatus.OPEN, meta1.status());

        meta3 = buffer.getTransactionMeta(txnId3).get();
        assertEquals(txnId3, meta3.id());
        assertEquals(TxnStatus.COMMITTED, meta3.status());
    }

    private void verifyTxnNotExist(TxnID txnID) throws Exception {
        try {
            buffer.getTransactionMeta(txnID).get();
        } catch (ExecutionException ee) {
            assertTrue(ee.getCause() instanceof TransactionNotFoundException);
        }
    }

    private void appendEntries(TxnID id, int numEntries, long startSequenceId) {
        for (int i = 0; i < numEntries; i++) {
            long sequenceId = startSequenceId + i;
            buffer.appendBufferToTxn(id, sequenceId, Unpooled.copiedBuffer("message-" + sequenceId, UTF_8)).join();
        }
    }

    private void verifyAndReleaseEntries(List<TransactionEntry> txnEntries,
                                         TxnID txnID,
                                         long startSequenceId,
                                         int numEntriesToRead) {
        assertEquals(txnEntries.size(), numEntriesToRead);
        for (int i = 0; i < numEntriesToRead; i++) {
            try (TransactionEntry txnEntry = txnEntries.get(i)) {
                assertEquals(txnEntry.committedAtLedgerId(), 22L);
                assertEquals(txnEntry.committedAtEntryId(), 33L);
                assertEquals(txnEntry.txnId(), txnID);
                assertEquals(txnEntry.sequenceId(), startSequenceId + i);
                assertEquals(new String(
                    ByteBufUtil.getBytes(txnEntry.getEntryBuffer()),
                    UTF_8
                ), "message-" + i);
            }
        }
    }

}

