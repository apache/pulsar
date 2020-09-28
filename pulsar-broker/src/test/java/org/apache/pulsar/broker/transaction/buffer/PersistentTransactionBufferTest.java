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
package org.apache.pulsar.broker.transaction.buffer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.matches;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

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
import java.util.SortedMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.bookkeeper.client.PulsarMockBookKeeper;
import org.apache.bookkeeper.mledger.AsyncCallbacks.AddEntryCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.CloseCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteCursorCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteLedgerCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.MarkDeleteCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OpenCursorCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OpenLedgerCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.cache.ConfigurationCacheService;
import org.apache.pulsar.broker.cache.LocalZooKeeperCacheService;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.transaction.buffer.impl.PersistentTransactionBuffer;
import org.apache.pulsar.broker.transaction.buffer.impl.TransactionMetaImpl;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSubscribe.InitialPosition;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.Markers;
import org.apache.pulsar.compaction.Compactor;
import org.apache.pulsar.broker.transaction.buffer.exceptions.EndOfTransactionException;
import org.apache.pulsar.broker.transaction.buffer.exceptions.NoTxnsCommittedAtLedgerException;
import org.apache.pulsar.broker.transaction.buffer.exceptions.TransactionNotFoundException;
import org.apache.pulsar.broker.transaction.buffer.exceptions.TransactionNotSealedException;
import org.apache.pulsar.broker.transaction.buffer.exceptions.TransactionStatusException;
import org.apache.pulsar.transaction.impl.common.TxnStatus;
import org.apache.pulsar.zookeeper.ZooKeeperCache;
import org.apache.pulsar.zookeeper.ZooKeeperDataCache;
import org.apache.pulsar.zookeeper.ZookeeperClientFactoryImpl;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.MockZooKeeper;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.mockito.Mockito;
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

    private long committedLedgerId = -1L;
    private long committedEntryId = -1L;

    final String successTopicName = "persistent://prop/use/ns-abc/successTopic_txn";
    private static final Logger log = LoggerFactory.getLogger(PersistentTransactionBufferTest.class);

    private MockZooKeeper mockZk;

    @BeforeMethod
    public void setup() throws Exception {
        ServiceConfiguration svcConfig = spy(new ServiceConfiguration());
        pulsar = spy(new PulsarService(svcConfig));
        doReturn(svcConfig).when(pulsar).getConfiguration();
        doReturn(mock(Compactor.class)).when(pulsar).getCompactor();

        mlFactoryMock = mock(ManagedLedgerFactory.class);
        doReturn(mlFactoryMock).when(pulsar).getManagedLedgerFactory();

        mockZk = createMockZooKeeper();
        doReturn(mockZk).when(pulsar).getZkClient();
        doReturn(createMockBookKeeper(mockZk, pulsar.getExecutor()))
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
            throws BrokerServiceException.NamingException, ManagedLedgerException, InterruptedException, ExecutionException {
        ledgerMock = mock(ManagedLedger.class);
        cursorMock = mock(ManagedCursor.class);
        final CompletableFuture<Void> closeFuture = new CompletableFuture<>();

        doReturn(new ArrayList<Object>()).when(ledgerMock).getCursors();
        doReturn("mockCursor").when(cursorMock).getName();
        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
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
                .asyncOpen(matches(".*success.*"), any(ManagedLedgerConfig.class), any(OpenLedgerCallback.class),
                        any(Supplier.class), any());

        // call openLedgerFailed on ML factory asyncOpen
        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                ((OpenLedgerCallback) invocationOnMock.getArguments()[2])
                    .openLedgerFailed(new ManagedLedgerException("Managed ledger failure"), null);
                return null;
            }
        }).when(mlFactoryMock)
                .asyncOpen(matches(".*fail.*"), any(ManagedLedgerConfig.class), any(OpenLedgerCallback.class),
                        any(Supplier.class), any());

        // call addComplete on ledger asyncAddEntry
        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                ((AddEntryCallback) invocationOnMock.getArguments()[1])
                    .addComplete(new PositionImpl(1, 1), invocationOnMock.getArguments()[2]);
                return null;
            }
        }).when(ledgerMock).asyncAddEntry(any(ByteBuf.class), any(AddEntryCallback.class), any());

        // call openCursorComplete on cursor asyncOpen
        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                ((OpenCursorCallback) invocationOnMock.getArguments()[2]).openCursorComplete(cursorMock, null);
                return null;
            }
        }).when(ledgerMock)
          .asyncOpenCursor(matches(".*success.*"), any(InitialPosition.class), any(OpenCursorCallback.class), any());

        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                ((OpenCursorCallback) invocationOnMock.getArguments()[3]).openCursorComplete(cursorMock, null);
                return null;
            }
        }).when(ledgerMock).asyncOpenCursor(matches(".*success.*"), any(InitialPosition.class), any(Map.class),
                                            any(OpenCursorCallback.class), any());

        // call deleteLedgerComplete on ledger asyncDelete
        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                ((DeleteLedgerCallback) invocationOnMock.getArguments()[0]).deleteLedgerComplete(null);
                return null;
            }
        }).when(ledgerMock).asyncDelete(any(DeleteLedgerCallback.class), any());

        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                ((DeleteCursorCallback) invocationOnMock.getArguments()[1]).deleteCursorComplete(null);
                return null;
            }
        }).when(ledgerMock).asyncDeleteCursor(matches(".*success.*"), any(DeleteCursorCallback.class), any());

        doAnswer((invokactionOnMock) -> {
            ((MarkDeleteCallback) invokactionOnMock.getArguments()[2])
                .markDeleteComplete(invokactionOnMock.getArguments()[3]);
            return null;
        }).when(cursorMock).asyncMarkDelete(any(), any(), any(MarkDeleteCallback.class), any());

        this.buffer = new PersistentTransactionBuffer(successTopicName,
                factory.open("hello"), brokerService, getMockPersistentTopic());
    }

    private PersistentTopic getMockPersistentTopic() {
        PersistentTopic persistentTopic = Mockito.mock(PersistentTopic.class);
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                ((Topic.PublishContext) invocationOnMock.getArguments()[1])
                        .completed(null, committedLedgerId, committedEntryId);
                return null;
            }
        }).when(persistentTopic).publishMessage(Mockito.any(), Mockito.any());
        return persistentTopic;
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

        mockZk.shutdown();
    }

    private final TxnID txnID = new TxnID(1234L, 5678L);
    private PersistentTransactionBuffer buffer;

    @Test
    public void testGetANonExistTxn() throws InterruptedException {
        try {
            buffer.getTransactionMeta(txnID).get();
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof TransactionNotFoundException);
        }
    }

    @Test
    public void testOpenReaderOnNonExistentTxn() throws InterruptedException{
        try {
            buffer.openTransactionBufferReader(txnID, 0L).get();
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof TransactionNotFoundException);
        }
    }

    @Test
    public void testOpenReadOnAnOpenTxn() throws InterruptedException {
        final int numEntries = 10;
        appendEntries(buffer, txnID, numEntries, 0L);
        TransactionMeta meta = null;
        try {
            meta = buffer.getTransactionMeta(txnID).get();
        } catch (ExecutionException e) {
            fail("Should not failed at here");
        }
        assertEquals(txnID, meta.id());
        assertEquals(TxnStatus.OPEN, meta.status());

        try {
            buffer.openTransactionBufferReader(txnID, 0L).get();
            fail("Should failed");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof TransactionNotSealedException);
        }
    }

    @Test
    public void testOpenReaderOnCommittedTxn() throws Exception {
        final int numEntries = 10;
        appendEntries(buffer, txnID, numEntries, 0L);
        TransactionMeta meta = buffer.getTransactionMeta(txnID).get();
        assertEquals(txnID, meta.id());
        assertEquals(TxnStatus.OPEN, meta.status());

        endTxnAndWaitTillFinish(buffer, txnID, PulsarApi.TxnAction.COMMIT, 22L, 33L);

        meta = buffer.getTransactionMeta(txnID).get();
        assertEquals(txnID, meta.id());
        assertEquals(TxnStatus.COMMITTED, meta.status());

        try (TransactionBufferReader reader = buffer.openTransactionBufferReader(txnID, 0L).get()) {
            List<TransactionEntry> entries = reader.readNext(numEntries).get();
            verifyAndReleaseEntries(entries, txnID, 0L, numEntries);

            reader.readNext(1).get();
            Assert.fail("Should cause the exception `EndOfTransactionException`.");
        } catch (ExecutionException ee) {
             assertTrue(ee.getCause() instanceof EndOfTransactionException);
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
    public void testEndOnPartition() throws Exception {
        final int numEntries = 10;
        TxnID commitTxn = new TxnID(RandomUtils.nextLong(), RandomUtils.nextLong());
        appendEntries(buffer, commitTxn, numEntries, 0L);
        endTxnAndWaitTillFinish(buffer, commitTxn, PulsarApi.TxnAction.COMMIT, 22L, 33L);

        TransactionMeta meta = buffer.getTransactionMeta(commitTxn).get();
        assertEquals(meta.status(), TxnStatus.COMMITTED);
    }

    @Test()
    public void testCommitTxn() throws Exception {
        final int numEntries = 10;
        appendEntries(buffer, txnID, numEntries, 0L);
        TransactionMeta meta = buffer.getTransactionMeta(txnID).get();

        assertEquals(txnID, meta.id());
        assertEquals(meta.status(), TxnStatus.OPEN);

        endTxnAndWaitTillFinish(buffer, txnID, PulsarApi.TxnAction.COMMIT, 22L, 33L);
        meta = buffer.getTransactionMeta(txnID).get();

        assertEquals(txnID, meta.id());
        assertEquals(meta.status(), TxnStatus.COMMITTED);
    }

    @Test
    public void testCommitTxnMultiTimes() throws Exception {
        final int numEntries = 10;
        appendEntries(buffer, txnID, numEntries, 0L);
        TransactionMeta meta = buffer.getTransactionMeta(txnID).get();

        assertEquals(txnID, meta.id());
        assertEquals(meta.status(), TxnStatus.OPEN);

        endTxnAndWaitTillFinish(buffer, txnID, PulsarApi.TxnAction.COMMIT, 22L, 33L);
        try {
            buffer.commitTxn(txnID, 23L, 34L).get();
            buffer.commitTxn(txnID, 24L, 34L).get();
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof TransactionStatusException);
        }
        meta = buffer.getTransactionMeta(txnID).get();

        assertEquals(txnID, meta.id());
        assertEquals(meta.status(), TxnStatus.COMMITTED);
        assertEquals(meta.committedAtLedgerId(), 22L);
        assertEquals(meta.committedAtEntryId(), 33L);
        assertEquals(meta.numEntries(), numEntries);
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
        appendEntries(buffer, txnID, numEntries, 0L);
        TransactionMeta meta = buffer.getTransactionMeta(txnID).get();
        assertEquals(txnID, meta.id());
        assertEquals(TxnStatus.OPEN, meta.status());

        endTxnAndWaitTillFinish(buffer, txnID, PulsarApi.TxnAction.COMMIT, 22L, 33L);
        meta = buffer.getTransactionMeta(txnID).get();
        assertEquals(txnID, meta.id());
        assertEquals(TxnStatus.COMMITTED, meta.status());

        try {
            buffer.abortTxn(txnID).get();
            fail("Should fail to abort a committed transaction");
        } catch (ExecutionException ee) {
            assertTrue(ee.getCause() instanceof TransactionStatusException);
        }

        meta = buffer.getTransactionMeta(txnID).get();
        assertEquals(txnID, meta.id());
        assertEquals(TxnStatus.COMMITTED, meta.status());
    }

    @Test
    public void testAbortTxn() throws Exception {
        final int numEntries = 10;
        appendEntries(buffer, txnID, numEntries, 0L);
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
        appendEntries(buffer, txnId1, numEntries, 0L);
        TransactionMeta meta1 = buffer.getTransactionMeta(txnId1).get();
        assertEquals(txnId1, meta1.id());
        assertEquals(TxnStatus.OPEN, meta1.status());

        TxnID txnId2 = new TxnID(1234L, 3456L);
        appendEntries(buffer, txnId2, numEntries, 0L);
        endTxnAndWaitTillFinish(buffer, txnId2, PulsarApi.TxnAction.COMMIT, 22L, 0L);
        TransactionMeta meta2 = buffer.getTransactionMeta(txnId2).get();
        assertEquals(txnId2, meta2.id());
        assertEquals(TxnStatus.COMMITTED, meta2.status());

        TxnID txnId3 = new TxnID(1234L, 4567L);
        appendEntries(buffer, txnId3, numEntries, 0L);
        endTxnAndWaitTillFinish(buffer, txnId3, PulsarApi.TxnAction.COMMIT, 23L, 0L);
        TransactionMeta meta3 = buffer.getTransactionMeta(txnId3).get();
        assertEquals(txnId3, meta3.id());
        assertEquals(TxnStatus.COMMITTED, meta3.status());

        buffer.purgeTxns(Lists.newArrayList(22L)).get();

        verifyTxnNotExist(txnId2);

        meta1 = buffer.getTransactionMeta(txnId1).get();
        assertEquals(txnId1, meta1.id());
        assertEquals(TxnStatus.OPEN, meta1.status());

        meta3 = buffer.getTransactionMeta(txnId3).get();
        assertEquals(txnId3, meta3.id());
        assertEquals(TxnStatus.COMMITTED, meta3.status());

        // purge a non exist ledger.
        try {
            buffer.purgeTxns(Lists.newArrayList(1L)).get();
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof NoTxnsCommittedAtLedgerException);
        }

        verifyTxnNotExist(txnId2);

        meta1 = buffer.getTransactionMeta(txnId1).get();
        assertEquals(txnId1, meta1.id());
        assertEquals(TxnStatus.OPEN, meta1.status());

        meta3 = buffer.getTransactionMeta(txnId3).get();
        assertEquals(txnId3, meta3.id());
        assertEquals(TxnStatus.COMMITTED, meta3.status());
    }

    @Test
    public void testAppendEntry() throws Exception {
        ManagedLedger ledger = factory.open("test_ledger");
        PersistentTransactionBuffer newBuffer = new PersistentTransactionBuffer(successTopicName, ledger,
                brokerService, getMockPersistentTopic());
        final int numEntries = 10;
        TxnID txnID = new TxnID(1111L, 2222L);
        List<ByteBuf> appendEntries =  appendEntries(newBuffer, txnID, numEntries, 0L);
        List<ByteBuf> copy = new ArrayList<>(appendEntries);
        TransactionMetaImpl meta = (TransactionMetaImpl) newBuffer.getTransactionMeta(txnID).get();
        assertEquals(meta.id(), txnID);
        assertEquals(numEntries, meta.numEntries());
        assertEquals(meta.status(), TxnStatus.OPEN);

        verifyEntries(ledger, appendEntries, meta.getEntries());

        endTxnAndWaitTillFinish(newBuffer, txnID, PulsarApi.TxnAction.COMMIT, 22L, 33L);
        meta = (TransactionMetaImpl) newBuffer.getTransactionMeta(txnID).get();

        assertEquals(meta.id(), txnID);
        assertEquals(meta.numEntries(), numEntries);
        assertEquals(meta.status(), TxnStatus.COMMITTED);
        verifyEntries(ledger, copy, meta.getEntries());
    }

    @Test
    public void testCommitMarker() throws Exception {
        ManagedLedger ledger = factory.open("test_commit_ledger");
        PersistentTransactionBuffer commitBuffer = new PersistentTransactionBuffer(successTopicName, ledger,
                brokerService, getMockPersistentTopic());
        final int numEntries = 10;
        List<ByteBuf> appendEntires = appendEntries(commitBuffer, txnID, numEntries, 0L);

        TransactionMetaImpl meta = (TransactionMetaImpl) commitBuffer.getTransactionMeta(txnID).get();
        assertEquals(meta.id(), txnID);
        assertEquals(meta.numEntries(), numEntries);
        assertEquals(meta.status(), TxnStatus.OPEN);

        verifyEntries(ledger, appendEntires, meta.getEntries());

        endTxnAndWaitTillFinish(commitBuffer, txnID, PulsarApi.TxnAction.COMMIT, 22L, 33L);
        assertEquals(meta.id(), txnID);
        assertEquals(meta.numEntries(), numEntries);
        assertEquals(meta.status(), TxnStatus.COMMITTED);

        ManagedCursor cursor = ledger.newNonDurableCursor(PositionImpl.earliest);
        Entry entry = getEntry(cursor, ledger.getLastConfirmedEntry());

        boolean commitMarker = Markers.isTxnCommitMarker(Commands.parseMessageMetadata(entry.getDataBuffer()));
        assertTrue(commitMarker);

    }

    @Test
    public void testAbortMarker() throws Exception {
        ManagedLedger ledger = factory.open("test_abort_ledger");
        PersistentTransactionBuffer abortBuffer = new PersistentTransactionBuffer(successTopicName, ledger,
                brokerService, getMockPersistentTopic());
        final int numEntries = 10;
        List<ByteBuf> appendEntires = appendEntries(abortBuffer, txnID, numEntries, 0L);

        TransactionMetaImpl meta = (TransactionMetaImpl) abortBuffer.getTransactionMeta(txnID).get();
        assertEquals(meta.id(), txnID);
        assertEquals(meta.numEntries(), numEntries);
        assertEquals(meta.status(), TxnStatus.OPEN);

        verifyEntries(ledger, appendEntires, meta.getEntries());

        abortBuffer.abortTxn(txnID).get();
        assertEquals(meta.id(), txnID);
        assertEquals(meta.numEntries(), numEntries);
        assertEquals(meta.status(), TxnStatus.ABORTED);

        ManagedCursor cursor = ledger.newNonDurableCursor(PositionImpl.earliest);
        Entry entry = getEntry(cursor, ledger.getLastConfirmedEntry());

        boolean abortMarker = Markers.isTxnAbortMarker(Commands.parseMessageMetadata(entry.getDataBuffer()));
        assertTrue(abortMarker);
    }

    private void verifyEntries(ManagedLedger ledger, List<ByteBuf> appendEntries,
                               SortedMap<Long, Position> addedEntries)
        throws ManagedLedgerException, InterruptedException {
        ManagedCursor cursor = ledger.newNonDurableCursor(PositionImpl.earliest);
        assertNotNull(cursor);
        for (Map.Entry<Long, Position> longPositionEntry : addedEntries.entrySet()) {
            Entry entry = getEntry(cursor, longPositionEntry.getValue());
            assertTrue(appendEntries.remove(entry.getDataBuffer()));
        }
    }

    private Entry getEntry(ManagedCursor cursor, Position position)
        throws ManagedLedgerException, InterruptedException {
        assertNotNull(cursor);
        cursor.seek(position);
        List<Entry> readEntry = cursor.readEntries(1);
        assertEquals(readEntry.size(), 1);
        return readEntry.get(0);
    }

    @Test
    public void testNoDeduplicateMessage()
        throws ManagedLedgerException, InterruptedException, BrokerServiceException.NamingException,
               ExecutionException {
        ManagedLedger ledger = factory.open("test_deduplicate");
        PersistentTransactionBuffer newBuffer = new PersistentTransactionBuffer(
                successTopicName, ledger, brokerService, getMockPersistentTopic());
        final int numEntries = 10;

        TxnID txnID = new TxnID(1234L, 5678L);
        List<ByteBuf> appendEntries = appendEntries(newBuffer, txnID, numEntries, 0L);
        TransactionMetaImpl meta = (TransactionMetaImpl) newBuffer.getTransactionMeta(txnID).get();

        assertEquals(meta.id(), txnID);
        assertEquals(meta.status(), TxnStatus.OPEN);
        assertEquals(meta.numEntries(), appendEntries.size());

        verifyEntries(ledger, appendEntries, meta.getEntries());

        // append new message with same sequenceId
        List<ByteBuf> deduplicateData = new ArrayList<>();
        for (int i = 0; i < numEntries; i++) {
            long sequenceId = i;
            ByteBuf data = Unpooled.copiedBuffer("message-deduplicate-" + sequenceId, UTF_8);
            newBuffer.appendBufferToTxn(txnID, sequenceId, 1, data);
            deduplicateData.add(data);
        }

        TransactionMetaImpl meta1 = (TransactionMetaImpl) newBuffer.getTransactionMeta(txnID).get();

        assertEquals(meta1.id(), txnID);
        assertEquals(meta1.numEntries(), numEntries);
        assertEquals(meta.status(), TxnStatus.OPEN);

        // read all entries in new buffer
        ManagedCursor read = ledger.newNonDurableCursor(PositionImpl.earliest);
        List<Entry> allEntries = read.readEntries(100);
        List<ByteBuf> allMsg = allEntries.stream().map(entry -> entry.getDataBuffer()).collect(Collectors.toList());

        assertEquals(allEntries.size(), numEntries);
        verifyEntries(ledger, allMsg, meta1.getEntries());
    }

    private void verifyTxnNotExist(TxnID txnID) throws Exception {
        try {
            buffer.getTransactionMeta(txnID).get();
        } catch (ExecutionException ee) {
            assertTrue(ee.getCause() instanceof TransactionNotFoundException);
        }
    }

    private List<ByteBuf> appendEntries(PersistentTransactionBuffer writeBuffer, TxnID id, int numEntries,
                                        long startSequenceId) {
        List<ByteBuf> entries = new ArrayList<>();
        for (int i = 0; i < numEntries; i++) {
            long sequenceId = startSequenceId + i;
            writeBuffer.appendBufferToTxn(id, sequenceId, 1, Unpooled.copiedBuffer("message-" + sequenceId, UTF_8)).join();
            entries.add(Unpooled.copiedBuffer("message-" + sequenceId, UTF_8));
        }
        return entries;
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
                    ByteBufUtil.getBytes(txnEntry.getEntry().getDataBuffer()),
                    UTF_8
                ), "message-" + i);
            }
        }
    }

    private void endTxnAndWaitTillFinish(TransactionBuffer tb, TxnID txnId, PulsarApi.TxnAction txnAction,
                                         long committedLedgerId, long committedEntryId) throws Exception {
        this.committedLedgerId = committedLedgerId;
        this.committedEntryId = committedEntryId;
        tb.endTxnOnPartition(txnId, txnAction.getNumber());
        TransactionMeta meta = tb.getTransactionMeta(txnId).get();
        while (meta.status().equals(TxnStatus.OPEN)
                || meta.status().equals(TxnStatus.COMMITTING)
                || meta.status().equals(TxnStatus.ABORTING)) {
            Thread.sleep(1000);
        }
    }

}
