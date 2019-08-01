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
 *
 */
package org.apache.pulsar.transaction.buffer.impl;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.mockito.ArgumentMatchers.endsWith;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
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
import java.util.SortedMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
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
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.Markers;
import org.apache.pulsar.compaction.Compactor;
import org.apache.pulsar.transaction.buffer.TransactionBufferClient;
import org.apache.pulsar.transaction.buffer.TransactionBufferReader;
import org.apache.pulsar.transaction.buffer.TransactionEntry;
import org.apache.pulsar.transaction.buffer.TransactionMeta;
import org.apache.pulsar.transaction.buffer.exceptions.EndOfTransactionException;
import org.apache.pulsar.transaction.buffer.exceptions.NoTxnsCommittedAtLedgerException;
import org.apache.pulsar.transaction.buffer.exceptions.TransactionBufferException;
import org.apache.pulsar.transaction.buffer.exceptions.TransactionClientException;
import org.apache.pulsar.transaction.buffer.exceptions.TransactionNotFoundException;
import org.apache.pulsar.transaction.buffer.exceptions.TransactionNotSealedException;
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
import org.apache.zookeeper.txn.Txn;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
public class TransactionClientTest extends MockedBookKeeperTestCase {

    private PulsarService pulsar;
    private BrokerService brokerService;
    private ManagedLedgerFactory mlFactoryMock;
    private ServerCnx serverCnx;
    private ManagedLedger ledgerMock;
    private ManagedCursor cursorMock;
    private ConfigurationCacheService configCacheService;

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

    public static PersistentTransactionBufferTest.NonClosableMockBookKeeper createMockBookKeeper(ZooKeeper zookeeper,
                                                                                                 ExecutorService executor) throws Exception {
        return spy(new PersistentTransactionBufferTest.NonClosableMockBookKeeper(zookeeper, executor));
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

    private TransactionBufferClient createClient(String ledgerName, String topic)
        throws ManagedLedgerException, InterruptedException, BrokerServiceException.NamingException {
        ManagedLedger ledger = factory.open(ledgerName);
        PersistentTransactionBuffer buffer = new PersistentTransactionBuffer(topic, ledger, brokerService);
        return new TransactionBufferClientImpl(buffer);
    }

    @Test
    public void testNonExistTxn()
        throws ManagedLedgerException, InterruptedException, BrokerServiceException.NamingException {
        TransactionBufferClient client = createClient("test", "test_topic");
        TxnID txnID = new TxnID(1L, 2L);
        try {
            assertFalse(client.exist(txnID));
        } catch (TransactionClientException e) {
            fail("should not have any exception");
        }
    }

    @Test
    public void testAbortStatus()
        throws ManagedLedgerException, InterruptedException, BrokerServiceException.NamingException,
               TransactionClientException {
        TransactionBufferClient client = createClient("status", "test_status");
        TxnID txnID = new TxnID(1L, 2L);
        try {
            client.abortTxn(txnID);
        } catch (TransactionClientException e) {
            assertTrue(e.getCause().getCause() instanceof TransactionNotFoundException);
        }
        client.append(txnID, 1L, createExmapleData());
        client.abortTxn(txnID);
        assertTrue(client.isAborted(txnID));
        assertFalse(client.isCommitted(txnID));
    }

    @Test
    public void testCommitStatus() throws Exception{
        TransactionBufferClient client = createClient("commit", "commit_test");
        TxnID txnID = new TxnID(1L, 3L);
        try {
            client.commitTxn(txnID, 1L, 2L);
        } catch (TransactionClientException e) {
            assertTrue(e.getCause().getCause() instanceof TransactionNotFoundException);
        }

        client.append(txnID, 1L, createExmapleData());
        client.commitTxn(txnID, 1L, 2L);
        assertTrue(client.isCommitted(txnID));
        assertFalse(client.isAborted(txnID));
    }

    @Test
    public void testGetAllMessages() throws Exception{
        TransactionBufferClient client = createClient("get", "get_test");
        TxnID txnID = new TxnID(1L, 2L);
        try {
            client.append(txnID, 1L, createExmapleData());
        } catch (TransactionClientException e) {
            assertTrue(e.getCause().getCause() instanceof TransactionNotFoundException);
        }

        client.append(txnID, 1L, createExmapleData());

        try {
            client.getTxnMessages(txnID, 1L, 1);
        } catch (TransactionClientException e) {
            assertTrue(e.getCause().getCause() instanceof TransactionNotSealedException);
        }

        client.commitTxn(txnID, 1L, 2L);
        List<ByteBuf> messages = client.getTxnMessages(txnID, 1L, 1);
        assertEquals(messages.size(), 1);
        assertEquals(messages.get(0), createExmapleData());
    }

    private ByteBuf createExmapleData() {
        return Unpooled.copiedBuffer("message", UTF_8);
    }
}
