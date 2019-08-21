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
package org.apache.pulsar.client.impl.transaction;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import io.netty.buffer.ByteBuf;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.ClientCnx;
import org.apache.pulsar.client.impl.ConnectionPool;
import org.apache.pulsar.client.impl.LookupService;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TransactionCoordinatorClientTest {

    PulsarClientImpl pulsarClient;
    ClientCnx clientCnx;
    LookupService lookupService;
    ConnectionPool connectionPool;
    ClientConfigurationData configurationData;

    static final String successTopicName = "successTopicName";
    static final String failTopicName = "failTopicName";

    static class MockException extends Exception {
        public MockException(String msg) {
            super(msg);
        }
    }

    @BeforeMethod
    public void setup() {
        pulsarClient = mock(PulsarClientImpl.class);
        clientCnx = mock(ClientCnx.class);
        lookupService = mock(LookupService.class);
        connectionPool = mock(ConnectionPool.class);
        configurationData = mock(ClientConfigurationData.class);

        InetSocketAddress address = new InetSocketAddress(9000);

        CompletableFuture<Pair<InetSocketAddress, InetSocketAddress>> future =
            CompletableFuture.supplyAsync(() -> Pair.of(address, address));

        doReturn(future).when(lookupService).getBroker(eq(TopicName.get(successTopicName)));
        doReturn(configurationData).when(pulsarClient).getConfiguration();
        doReturn(lookupService).when(pulsarClient).getLookup();
        doReturn(connectionPool).when(pulsarClient).getCnxPool();

        CompletableFuture<ClientCnx> clientCnxFuture = CompletableFuture.supplyAsync(() -> clientCnx);
        doReturn(clientCnxFuture).when(connectionPool).getConnection(address, address);

        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                return CompletableFuture.completedFuture(null);
            }
        }).when(clientCnx).sendTxnRequestToTBWithId(any(), eq(1L));

        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                return FutureUtil.failedFuture(new PulsarClientException("Failed"));
            }
        }).when(clientCnx).sendTxnRequestToTBWithId(any(), eq(2L));
    }

    @Test(timeOut = 10000)
    public void testCommitSuccess() {
        TransactionCoordinatorClientImpl client = new TransactionCoordinatorClientImpl(pulsarClient);
        doReturn(1L).when(pulsarClient).newRequestId();
        try {
            client.commitTxnOnTopic(successTopicName, 1, 1).get();
        } catch (Exception e) {
            fail("commit transaction should succeed");
        }

        verify(clientCnx, times(1)).sendTxnRequestToTBWithId(any(ByteBuf.class), eq(1L));
        verify(pulsarClient, times(1)).getLookup();
        verify(lookupService, times(1)).getBroker(eq(TopicName.get(successTopicName)));

        try {
            client.commitTxnOnTopic(successTopicName, 2, 2).get();
        } catch (Exception e) {
            fail("commit transaction should succeed");
        }

        verify(clientCnx, times(2)).sendTxnRequestToTBWithId(any(ByteBuf.class), eq(1L));
        verify(pulsarClient, times(1)).getLookup();
        verify(lookupService, times(1)).getBroker(eq(TopicName.get(successTopicName)));
    }

    @Test(timeOut = 10000)
    public void testCommitFailed() {
        TransactionCoordinatorClientImpl client = new TransactionCoordinatorClientImpl(pulsarClient);

        doReturn(2L).when(pulsarClient).newRequestId();
        try {
            client.commitTxnOnTopic(successTopicName, 1, 1).get();
            fail("commit transaction should failed");
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof PulsarClientException);
        }
    }

    @Test(timeOut = 10000)
    public void testAbortSuccess() {
        TransactionCoordinatorClientImpl client = new TransactionCoordinatorClientImpl(pulsarClient);

        doReturn(1L).when(pulsarClient).newRequestId();
        try {
            client.abortTxnOnTopic(successTopicName, 1, 1).get();
        } catch (Exception e) {
            fail("abort transaction should succeed");
        }

        verify(clientCnx, times(1)).sendTxnRequestToTBWithId(any(ByteBuf.class), eq(1L));
        verify(pulsarClient, times(1)).getLookup();
        verify(lookupService, times(1)).getBroker(eq(TopicName.get(successTopicName)));

        try {
            client.abortTxnOnTopic(successTopicName, 2, 2).get();
        } catch (Exception e) {
            fail("abort transaction should succeed");
        }

        verify(clientCnx, times(2)).sendTxnRequestToTBWithId(any(ByteBuf.class), eq(1L));
        verify(pulsarClient, times(1)).getLookup();
        verify(lookupService, times(1)).getBroker(eq(TopicName.get(successTopicName)));

    }

    @Test(timeOut = 10000)
    public void testAbortFailed() {
        TransactionCoordinatorClientImpl client = new TransactionCoordinatorClientImpl(pulsarClient);

        doReturn(2L).when(pulsarClient).newRequestId();
        try {
            client.abortTxnOnTopic(successTopicName, 1, 1).get();
            fail("abort transaction should failed");
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof PulsarClientException);
        }
    }
}
