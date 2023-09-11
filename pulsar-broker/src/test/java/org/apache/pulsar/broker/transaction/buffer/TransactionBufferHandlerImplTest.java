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

import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.namespace.NamespaceEphemeralData;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.transaction.buffer.impl.TransactionBufferHandlerImpl;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import org.apache.pulsar.client.impl.ClientCnx;
import org.apache.pulsar.client.impl.ConnectionPool;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.api.proto.TxnAction;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

@Test(groups = "broker")
public class TransactionBufferHandlerImplTest {

    @Test
    public void testRequestCredits() throws PulsarServerException {
        PulsarClientImpl pulsarClient = mock(PulsarClientImpl.class);
        ConnectionPool connectionPool = mock(ConnectionPool.class);
        when(pulsarClient.getCnxPool()).thenReturn(connectionPool);
        PulsarService pulsarService = mock(PulsarService.class);
        NamespaceService namespaceService = mock(NamespaceService.class);
        when(pulsarService.getNamespaceService()).thenReturn(namespaceService);
        when(pulsarService.getClient()).thenReturn(pulsarClient);
        when(namespaceService.getBundleAsync(any())).thenReturn(CompletableFuture.completedFuture(mock(NamespaceBundle.class)));
        Optional<NamespaceEphemeralData> opData = Optional.empty();
        when(namespaceService.getOwnerAsync(any())).thenReturn(CompletableFuture.completedFuture(opData));
        when(((PulsarClientImpl)pulsarClient).getConnection(anyString(), anyInt()))
                .thenReturn(CompletableFuture.completedFuture(mock(ClientCnx.class)));
        when(((PulsarClientImpl)pulsarClient).getConnection(anyString()))
                .thenReturn(CompletableFuture.completedFuture(mock(ClientCnx.class)));
        TransactionBufferHandlerImpl handler = spy(new TransactionBufferHandlerImpl(pulsarService, null, 1000, 3000));
        doNothing().when(handler).endTxn(any());
        doReturn(CompletableFuture.completedFuture(mock(ClientCnx.class))).when(handler).getClientCnx(anyString());
        for (int i = 0; i < 500; i++) {
            handler.endTxnOnTopic("public/default/t", 1L, 1L, TxnAction.COMMIT, 1L);
        }
        assertEquals(handler.getAvailableRequestCredits(), 500);
        for (int i = 0; i < 500; i++) {
            handler.endTxnOnTopic("public/default/t", 1L, 1L, TxnAction.COMMIT, 1L);
        }
        assertEquals(handler.getAvailableRequestCredits(), 0);
        handler.endTxnOnTopic("public/default/t", 1L, 1L, TxnAction.COMMIT, 1L);
        assertEquals(handler.getPendingRequestsCount(), 1);
        handler.onResponse(null);
        assertEquals(handler.getAvailableRequestCredits(), 0);
        assertEquals(handler.getPendingRequestsCount(), 0);
    }

    @Test
    public void testMinRequestCredits() throws PulsarServerException {
        ConnectionPool connectionPool = mock(ConnectionPool.class);
        PulsarClientImpl pulsarClient = mock(PulsarClientImpl.class);
        when(pulsarClient.getCnxPool()).thenReturn(connectionPool);
        PulsarService pulsarService = mock(PulsarService.class);
        when(pulsarService.getClient()).thenReturn(pulsarClient);
        TransactionBufferHandlerImpl handler = spy(new TransactionBufferHandlerImpl(pulsarService, null, 50, 3000));
        assertEquals(handler.getAvailableRequestCredits(), 100);
    }
}
