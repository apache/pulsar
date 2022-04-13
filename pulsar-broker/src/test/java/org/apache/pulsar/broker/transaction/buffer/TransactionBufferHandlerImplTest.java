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

import org.apache.pulsar.broker.transaction.buffer.impl.TransactionBufferHandlerImpl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import org.apache.pulsar.client.impl.ClientCnx;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.api.proto.TxnAction;
import org.testng.annotations.Test;

import java.util.concurrent.CompletableFuture;

@Test(groups = "broker")
public class TransactionBufferHandlerImplTest {

    @Test
    public void testRequestCredits() {
        PulsarClientImpl pulsarClient = mock(PulsarClientImpl.class);
        when(pulsarClient.getConnection(anyString())).thenReturn(CompletableFuture.completedFuture(mock(ClientCnx.class)));
        TransactionBufferHandlerImpl handler = spy(
                new TransactionBufferHandlerImpl(pulsarClient, null, 1000, 3000));
        doNothing().when(handler).endTxn(any());
        for (int i = 0; i < 500; i++) {
            handler.endTxnOnTopic("t", 1L, 1L, TxnAction.COMMIT, 1L);
        }
        assertEquals(handler.getAvailableRequestCredits(), 500);
        for (int i = 0; i < 500; i++) {
            handler.endTxnOnTopic("t", 1L, 1L, TxnAction.COMMIT, 1L);
        }
        assertEquals(handler.getAvailableRequestCredits(), 0);
        handler.endTxnOnTopic("t", 1L, 1L, TxnAction.COMMIT, 1L);
        assertEquals(handler.getPendingRequestsCount(), 1);
        handler.onResponse(null);
        assertEquals(handler.getAvailableRequestCredits(), 0);
        assertEquals(handler.getPendingRequestsCount(), 0);
    }

    @Test
    public void testMinRequestCredits() {
        TransactionBufferHandlerImpl handler = spy(
                new TransactionBufferHandlerImpl(null, null, 50, 3000));
        assertEquals(handler.getAvailableRequestCredits(), 100);
    }
}
