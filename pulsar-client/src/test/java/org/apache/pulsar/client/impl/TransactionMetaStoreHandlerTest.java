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
package org.apache.pulsar.client.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import io.netty.channel.Channel;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.PulsarClient;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

public class TransactionMetaStoreHandlerTest {

    @Test
    public void testStateChangeFailure() throws Exception {
        final PulsarClientImpl client = (PulsarClientImpl) PulsarClient.builder()
                .serviceUrl("pulsar://localhost:6650").build();
        final CompletableFuture<Void> connectFuture = new CompletableFuture<>();
        final TransactionMetaStoreHandler handler = new TransactionMetaStoreHandler(
                0L, client, "topic", connectFuture);
        final ClientCnx cnx = mock(ClientCnx.class);
        when(cnx.getRemoteEndpointProtocolVersion()).thenReturn(19);
        final CompletableFuture<ProducerResponse> responseFuture = CompletableFuture.completedFuture(null);
        when(cnx.sendRequestWithId(any(), anyLong())).thenReturn(responseFuture);

        final Channel channel = mock(Channel.class);
        when(cnx.channel()).thenReturn(channel);

        // Set an invalid state so that the state change will fail
        handler.setState(HandlerState.State.Terminated);
        handler.connectionOpened(cnx);

        Awaitility.await().atMost(Duration.ofSeconds(3)).until(connectFuture::isDone);

        assertTrue(connectFuture.isCompletedExceptionally());
        assertEquals(handler.getState(), HandlerState.State.Terminated);
        verify(cnx, times(0)).registerTransactionMetaStoreHandler(anyLong(), any());

        client.close();
    }
}
