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
 */
package org.apache.pulsar.client.impl;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;
import static org.testng.Assert.*;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import io.netty.util.HashedWheelTimer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.apache.pulsar.client.impl.metrics.LatencyHistogram;
import org.apache.pulsar.client.impl.metrics.InstrumentProvider;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class ProducerImplTest {
    @Test
    public void testChunkedMessageCtxDeallocate() {
        int totalChunks = 3;
        ProducerImpl.ChunkedMessageCtx ctx = ProducerImpl.ChunkedMessageCtx.get(totalChunks);
        MessageIdImpl testMessageId = new MessageIdImpl(1, 1, 1);
        ctx.firstChunkMessageId = testMessageId;

        for (int i = 0; i < totalChunks; i++) {
            ProducerImpl.OpSendMsg opSendMsg =
                    ProducerImpl.OpSendMsg.create(
                            LatencyHistogram.NOOP,
                            MessageImpl.create(new MessageMetadata(), ByteBuffer.allocate(0), Schema.STRING, null),
                            null, 0, null);
            opSendMsg.chunkedMessageCtx = ctx;
            // check the ctx hasn't been deallocated.
            assertEquals(ctx.firstChunkMessageId, testMessageId);
            opSendMsg.recycle();
        }

        // check if the ctx is deallocated successfully.
        assertNull(ctx.firstChunkMessageId);
    }

    @Test
    public void testPopulateMessageSchema() {
        MessageImpl<?> msg = mock(MessageImpl.class);
        when(msg.hasReplicateFrom()).thenReturn(true);
        when(msg.getSchemaInternal()).thenReturn(mock(Schema.class));
        when(msg.getSchemaInfoForReplicator()).thenReturn(null);
        ProducerImpl<?> producer = mock(ProducerImpl.class, withSettings()
                .defaultAnswer(Mockito.CALLS_REAL_METHODS));
        assertTrue(producer.populateMessageSchema(msg, null));
        verify(msg).setSchemaState(MessageImpl.SchemaState.Ready);
    }

    @Test
    public void testClearPendingMessageWhenCloseAsync() {
        PulsarClientImpl client = mock(PulsarClientImpl.class);
        Mockito.doReturn(1L).when(client).newProducerId();
        ClientConfigurationData clientConf = new ClientConfigurationData();
        clientConf.setStatsIntervalSeconds(-1);
        Mockito.doReturn(clientConf).when(client).getConfiguration();
        Mockito.doReturn(new InstrumentProvider(null)).when(client).instrumentProvider();
        ConnectionPool connectionPool = mock(ConnectionPool.class);
        Mockito.doReturn(1).when(connectionPool).genRandomKeyToSelectCon();
        Mockito.doReturn(connectionPool).when(client).getCnxPool();
        HashedWheelTimer timer = mock(HashedWheelTimer.class);
        Mockito.doReturn(null).when(timer).newTimeout(Mockito.any(), Mockito.anyLong(), Mockito.any());
        Mockito.doReturn(timer).when(client).timer();
        ProducerConfigurationData producerConf = new ProducerConfigurationData();
        producerConf.setSendTimeoutMs(-1);
        ProducerImpl<?> producer = Mockito.spy(new ProducerImpl<>(client, "topicName", producerConf, null, 0, null, null, Optional.empty()));
        
        // make sure throw exception when send request to broker
        ClientCnx clientCnx = mock(ClientCnx.class);
        CompletableFuture<ProducerResponse> tCompletableFuture = new CompletableFuture<>();
        tCompletableFuture.completeExceptionally(new PulsarClientException("error"));
        when(clientCnx.sendRequestWithId(Mockito.any(), Mockito.anyLong())).thenReturn(tCompletableFuture);
        Mockito.doReturn(clientCnx).when(producer).cnx();

        // run closeAsync and verify
        CompletableFuture<Void> voidCompletableFuture = producer.closeAsync();
        verify(producer).closeAndClearPendingMessages();
    }

}
