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

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.apache.pulsar.client.impl.metrics.InstrumentProvider;
import org.apache.pulsar.client.impl.metrics.LatencyHistogram;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.testng.annotations.Test;

public class GeoReplicationProducerImplTest {

    private static final String TOPIC = "persistent://public/default/geo-repl-producer";

    @Test
    public void testRewindDuplicateAckWithoutTargetPositionFailsCallback() {
        GeoReplicationProducerImpl producer = newProducer();
        producer.setLastPersistedSourcePosition(7, 9);
        AtomicReference<Throwable> callbackError = new AtomicReference<>(new AssertionError("not called"));
        ProducerImpl.OpSendMsg op = newPendingOp(7, 3, callbackError);
        producer.pendingMessages.add(op);

        producer.ackReceived(newClientCnx(), 7, 3, -1, -1);

        assertTrue(callbackError.get() instanceof PulsarClientException);
        assertEquals(producer.pendingMessages.messagesCount(), 0);
    }

    @Test
    public void testRewindAckWithTargetPositionCompletesCallback() {
        GeoReplicationProducerImpl producer = newProducer();
        producer.setLastPersistedSourcePosition(7, 9);
        AtomicReference<Throwable> callbackError = new AtomicReference<>(new AssertionError("not called"));
        ProducerImpl.OpSendMsg op = newPendingOp(7, 3, callbackError);
        producer.pendingMessages.add(op);

        producer.ackReceived(newClientCnx(), 7, 3, 10, 11);

        assertNull(callbackError.get());
        assertEquals(producer.pendingMessages.messagesCount(), 0);
    }

    @Test
    public void testRewindDuplicateAckAtLastPersistedPositionCompletesCallback() {
        GeoReplicationProducerImpl producer = newProducer();
        producer.setLastPersistedSourcePosition(7, 3);
        AtomicReference<Throwable> callbackError = new AtomicReference<>(new AssertionError("not called"));
        ProducerImpl.OpSendMsg op = newPendingOp(7, 3, callbackError);
        producer.pendingMessages.add(op);

        producer.ackReceived(newClientCnx(), 7, 3, -1, -1);

        assertNull(callbackError.get());
        assertEquals(producer.pendingMessages.messagesCount(), 0);
    }

    private static GeoReplicationProducerImpl newProducer() {
        PulsarClientImpl client = mock(PulsarClientImpl.class);
        ClientConfigurationData clientConfiguration = new ClientConfigurationData();
        clientConfiguration.setStatsIntervalSeconds(0);
        ConnectionPool connectionPool = mock(ConnectionPool.class);
        when(connectionPool.genRandomKeyToSelectCon()).thenReturn(1);
        when(client.getCnxPool()).thenReturn(connectionPool);
        when(client.newProducerId()).thenReturn(1L);
        when(client.getConfiguration()).thenReturn(clientConfiguration);
        when(client.instrumentProvider()).thenReturn(InstrumentProvider.NOOP);
        when(client.getMemoryLimitController()).thenReturn(mock(MemoryLimitController.class));
        when(client.getConnection(anyString(), anyInt())).thenReturn(new CompletableFuture<>());

        ProducerConfigurationData producerConfiguration = new ProducerConfigurationData();
        producerConfiguration.setBatchingEnabled(false);
        producerConfiguration.setMaxPendingMessages(0);
        producerConfiguration.setSendTimeoutMs(0);
        CompletableFuture<Producer<byte[]>> createdFuture = new CompletableFuture<>();
        return new GeoReplicationProducerImpl(client, TOPIC, producerConfiguration, createdFuture, -1,
                Schema.BYTES, null, Optional.empty());
    }

    private static ClientCnx newClientCnx() {
        ClientCnx cnx = mock(ClientCnx.class);
        when(cnx.isBrokerSupportsReplDedupByLidAndEid()).thenReturn(true);
        return cnx;
    }

    private static ProducerImpl.OpSendMsg newPendingOp(long ledgerId, long entryId,
                                                       AtomicReference<Throwable> callbackError) {
        MessageMetadata metadata = new MessageMetadata();
        metadata.setProducerName("repl");
        metadata.setSequenceId(entryId);
        metadata.setPublishTime(System.currentTimeMillis());
        metadata.addProperty()
                .setKey(GeoReplicationProducerImpl.MSG_PROP_REPL_SOURCE_POSITION)
                .setValue(ledgerId + ":" + entryId);
        MessageImpl<byte[]> message = MessageImpl.create(metadata, ByteBuffer.allocate(0), Schema.BYTES, TOPIC);
        return ProducerImpl.OpSendMsg.create(LatencyHistogram.NOOP, message, null, entryId, new SendCallback() {
            @Override
            public void sendComplete(Throwable error, OpSendMsgStats opSendMsgStats) {
                callbackError.set(error);
            }

            @Override
            public void addCallback(MessageImpl<?> msg, SendCallback scb) {
            }

            @Override
            public SendCallback getNextSendCallback() {
                return null;
            }

            @Override
            public MessageImpl<?> getNextMessage() {
                return null;
            }

            @Override
            public CompletableFuture<MessageId> getFuture() {
                return null;
            }
        });
    }
}
