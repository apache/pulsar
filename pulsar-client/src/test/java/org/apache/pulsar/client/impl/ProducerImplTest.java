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

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import io.netty.channel.EventLoopGroup;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.apache.pulsar.client.impl.schema.BytesSchema;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.common.api.proto.CommandCloseProducer;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;

public class ProducerImplTest {

    private ExecutorProvider executorProvider;
    private ExecutorService internalExecutor;
    private ProducerImpl<byte[]> producer;
    private ProducerConfigurationData producerConfigurationData;


    @BeforeMethod(alwaysRun = true)
    public void setUp() {
        this.executorProvider = new ExecutorProvider(1, "ProducerImplTest");
        this.internalExecutor = Executors.newSingleThreadScheduledExecutor();
        this.producerConfigurationData = new ProducerConfigurationData();
        PulsarClientImpl client = ClientTestFixtures.createPulsarClientMock(executorProvider, internalExecutor);
        ClientConfigurationData clientConf = client.getConfiguration();
        clientConf.setOperationTimeoutMs(100);
        clientConf.setStatsIntervalSeconds(0);
        CompletableFuture<Producer<byte[]>> producerFuture = new CompletableFuture<>();
        String topic = "non-persistent://tenant/ns1/my-topic";
        this.producer = new ProducerImpl<>(client, topic, producerConfigurationData, producerFuture,
                0, BytesSchema.of(), null, Optional.of("test"));
    }

    @Test
    public void testChunkedMessageCtxDeallocate() {
        int totalChunks = 3;
        ProducerImpl.ChunkedMessageCtx ctx = ProducerImpl.ChunkedMessageCtx.get(totalChunks);
        MessageIdImpl testMessageId = new MessageIdImpl(1, 1, 1);
        ctx.firstChunkMessageId = testMessageId;

        for (int i = 0; i < totalChunks; i++) {
            ProducerImpl.OpSendMsg opSendMsg =
                    ProducerImpl.OpSendMsg.create(
                            MessageImpl.create(new MessageMetadata(), ByteBuffer.allocate(0), Schema.STRING, null),
                            null, 0, null);
            opSendMsg.chunkedMessageCtx = ctx;
            // check the ctx hasn't been deallocated.
            Assert.assertEquals(testMessageId, ctx.firstChunkMessageId);
            opSendMsg.recycle();
        }

        // check if the ctx is deallocated successfully.
        Assert.assertNull(ctx.firstChunkMessageId);
    }

    @Test
    public void testCommandCloseConsumerNotReconnect() {
        ClientConfigurationData conf = new ClientConfigurationData();
        EventLoopGroup mockEventLoop = mock(EventLoopGroup.class);
        ClientCnx cnx = new ClientCnx(conf, mockEventLoop);
        cnx.registerProducer(producer.producerId, producer);
        CommandCloseProducer commandCloseProducer = new CommandCloseProducer();
        commandCloseProducer.setProducerId(producer.producerId);
        commandCloseProducer.setRequestId(1);
        commandCloseProducer.setReconnect(false);
        cnx.handleCloseProducer(commandCloseProducer);
        Awaitility.await().untilAsserted(() -> {
            HandlerState.State state = producer.getState();
            org.testng.Assert.assertEquals(state, HandlerState.State.Closed);
        });
    }
}
