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

import lombok.Cleanup;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.common.api.proto.CommandSuccess;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Test(groups = "broker-impl")
public class ProducerCloseTest extends ProducerConsumerBase {

    @Override
    @BeforeMethod
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @Override
    @AfterMethod(alwaysRun = true)
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test(timeOut = 10_000)
    public void testProducerCloseCallback() throws Exception {
        initClient();
        @Cleanup
        ProducerImpl<byte[]> producer = (ProducerImpl<byte[]>) pulsarClient.newProducer()
                .topic("testProducerClose")
                .sendTimeout(5, TimeUnit.SECONDS)
                .maxPendingMessages(0)
                .enableBatching(false)
                .create();
        final TypedMessageBuilder<byte[]> messageBuilder = producer.newMessage();
        final TypedMessageBuilder<byte[]> value = messageBuilder.value("test-msg".getBytes(StandardCharsets.UTF_8));
        producer.getClientCnx().channel().config().setAutoRead(false);
        final CompletableFuture<MessageId> completableFuture = value.sendAsync();
        producer.closeAsync();
        final CommandSuccess commandSuccess = new CommandSuccess();
        PulsarClientImpl clientImpl = (PulsarClientImpl) this.pulsarClient;
        commandSuccess.setRequestId(clientImpl.newRequestId() -1);
        producer.getClientCnx().handleSuccess(commandSuccess);
        Thread.sleep(3000);
        Assert.assertEquals(completableFuture.isDone(), true);
    }

    @Test(timeOut = 10_000)
    public void testProducerCloseFailsPendingBatchWhenPreviousStateNotReadyCallback() throws Exception {
        initClient();
        @Cleanup
        ProducerImpl<byte[]> producer = (ProducerImpl<byte[]>) pulsarClient.newProducer()
                .topic("testProducerClose")
                .maxPendingMessages(10)
                .batchingMaxPublishDelay(10, TimeUnit.SECONDS)
                .batchingMaxBytes(Integer.MAX_VALUE)
                .enableBatching(true)
                .create();
        CompletableFuture<MessageId> completableFuture = producer.newMessage()
            .value("test-msg".getBytes(StandardCharsets.UTF_8))
            .sendAsync();
        // By setting the state to Failed, the close method will exit early because the previous state was not Ready.
        producer.setState(HandlerState.State.Failed);
        producer.closeAsync();
        Assert.assertTrue(completableFuture.isCompletedExceptionally());
        try {
            completableFuture.get();
        } catch (ExecutionException e) {
            Assert.assertTrue(e.getCause() instanceof PulsarClientException.AlreadyClosedException);
        }
    }

    private void initClient() throws PulsarClientException {
        pulsarClient = PulsarClient.builder().
                serviceUrl(lookupUrl.toString())
                .build();
    }

}
