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

import java.time.Duration;
import lombok.Cleanup;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.common.api.proto.CommandSuccess;
import org.apache.pulsar.common.naming.TopicName;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
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

    /**
     * Param1: Producer enableBatch or not
     * Param2: Send in async way or not
     */
    @DataProvider(name = "produceConf")
    public Object[][] produceConf() {
        return new Object[][]{
                {true, true},
                {true, false},
                {false, true},
                {false, false}
        };
    }

    /**
     * Param1: Producer enableBatch or not
     * Param2: Send in async way or not
     */
    @DataProvider(name = "brokenPipeline")
    public Object[][] brokenPipeline() {
        return new Object[][]{
            {true},
            {false}
        };
    }

    @Test(dataProvider = "brokenPipeline")
    public void testProducerCloseCallback2(boolean brokenPipeline) throws Exception {
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
        Thread.sleep(3000);
        if (brokenPipeline) {
            //producer.getClientCnx().channel().config().setAutoRead(true);
            producer.getClientCnx().channel().close();
        } else {
            producer.getClientCnx().channel().config().setAutoRead(true);
        }
        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            System.out.println(1);
            Assert.assertTrue(completableFuture.isDone());
        });
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

    @Test(timeOut = 10_000, dataProvider = "produceConf")
    public void brokerCloseTopicTest(boolean enableBatch, boolean isAsyncSend) throws Exception {
        @Cleanup
        PulsarClient longBackOffClient = PulsarClient.builder()
                .startingBackoffInterval(5, TimeUnit.SECONDS)
                .maxBackoffInterval(5, TimeUnit.SECONDS)
                .serviceUrl(lookupUrl.toString())
                .build();
        String topic = "broker-close-test-" + RandomStringUtils.randomAlphabetic(5);
        @Cleanup
        ProducerImpl<byte[]> producer = (ProducerImpl<byte[]>) longBackOffClient.newProducer()
                .topic(topic)
                .enableBatching(enableBatch)
                .create();
        producer.newMessage().value("test".getBytes()).send();

        Optional<Topic> topicOptional = pulsar.getBrokerService()
                .getTopicReference(TopicName.get(topic).getPartitionedTopicName());
        Assert.assertTrue(topicOptional.isPresent());
        topicOptional.get().close(true).get();
        Awaitility.await().untilAsserted(() -> Assert.assertEquals(producer.getState(), HandlerState.State.Connecting));
        if (isAsyncSend) {
            producer.newMessage().value("test".getBytes()).sendAsync().get();
        } else {
            producer.newMessage().value("test".getBytes()).send();
        }
    }

    private void initClient() throws PulsarClientException {
        replacePulsarClient(PulsarClient.builder().
                serviceUrl(lookupUrl.toString()));
    }

}
