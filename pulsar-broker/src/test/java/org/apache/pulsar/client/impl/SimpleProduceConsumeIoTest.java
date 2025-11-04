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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.GenericFutureListener;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
public class SimpleProduceConsumeIoTest extends ProducerConsumerBase {

    private ExecutorService executor;
    private String topic;
    private PulsarClientImpl singleConnectionPerBrokerClient;

    @BeforeClass(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        executor.shutdown();
        super.internalCleanup();
    }

    @BeforeMethod
    public void setupTopic() throws Exception {
        executor = Executors.newSingleThreadExecutor();
        singleConnectionPerBrokerClient = (PulsarClientImpl) PulsarClient.builder().connectionsPerBroker(1)
                .serviceUrl(lookupUrl.toString()).build();
        topic = BrokerTestUtil.newUniqueName("persistent://public/default/tp");
        admin.topics().createNonPartitionedTopic(topic);
    }

    @AfterMethod(alwaysRun = true)
    public void afterMethod() throws Exception {
        if (singleConnectionPerBrokerClient != null) {
            singleConnectionPerBrokerClient.close();
        }
        executor.shutdown();
    }

    @Test
    public void testUnstableNetWorkForProducer() throws Exception {
        final var producer = (ProducerImpl<byte[]>) singleConnectionPerBrokerClient.newProducer().topic(topic).create();
        final var producerNetwork = new Network(producer.getClientCnx());
        producer.close();
        producerNetwork.close();

        final var newProducer = executor.submit(() -> createProducer(__ -> producerNetwork.waitForClose())).get().get();
        assertEquals(newProducer.getState().toString(), "Ready");
    }

    @Test
    public void testUnstableNetWorkForConsumer() throws Exception {
        final var consumer = (ConsumerImpl<byte[]>) singleConnectionPerBrokerClient.newConsumer().topic(topic)
                .subscriptionName("sub").subscribe();
        final var consumerNetwork = new Network(consumer.getClientCnx());
        consumer.close();
        consumerNetwork.close();

        final var newConsumer = executor.submit(() -> subscribe(cnx -> consumerNetwork.waitForClose())).get().get();
        assertEquals(newConsumer.getState().toString(), "Ready");
    }

    @Test
    public void testUnknownRpcExceptionForProducer() throws Exception {
        final var firstTime = new AtomicBoolean(true);
        final var producer = createProducer(cnx -> {
            if (firstTime.compareAndSet(true, false)) {
                setFailedContext(cnx);
            }
        }).get(3, TimeUnit.SECONDS);
        assertEquals(producer.getState().toString(), "Ready");
    }

    @Test
    public void testUnknownRpcExceptionForConsumer() throws Exception {
        final var firstTime = new AtomicBoolean(true);
        final var consumer = subscribe(cnx -> {
            if (firstTime.compareAndSet(true, false)) {
                setFailedContext(cnx);
            }
        }).get(3, TimeUnit.SECONDS);
        assertEquals(consumer.getState().toString(), "Ready");
    }

    private CompletableFuture<ProducerImpl<byte[]>> createProducer(
            java.util.function.Consumer<ClientCnx> cnxInterceptor) {
        final var producerConf = ((ProducerBuilderImpl<byte[]>) singleConnectionPerBrokerClient.newProducer()
                .topic(topic)).getConf();
        final var future = new CompletableFuture<Producer<byte[]>>();
        new ProducerImpl<>(singleConnectionPerBrokerClient, topic, producerConf, future,
                -1, Schema.BYTES, null, Optional.empty()) {
            @Override
            public CompletableFuture<Void> connectionOpened(ClientCnx cnx) {
                cnxInterceptor.accept(cnx);
                return super.connectionOpened(cnx);
            }
        };
        return future.thenApply(__ -> (ProducerImpl<byte[]>) __);
    }

    private CompletableFuture<ConsumerImpl<byte[]>> subscribe(java.util.function.Consumer<ClientCnx> cnxInterceptor) {
        final var consumerConf = ((ConsumerBuilderImpl<byte[]>) singleConnectionPerBrokerClient.newConsumer()
                .topic(topic).subscriptionName("sub")).getConf();
        final var future = new CompletableFuture<Consumer<byte[]>>();
        new ConsumerImpl<>(singleConnectionPerBrokerClient, topic, consumerConf,
                new ExecutorProvider(1, "internal"), -1, true, false, future, null, 3600, Schema.BYTES,
                new ConsumerInterceptors<>(List.of()), true) {

            @Override
            public CompletableFuture<Void> connectionOpened(ClientCnx cnx) {
                cnxInterceptor.accept(cnx);
                return super.connectionOpened(cnx);
            }
        };
        return future.thenApply(__ -> (ConsumerImpl<byte[]>) __);
    }

    private void setFailedContext(ClientCnx cnx) {
        final var oldCtx = cnx.ctx();
        final var newCtx = spy(oldCtx);
        doAnswer(invocationOnMock -> {
            final var failedFuture = mock(ChannelFuture.class);
            doAnswer(invocation -> {
                @SuppressWarnings("unchecked")
                final var listener = (GenericFutureListener<ChannelFuture>) invocation.getArgument(0);
                final var future = mock(ChannelFuture.class);
                when(future.isSuccess()).thenReturn(false);
                when(future.cause()).thenReturn(new RuntimeException("network exception"));
                listener.operationComplete(future);
                return future;
            }).when(failedFuture).addListener(any());
            // Set back the original context because reconnection will still get the same `ClientCnx` from the pool
            cnx.setCtx(oldCtx);
            return failedFuture;
        }).when(newCtx).writeAndFlush(any());

        cnx.setCtx(newCtx);
    }

    @RequiredArgsConstructor
    private static class Network {

        private final ClientCnx cnx;
        private final CountDownLatch countDownLatch = new CountDownLatch(1);

        public void close() {
            new Thread(() -> {
                try {
                    countDownLatch.await();
                    cnx.ctx().close();
                } catch (Exception ignored) {
                }
            }).start();
        }

        public void waitForClose() {
            countDownLatch.countDown();
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ignored) {
            }
        }
    }
}
