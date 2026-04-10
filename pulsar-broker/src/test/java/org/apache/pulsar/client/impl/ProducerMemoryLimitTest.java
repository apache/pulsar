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
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import io.netty.buffer.ByteBufAllocator;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.PulsarClientSharedResources;
import org.apache.pulsar.client.api.SizeUnit;
import org.awaitility.Awaitility;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker-impl")
public class ProducerMemoryLimitTest extends ProducerConsumerBase {

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
    public void testProducerInvalidMessageMemoryRelease() throws Exception {
        initClientWithMemoryLimit();
        @Cleanup
        ProducerImpl<byte[]> producer = (ProducerImpl<byte[]>) pulsarClient.newProducer()
                .topic("testProducerMemoryLimit")
                .sendTimeout(5, TimeUnit.SECONDS)
                .batchingMaxPublishDelay(100, TimeUnit.MILLISECONDS)
                .batchingMaxBytes(10240)
                .enableBatching(true)
                .create();
        this.stopBroker();
        try {
            ConnectionHandler connectionHandler = Mockito.spy(producer.getConnectionHandler());
            Field field = producer.getClass().getDeclaredField("connectionHandler");
            field.setAccessible(true);
            field.set(producer, connectionHandler);
            when(connectionHandler.getMaxMessageSize()).thenReturn(8);
            producer.send("memory-test".getBytes(StandardCharsets.UTF_8));
            throw new IllegalStateException("can not reach here");
        } catch (PulsarClientException.InvalidMessageException ex) {
            PulsarClientImpl clientImpl = (PulsarClientImpl) this.pulsarClient;
            final MemoryLimitController memoryLimitController = clientImpl.getMemoryLimitController();
            Assert.assertEquals(memoryLimitController.currentUsage(), 0);
        }
    }

    @Test(timeOut = 10_000)
    public void testProducerTimeoutMemoryRelease() throws Exception {
        initClientWithMemoryLimit();
        @Cleanup
        ProducerImpl<byte[]> producer = (ProducerImpl<byte[]>) pulsarClient.newProducer()
                .topic("testProducerMemoryLimit")
                .sendTimeout(5, TimeUnit.SECONDS)
                .maxPendingMessages(0)
                .enableBatching(false)
                .create();
        this.stopBroker();
        try {
            producer.send("memory-test".getBytes(StandardCharsets.UTF_8));
            throw new IllegalStateException("can not reach here");
        } catch (PulsarClientException.TimeoutException ex) {
            PulsarClientImpl clientImpl = (PulsarClientImpl) this.pulsarClient;
            final MemoryLimitController memoryLimitController = clientImpl.getMemoryLimitController();
            Assert.assertEquals(memoryLimitController.currentUsage(), 0);
        }

    }

    @Test(timeOut = 10_000)
    public void testProducerBatchSendTimeoutMemoryRelease() throws Exception {
        initClientWithMemoryLimit();
        @Cleanup
        ProducerImpl<byte[]> producer = (ProducerImpl<byte[]>) pulsarClient.newProducer()
                .topic("testProducerMemoryLimit")
                .sendTimeout(5, TimeUnit.SECONDS)
                .maxPendingMessages(0)
                .enableBatching(true)
                .batchingMaxPublishDelay(100, TimeUnit.MILLISECONDS)
                .batchingMaxBytes(12)
                .create();
        this.stopBroker();
        try {
            producer.newMessage().value("memory-test".getBytes(StandardCharsets.UTF_8)).sendAsync();
            try {
                producer.newMessage().value("memory-test".getBytes(StandardCharsets.UTF_8)).sendAsync().get();
            } catch (Exception e) {
                throw PulsarClientException.unwrap(e);
            }

            throw new IllegalStateException("can not reach here");
        } catch (PulsarClientException.TimeoutException ex) {
            PulsarClientImpl clientImpl = (PulsarClientImpl) this.pulsarClient;
            final MemoryLimitController memoryLimitController = clientImpl.getMemoryLimitController();
            Assert.assertEquals(memoryLimitController.currentUsage(), 0);
        }
    }

    @Test(timeOut = 10_000)
    public void testBatchMessageOOMMemoryRelease() throws Exception {
        initClientWithMemoryLimit();
        @Cleanup
        ProducerImpl<byte[]> producer = (ProducerImpl<byte[]>) pulsarClient.newProducer()
                .topic("testProducerMemoryLimit")
                .sendTimeout(5, TimeUnit.SECONDS)
                .maxPendingMessages(0)
                .enableBatching(true)
                .batchingMaxPublishDelay(100, TimeUnit.MILLISECONDS)
                .batchingMaxBytes(12)
                .create();
        this.stopBroker();

        try {
            ProducerImpl<byte[]> spyProducer = Mockito.spy(producer);
            final ByteBufAllocator mockAllocator = mock(ByteBufAllocator.class);
            doAnswer((ignore) -> {
                throw new OutOfMemoryError("memory-test");
            }).when(mockAllocator).buffer(anyInt());

            final BatchMessageContainerImpl batchMessageContainer = new BatchMessageContainerImpl(mockAllocator);
            /* Without `batchMessageContainer.setProducer(producer);` it throws NPE since producer is null, and
                eventually sendAsync() catches this NPE and releases the memory and semaphore.
                } catch (Throwable t) {
                    completeCallbackAndReleaseSemaphore(uncompressedSize, callback,
                            new PulsarClientException(t, msg.getSequenceId()));
                }
            */
            batchMessageContainer.setProducer(producer);
            Field batchMessageContainerField = ProducerImpl.class.getDeclaredField("batchMessageContainer");
            batchMessageContainerField.setAccessible(true);
            batchMessageContainerField.set(spyProducer, batchMessageContainer);

            spyProducer.send("memory-test".getBytes(StandardCharsets.UTF_8));
            Assert.fail("can not reach here");
    } catch (PulsarClientException ex) {
        PulsarClientImpl clientImpl = (PulsarClientImpl) this.pulsarClient;
        final MemoryLimitController memoryLimitController = clientImpl.getMemoryLimitController();
        Assert.assertEquals(memoryLimitController.currentUsage(), 0);
    }
    }

    @Test(timeOut = 10_000)
    public void testProducerCloseMemoryRelease() throws Exception {
        initClientWithMemoryLimit();
        @Cleanup
        ProducerImpl<byte[]> producer = (ProducerImpl<byte[]>) pulsarClient.newProducer()
                .topic("testProducerMemoryLimit")
                .sendTimeout(5, TimeUnit.SECONDS)
                .maxPendingMessages(0)
                .enableBatching(false)
                .create();
        this.stopBroker();
        producer.sendAsync("memory-test".getBytes(StandardCharsets.UTF_8));
        producer.close();
        PulsarClientImpl clientImpl = (PulsarClientImpl) this.pulsarClient;
        final MemoryLimitController memoryLimitController = clientImpl.getMemoryLimitController();
        Assert.assertEquals(memoryLimitController.currentUsage(), 0);
    }

    @Test(timeOut = 10_000)
    public void testProducerBlockReserveMemory() throws Exception {
        replacePulsarClient(PulsarClient.builder().
                serviceUrl(lookupUrl.toString())
                .memoryLimit(1, SizeUnit.KILO_BYTES));
        @Cleanup
        ProducerImpl<byte[]> producer = (ProducerImpl<byte[]>) pulsarClient.newProducer()
                .topic("testProducerMemoryLimit")
                .sendTimeout(5, TimeUnit.SECONDS)
                .compressionType(CompressionType.SNAPPY)
                .messageRoutingMode(MessageRoutingMode.RoundRobinPartition)
                .maxPendingMessages(0)
                .blockIfQueueFull(true)
                .enableBatching(true)
                .batchingMaxMessages(100)
                .batchingMaxBytes(65536)
                .batchingMaxPublishDelay(100, TimeUnit.MILLISECONDS)
                .create();
        int msgCount = 5;
        CountDownLatch cdl = new CountDownLatch(msgCount);
        for (int i = 0; i < msgCount; i++) {
            producer.sendAsync("memory-test".getBytes(StandardCharsets.UTF_8)).whenComplete(((messageId, throwable) -> {
                cdl.countDown();
            }));
        }

        cdl.await();

        producer.close();
        PulsarClientImpl clientImpl = (PulsarClientImpl) this.pulsarClient;
        final MemoryLimitController memoryLimitController = clientImpl.getMemoryLimitController();
        Assert.assertEquals(memoryLimitController.currentUsage(), 0);
    }

    @Test(timeOut = 15000)
    public void testMultiPulsarClientProducerShareMemoryLimitController() throws Exception {
        int msgSize = 100;
        int msgCount = 6;
        int memoryLimit = msgSize * msgCount;
        PulsarClientSharedResources sharedResources = PulsarClientSharedResources.builder()
                .configureMemoryLimitController(
                        memoryLimitConfig -> memoryLimitConfig.memoryLimit(memoryLimit, SizeUnit.BYTES)).build();
        @Cleanup
        PulsarClientImpl pulsarClient1 = ((PulsarClientImpl) PulsarClient.builder().serviceUrl(lookupUrl.toString())
                .sharedResources(sharedResources).build());
        @Cleanup
        PulsarClientImpl pulsarClient2 = ((PulsarClientImpl) PulsarClient.builder().serviceUrl(lookupUrl.toString())
                .sharedResources(sharedResources).build());

        Assert.assertSame(pulsarClient1.getMemoryLimitController(), pulsarClient2.getMemoryLimitController());

        Producer<byte[]> producer1 = pulsarClient1.newProducer()
                .topic("testProducerShareMemoryLimitController")
                .sendTimeout(20, TimeUnit.SECONDS)
                .maxPendingMessages(0)
                .blockIfQueueFull(false)
                .enableBatching(false)
                .create();
        Producer<byte[]> producer2 = pulsarClient2.newProducer()
                .topic("testProducerShareMemoryLimitController")
                .sendTimeout(20, TimeUnit.SECONDS)
                .maxPendingMessages(0)
                .blockIfQueueFull(false)
                .enableBatching(false)
                .create();

        this.stopBroker();

        byte[] msgBytes = new byte[msgSize];
        int halfMsgCount = msgCount / 2;
        int producer1MemoryUsage = halfMsgCount * msgSize;
        for (int i = 0; i < msgCount / 2; i++) {
            producer1.sendAsync(msgBytes);
        }
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(() -> pulsarClient1.getMemoryLimitController().currentUsage() == producer1MemoryUsage);

        // The MemoryLimitController.tryReserveMemory() method returns false only when currentUsage > memoryLimit.
        int producer2MsgCount = halfMsgCount + 1;
        int producer2MemoryUsage = producer2MsgCount * msgSize;
        int totalMemoryUsage = producer1MemoryUsage + producer2MemoryUsage;
        for (int i = 0; i < producer2MsgCount; i++) {
            producer2.sendAsync(msgBytes);
        }
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(() -> pulsarClient2.getMemoryLimitController().currentUsage() == totalMemoryUsage);

        try {
            producer1.send(msgBytes);
            Assert.fail("producer can not send message due to memory limit exceeded");
        } catch (PulsarClientException.MemoryBufferIsFullError ex) {
            long currentUsage = pulsarClient1.getMemoryLimitController().currentUsage();
            Assert.assertEquals(currentUsage, totalMemoryUsage);
        }

        try {
            producer2.send(msgBytes);
            Assert.fail("producer can not send message due to memory limit exceeded");
        } catch (PulsarClientException.MemoryBufferIsFullError ex) {
            long currentUsage = pulsarClient2.getMemoryLimitController().currentUsage();
            Assert.assertEquals(currentUsage, totalMemoryUsage);
        }

        producer1.close();
        Assert.assertEquals(pulsarClient1.getMemoryLimitController().currentUsage(), producer2MemoryUsage);

        for (int i = 0; i < halfMsgCount; i++) {
            producer2.sendAsync(msgBytes);
        }
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(() -> pulsarClient2.getMemoryLimitController().currentUsage() == totalMemoryUsage);

        producer2.close();
        Assert.assertEquals(pulsarClient1.getMemoryLimitController().currentUsage(), 0);
    }

    private void initClientWithMemoryLimit() throws PulsarClientException {
        replacePulsarClient(PulsarClient.builder().
                serviceUrl(lookupUrl.toString())
                .memoryLimit(50, SizeUnit.KILO_BYTES));
    }

}
