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
import io.netty.buffer.ByteBufAllocator;
import java.lang.reflect.Field;
import lombok.Cleanup;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SizeUnit;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

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
            try (MockedStatic<ClientCnx> mockedStatic = Mockito.mockStatic(ClientCnx.class)) {
                mockedStatic.when(ClientCnx::getMaxMessageSize).thenReturn(8);
                producer.send("memory-test".getBytes(StandardCharsets.UTF_8));
            }
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

    private void initClientWithMemoryLimit() throws PulsarClientException {
        replacePulsarClient(PulsarClient.builder().
                serviceUrl(lookupUrl.toString())
                .memoryLimit(50, SizeUnit.KILO_BYTES));
    }

}
