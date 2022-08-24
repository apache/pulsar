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
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.util.FutureUtil;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@Test(groups = "broker-impl")
public class ProducerSemaphoreTest extends ProducerConsumerBase {

    @Override
    @BeforeMethod
    public void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @Override
    @AfterMethod(alwaysRun = true)
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test(timeOut = 10_000)
    public void testProducerSemaphoreInvalidMessage() throws Exception {
        final int pendingQueueSize = 100;

        @Cleanup
        ProducerImpl<byte[]> producer = (ProducerImpl<byte[]>) pulsarClient.newProducer()
                .topic("testProducerSemaphoreAcquire")
                .maxPendingMessages(pendingQueueSize)
                .enableBatching(true)
                .create();

        this.stopBroker();
        try {
            try (MockedStatic<ClientCnx> mockedStatic = Mockito.mockStatic(ClientCnx.class)) {
                mockedStatic.when(ClientCnx::getMaxMessageSize).thenReturn(2);
                producer.send("semaphore-test".getBytes(StandardCharsets.UTF_8));
            }
            throw new IllegalStateException("can not reach here");
        } catch (PulsarClientException.InvalidMessageException ex) {
            Assert.assertEquals(producer.getSemaphore().get().availablePermits(), pendingQueueSize);
        }

        producer.conf.setBatchingEnabled(false);
        try {
            try (MockedStatic<ClientCnx> mockedStatic = Mockito.mockStatic(ClientCnx.class)) {
                mockedStatic.when(ClientCnx::getMaxMessageSize).thenReturn(2);
                producer.send("semaphore-test".getBytes(StandardCharsets.UTF_8));
            }
            throw new IllegalStateException("can not reach here");
        } catch (PulsarClientException.InvalidMessageException ex) {
            Assert.assertEquals(producer.getSemaphore().get().availablePermits(), pendingQueueSize);
        }
    }

    @Test(timeOut = 30000)
    public void testProducerSemaphoreAcquireAndRelease() throws PulsarClientException, ExecutionException, InterruptedException {

        final int pendingQueueSize = 100;

        @Cleanup
        ProducerImpl<byte[]> producer = (ProducerImpl<byte[]>) pulsarClient.newProducer()
                .topic("testProducerSemaphoreAcquire")
                .maxPendingMessages(pendingQueueSize)
                .enableBatching(false)
                .create();

        final int messages = 10;
        final List<CompletableFuture<MessageId>> futures = new ArrayList<>(messages);
        producer.getClientCnx().channel().config().setAutoRead(false);
        try {
            for (int i = 0; i < messages; i++) {
                futures.add(producer.newMessage().value(("Semaphore-test-" + i).getBytes()).sendAsync());
            }
            Assert.assertEquals(producer.getSemaphore().get().availablePermits(), pendingQueueSize - messages);
            Assert.assertFalse(producer.isErrorStat());
        } finally {
            producer.getClientCnx().channel().config().setAutoRead(true);
        }
        FutureUtil.waitForAll(futures).get();
        Assert.assertEquals(producer.getSemaphore().get().availablePermits(), pendingQueueSize);
        Assert.assertFalse(producer.isErrorStat());
        futures.clear();

        // Simulate replicator, non batching message but `numMessagesInBatch` of message metadata > 1
        producer.getClientCnx().channel().config().setAutoRead(false);
        try {
            for (int i = 0; i < messages / 2; i++) {
                MessageMetadata metadata = new MessageMetadata()
                        .setNumMessagesInBatch(10);
                MessageImpl<byte[]> msg = MessageImpl.create(metadata, ByteBuffer.wrap(new byte[0]), Schema.BYTES, null);
                futures.add(producer.sendAsync(msg));
            }
            Assert.assertEquals(producer.getSemaphore().get().availablePermits(), pendingQueueSize - messages/2);
            Assert.assertFalse(producer.isErrorStat());
        } finally {
            producer.getClientCnx().channel().config().setAutoRead(true);
        }
        FutureUtil.waitForAll(futures).get();
        Assert.assertEquals(producer.getSemaphore().get().availablePermits(), pendingQueueSize);
        Assert.assertFalse(producer.isErrorStat());
        futures.clear();

        // Here must ensure that the semaphore available permits is 0
        Assert.assertEquals(producer.getSemaphore().get().availablePermits(), pendingQueueSize);
        Assert.assertFalse(producer.isErrorStat());

        // Acquire 5 and not wait the send ack call back
        producer.getClientCnx().channel().config().setAutoRead(false);
        try {
            for (int i = 0; i < messages / 2; i++) {
                futures.add(producer.newMessage().value(("Semaphore-test-" + i).getBytes()).sendAsync());
            }

            // Here must ensure that the Semaphore a acquired 5
            Assert.assertEquals(producer.getSemaphore().get().availablePermits(), pendingQueueSize - messages / 2);
            Assert.assertFalse(producer.isErrorStat());
        } finally {
            producer.getClientCnx().channel().config().setAutoRead(true);

        }
        FutureUtil.waitForAll(futures).get();
        Assert.assertEquals(producer.getSemaphore().get().availablePermits(), pendingQueueSize);
        Assert.assertFalse(producer.isErrorStat());
    }

    /**
     * We use semaphore to limit the pending send, so we must ensure that the thread of sending message never block
     * at the pending message queue. If not, the dead lock might occur. Here is the related issue to describe the
     * dead lock happens {https://github.com/apache/pulsar/issues/5585}
     */
    @Test(timeOut = 30000)
    public void testEnsureNotBlockOnThePendingQueue() throws Exception {
        final int pendingQueueSize = 10;

        @Cleanup
        ProducerImpl<byte[]> producer = (ProducerImpl<byte[]>) pulsarClient.newProducer()
                .topic("testProducerSemaphoreAcquire")
                .maxPendingMessages(pendingQueueSize)
                .enableBatching(false)
                .create();

        final List<CompletableFuture<MessageId>> futures = new ArrayList<>();

        // Simulate replicator, non batching message but `numMessagesInBatch` of message metadata > 1
        // Test that when we fill the queue with "replicator" messages, we are notified
        // (replicator itself would block)
        Assert.assertEquals(producer.getSemaphore().get().availablePermits(), pendingQueueSize);
        Assert.assertFalse(producer.isErrorStat());
        producer.getClientCnx().channel().config().setAutoRead(false);
        try {
            for (int i = 0; i < pendingQueueSize; i++) {
                MessageMetadata metadata = new MessageMetadata()
                        .setNumMessagesInBatch(10);

                MessageImpl<byte[]> msg = MessageImpl.create(metadata, ByteBuffer.wrap(new byte[0]), Schema.BYTES, null);
                futures.add(producer.sendAsync(msg));
            }
            Assert.assertEquals(producer.getSemaphore().get().availablePermits(), 0);
            Assert.assertFalse(producer.isErrorStat());
            try {
                MessageMetadata metadata = new MessageMetadata()
                        .setNumMessagesInBatch(10);

                MessageImpl<byte[]> msg = MessageImpl.create(metadata, ByteBuffer.wrap(new byte[0]), Schema.BYTES, null);
                producer.sendAsync(msg).get();
                Assert.fail("Shouldn't be able to send message");
            } catch (ExecutionException ee) {
                Assert.assertEquals(ee.getCause().getClass(),
                                    PulsarClientException.ProducerQueueIsFullError.class);
                Assert.assertEquals(producer.getSemaphore().get().availablePermits(), 0);
                Assert.assertFalse(producer.isErrorStat());
            }
        } finally {
            producer.getClientCnx().channel().config().setAutoRead(true);
        }
        FutureUtil.waitForAll(futures).get();
        futures.clear();

        // Test that when we fill the queue with normal messages, we get an error
        Assert.assertEquals(producer.getSemaphore().get().availablePermits(), pendingQueueSize);
        Assert.assertFalse(producer.isErrorStat());
        producer.getClientCnx().channel().config().setAutoRead(false);
        try {
            for (int i = 0; i < pendingQueueSize; i++) {
                futures.add(producer.newMessage().value(("Semaphore-test-" + i).getBytes()).sendAsync());
            }
            Assert.assertEquals(producer.getSemaphore().get().availablePermits(), 0);
            Assert.assertFalse(producer.isErrorStat());

            try {
                producer.newMessage().value(("Semaphore-test-Q-full").getBytes()).sendAsync().get();
            } catch (ExecutionException ee) {
                Assert.assertEquals(ee.getCause().getClass(),
                                    PulsarClientException.ProducerQueueIsFullError.class);
                Assert.assertEquals(producer.getSemaphore().get().availablePermits(), 0);
                Assert.assertFalse(producer.isErrorStat());

            }
        } finally {
            producer.getClientCnx().channel().config().setAutoRead(true);
        }
        FutureUtil.waitForAll(futures).get();
        Assert.assertEquals(producer.getSemaphore().get().availablePermits(), pendingQueueSize);
        Assert.assertFalse(producer.isErrorStat());
    }

    @Test(timeOut = 10_000)
    public void testBatchMessageSendTimeoutProducerSemaphoreRelease() throws Exception {
        final int pendingQueueSize = 10;
        @Cleanup
        ProducerImpl<byte[]> producer =
                (ProducerImpl<byte[]>) pulsarClient.newProducer()
                        .topic("testProducerSemaphoreRelease")
                        .sendTimeout(2, TimeUnit.SECONDS)
                        .maxPendingMessages(pendingQueueSize)
                        .enableBatching(true)
                        .batchingMaxPublishDelay(100, TimeUnit.MILLISECONDS)
                        .batchingMaxBytes(15)
                        .create();
        this.stopBroker();
        try {
            ProducerImpl<byte[]> spyProducer = Mockito.spy(producer);
            // Make the pendingMessages not empty
            spyProducer.newMessage().value("semaphore-test".getBytes(StandardCharsets.UTF_8)).sendAsync();
            spyProducer.newMessage().value("semaphore-test".getBytes(StandardCharsets.UTF_8)).sendAsync();

            Field batchMessageContainerField = ProducerImpl.class.getDeclaredField("batchMessageContainer");
            batchMessageContainerField.setAccessible(true);
            BatchMessageContainerImpl batchMessageContainer =
                    (BatchMessageContainerImpl) batchMessageContainerField.get(spyProducer);
            batchMessageContainer.setProducer(spyProducer);
            Mockito.doThrow(new PulsarClientException.CryptoException("crypto error")).when(spyProducer)
                    .encryptMessage(any(), any());

            try {
                spyProducer.newMessage().value("memory-test".getBytes(StandardCharsets.UTF_8)).sendAsync().get();
            } catch (Exception e) {
                throw PulsarClientException.unwrap(e);
            }

            throw new IllegalStateException("can not reach here");
        } catch (PulsarClientException.TimeoutException ex) {
            Assert.assertEquals(producer.getSemaphore().get().availablePermits(), pendingQueueSize);
        }
    }
}
