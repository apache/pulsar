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
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.util.FutureUtil;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        } finally {
            producer.getClientCnx().channel().config().setAutoRead(true);
        }
        FutureUtil.waitForAll(futures).get();
        Assert.assertEquals(producer.getSemaphore().get().availablePermits(), pendingQueueSize);
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
        } finally {
            producer.getClientCnx().channel().config().setAutoRead(true);
        }
        FutureUtil.waitForAll(futures).get();
        Assert.assertEquals(producer.getSemaphore().get().availablePermits(), pendingQueueSize);
        futures.clear();

        // Here must ensure that the semaphore available permits is 0
        Assert.assertEquals(producer.getSemaphore().get().availablePermits(), pendingQueueSize);

        // Acquire 5 and not wait the send ack call back
        producer.getClientCnx().channel().config().setAutoRead(false);
        try {
            for (int i = 0; i < messages / 2; i++) {
                futures.add(producer.newMessage().value(("Semaphore-test-" + i).getBytes()).sendAsync());
            }

            // Here must ensure that the Semaphore a acquired 5
            Assert.assertEquals(producer.getSemaphore().get().availablePermits(), pendingQueueSize - messages / 2);
        } finally {
            producer.getClientCnx().channel().config().setAutoRead(true);

        }
        FutureUtil.waitForAll(futures).get();
        Assert.assertEquals(producer.getSemaphore().get().availablePermits(), pendingQueueSize);
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
        producer.getClientCnx().channel().config().setAutoRead(false);
        try {
            for (int i = 0; i < pendingQueueSize; i++) {
                MessageMetadata metadata = new MessageMetadata()
                        .setNumMessagesInBatch(10);

                MessageImpl<byte[]> msg = MessageImpl.create(metadata, ByteBuffer.wrap(new byte[0]), Schema.BYTES, null);
                futures.add(producer.sendAsync(msg));
            }
            Assert.assertEquals(producer.getSemaphore().get().availablePermits(), 0);
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
            }
        } finally {
            producer.getClientCnx().channel().config().setAutoRead(true);
        }
        FutureUtil.waitForAll(futures).get();
        futures.clear();

        // Test that when we fill the queue with normal messages, we get an error
        Assert.assertEquals(producer.getSemaphore().get().availablePermits(), pendingQueueSize);
        producer.getClientCnx().channel().config().setAutoRead(false);
        try {
            for (int i = 0; i < pendingQueueSize; i++) {
                futures.add(producer.newMessage().value(("Semaphore-test-" + i).getBytes()).sendAsync());
            }
            Assert.assertEquals(producer.getSemaphore().get().availablePermits(), 0);

            try {
                producer.newMessage().value(("Semaphore-test-Q-full").getBytes()).sendAsync().get();
            } catch (ExecutionException ee) {
                Assert.assertEquals(ee.getCause().getClass(),
                                    PulsarClientException.ProducerQueueIsFullError.class);
                Assert.assertEquals(producer.getSemaphore().get().availablePermits(), 0);

            }
        } finally {
            producer.getClientCnx().channel().config().setAutoRead(true);
        }
        FutureUtil.waitForAll(futures).get();
        Assert.assertEquals(producer.getSemaphore().get().availablePermits(), pendingQueueSize);
    }
}
