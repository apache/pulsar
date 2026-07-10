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

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker-impl")
public class ProducerSyncRetryTest extends ProducerConsumerBase {

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
    public void testProducerSyncRetryAfterTimeout() throws Exception {
        final String topic = BrokerTestUtil.newUniqueName("persistent://my-property/my-ns/tp");
        @Cleanup
        ProducerImpl<byte[]> producer = (ProducerImpl<byte[]>) pulsarClient.newProducer()
                .topic(topic)
                .enableBatching(false)
                .sendTimeout(1, TimeUnit.MILLISECONDS) // force timeout
                .create();

        // To make sure first message is timed out
        this.stopBroker();

        // First message will get timed out, then be retried with same payload
        ByteBuffer payload = ByteBuffer.wrap(new byte[0]);
        MessageMetadata messageMetadata = new MessageMetadata();
        messageMetadata.setUncompressedSize(1);
        MessageImpl<byte[]> message = MessageImpl.create(messageMetadata, payload, Schema.BYTES, topic);

        MessageMetadata retryMessageMetadata = new MessageMetadata();
        retryMessageMetadata.setUncompressedSize(1);
        MessageImpl<byte[]> retryMessage = MessageImpl.create(retryMessageMetadata, payload, Schema.BYTES, topic);

        // First send is expected to fail
        CompletableFuture<MessageId> firstSend = producer.sendAsync(message);
        producer.triggerSendTimer();

        // Waits until firstSend returns timeout exception
        CompletableFuture<MessageId> retrySend =
                firstSend.handle((msgId, ex) -> {
                    assertNotNull(ex, "First send must timeout");
                    assertTrue(ex instanceof PulsarClientException.TimeoutException);
                    try {
                        // Retry should succeed
                        this.startBroker();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    producer.conf.setSendTimeoutMs(10000);
                    return producer.sendAsync(retryMessage);
                }).thenCompose(f -> f);

        // Wait until retry completes successfully
        MessageId retryMessageId = retrySend.join();
        assertNotNull(retryMessageId);
    }
}
