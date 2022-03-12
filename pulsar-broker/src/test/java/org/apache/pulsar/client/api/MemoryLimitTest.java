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
package org.apache.pulsar.client.api;

import lombok.Cleanup;
import org.apache.pulsar.client.api.PulsarClientException.MemoryBufferIsFullError;
import org.apache.pulsar.client.impl.ProducerImpl;
import org.apache.pulsar.client.impl.PulsarTestClient;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

@Test(groups = "broker-api")
public class MemoryLimitTest extends ProducerConsumerBase {

    @DataProvider(name = "batching")
    public Object[][] provider() {
        return new Object[][] {
                // "Batching"
                { false },
                { true },
        };
    }

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testRejectMessages()
            throws Exception {
        String topic = newTopicName();

        ClientBuilder clientBuilder = PulsarClient.builder()
                .serviceUrl(pulsar.getBrokerServiceUrl())
                .memoryLimit(100, SizeUnit.KILO_BYTES);

        @Cleanup
        PulsarTestClient client = PulsarTestClient.create(clientBuilder);

        @Cleanup
        ProducerImpl<byte[]> producer = (ProducerImpl<byte[]>) client.newProducer()
                .topic(topic)
                .blockIfQueueFull(false)
                .sendTimeout(5, TimeUnit.SECONDS)
                .create();

        // make sure all message pending at pendingMessages queue
        // connection with broker can not be established, so handleSendReceipt will not be invoked while sending message
        client.dropOpSendMessages();
        final int n = 101;
        for (int i = 0; i < n; i++) {
            producer.sendAsync(new byte[1024]);
        }
        Awaitility.await()
                .atMost(Duration.ofSeconds(5))
                .until(() -> producer.getPendingQueueSize() == n);
        assertEquals(client.getMemoryLimitController().currentUsage(), n * 1024);

        try {
            producer.send(new byte[1024]);
            fail("should have failed");
        } catch (MemoryBufferIsFullError e) {
            // Expected
        }

        client.allowReconnecting();
        Awaitility.await()
                .atMost(Duration.ofSeconds(30))
                .until(() -> producer.getPendingQueueSize() == 0);
        assertEquals(client.getMemoryLimitController().currentUsage(), 0);

        // We should now be able to send again
        producer.send(new byte[1024]);
    }

    @Test
    public void testRejectMessagesOnMultipleTopics() throws Exception {
        String t1 = newTopicName();
        String t2 = newTopicName();

        ClientBuilder clientBuilder = PulsarClient.builder()
                .serviceUrl(pulsar.getBrokerServiceUrl())
                .memoryLimit(100, SizeUnit.KILO_BYTES);

        @Cleanup
        PulsarTestClient client = PulsarTestClient.create(clientBuilder);

        @Cleanup
        ProducerImpl<byte[]> p1 = (ProducerImpl<byte[]>) client.newProducer()
                .topic(t1)
                .blockIfQueueFull(false)
                .sendTimeout(5, TimeUnit.SECONDS)
                .create();

        @Cleanup
        ProducerImpl<byte[]> p2 = (ProducerImpl<byte[]>) client.newProducer()
                .topic(t2)
                .blockIfQueueFull(false)
                .sendTimeout(5, TimeUnit.SECONDS)
                .create();

        client.dropOpSendMessages();
        final int n = 101;
        for (int i = 0; i < n / 2; i++) {
            p1.sendAsync(new byte[1024]);
            p2.sendAsync(new byte[1024]);
        }

        // Last message in order to reach the limit
        p1.sendAsync(new byte[1024]);

        Awaitility.await()
                .atMost(Duration.ofSeconds(5))
                .until(() -> (p1.getPendingQueueSize() + p2.getPendingQueueSize()) == n);
        assertEquals(client.getMemoryLimitController().currentUsage(), n * 1024);

        try {
            p1.send(new byte[1024]);
            fail("should have failed");
        } catch (MemoryBufferIsFullError e) {
            // Expected
        }

        try {
            p2.send(new byte[1024]);
            fail("should have failed");
        } catch (MemoryBufferIsFullError e) {
            // Expected
        }

        client.allowReconnecting();
        Awaitility.await()
                .atMost(Duration.ofSeconds(30))
                .until(() -> (p1.getPendingQueueSize() + p2.getPendingQueueSize()) == 0);
        assertEquals(client.getMemoryLimitController().currentUsage(), 0);

        // We should now be able to send again
        p1.send(new byte[1024]);
        p2.send(new byte[1024]);
    }
}
