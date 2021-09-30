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
package org.apache.pulsar.broker.service;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.common.util.FutureUtil;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class MessagePublishBufferThrottleTest extends BrokerTestBase {

    @Override
    protected void setup() throws Exception {
        //No-op
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
        resetConfig();
    }

    @Test
    public void testMessagePublishBufferThrottleDisabled() throws Exception {
        conf.setMaxMessagePublishBufferSizeInMB(-1);
        super.baseSetup();
        final String topic = "persistent://prop/ns-abc/testMessagePublishBufferThrottleDisabled";
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic)
                .producerName("producer-name")
                .create();
         assertEquals(pulsar.getBrokerService().getPausedConnections(), 0);

         mockBookKeeper.addEntryDelay(1, TimeUnit.SECONDS);

        // Make sure the producer can publish successfully
        byte[] payload = new byte[1024 * 1024];
        for (int i = 0; i < 10; i++) {
            producer.sendAsync(payload);
        }
        producer.flush();

        assertEquals(pulsar.getBrokerService().getPausedConnections(), 0);
    }

    @Test
    public void testMessagePublishBufferThrottleEnable() throws Exception {
        conf.setMaxMessagePublishBufferSizeInMB(1);
        super.baseSetup();

        assertEquals(pulsar.getBrokerService().getPausedConnections(), 0);
        final String topic = "persistent://prop/ns-abc/testMessagePublishBufferThrottleEnable";
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic)
                .producerName("producer-name")
                .create();

        assertEquals(pulsar.getBrokerService().getPausedConnections(), 0);

        mockBookKeeper.addEntryDelay(1, TimeUnit.SECONDS);

        byte[] payload = new byte[1024 * 1024];
        for (int i = 0; i < 10; i++) {
            producer.sendAsync(payload);
        }

        Awaitility.await().untilAsserted(
                () -> Assert.assertEquals(pulsar.getBrokerService().getPausedConnections(), 1L));
        assertEquals(pulsar.getBrokerService().getPausedConnections(), 1);

        producer.flush();

        Awaitility.await().untilAsserted(
            () -> Assert.assertEquals(pulsar.getBrokerService().getPausedConnections(), 0L));

        assertEquals(pulsar.getBrokerService().getPausedConnections(), 0);
    }

    @Test
    public void testBlockByPublishRateLimiting() throws Exception {
        conf.setMaxMessagePublishBufferSizeInMB(1);
        super.baseSetup();
        assertEquals(pulsar.getBrokerService().getPausedConnections(), 0);
        final String topic = "persistent://prop/ns-abc/testBlockByPublishRateLimiting";
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic)
                .producerName("producer-name")
                .create();
        Topic topicRef = pulsar.getBrokerService().getTopicReference(topic).get();
        Assert.assertNotNull(topicRef);
        assertEquals(pulsar.getBrokerService().getPausedConnections(), 0);

        mockBookKeeper.addEntryDelay(5, TimeUnit.SECONDS);

        // Block by publish buffer.
        byte[] payload = new byte[1024 * 1024];
        for (int i = 0; i < 10; i++) {
            producer.sendAsync(payload);
        }

        Awaitility.await().untilAsserted(() -> assertEquals(pulsar.getBrokerService().getPausedConnections(), 1));

        CompletableFuture<Void> flushFuture = producer.flushAsync();

        // Block by publish rate.
        // After 1 second, the message buffer throttling will be lifted, but the rate limiting will still be in place.
        assertEquals(pulsar.getBrokerService().getPausedConnections(), 1);
        try {
            flushFuture.get(2, TimeUnit.SECONDS);
            fail("Should have timed out");
        } catch (TimeoutException e) {
            // Ok
        }

        flushFuture.join();

        Awaitility.await().untilAsserted(() ->
                assertEquals(pulsar.getBrokerService().getPausedConnections(), 0));

        // Resume message publish.
        ((AbstractTopic)topicRef).producers.get("producer-name").getCnx().enableCnxAutoRead();

        flushFuture.get();
        Awaitility.await().untilAsserted(() ->
                assertEquals(pulsar.getBrokerService().getPausedConnections(), 0));
    }
}
