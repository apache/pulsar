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

import static org.testng.Assert.fail;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker-impl")
public class PulsarMultiHostClientTest extends ProducerConsumerBase {

    private static final Logger log = LoggerFactory.getLogger(PulsarMultiHostClientTest.class);

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
    public void testGetPartitionedTopicMetaData() {
        log.info("-- Starting {} test --", methodName);

        final String topicName = "persistent://my-property/my-ns/my-topic1";
        final String subscriptionName = "my-subscriber-name";

        try {
            String url = pulsar.getWebServiceAddress();
            if (isTcpLookup) {
                url = pulsar.getBrokerServiceUrl();
            }
            @Cleanup
            PulsarClient client = newPulsarClient(url, 0);

            Consumer<byte[]> consumer = client.newConsumer().topic(topicName).subscriptionName(subscriptionName)
                .acknowledgmentGroupTime(0, TimeUnit.SECONDS).subscribe();
            Producer<byte[]> producer = client.newProducer().topic(topicName).create();

            consumer.close();
            producer.close();
        } catch (PulsarClientException pce) {
            log.error("create producer or consumer error: ", pce);
            fail();
        }

        log.info("-- Exiting {} test --", methodName);
    }

    @Test (timeOut = 15000)
    public void testGetPartitionedTopicDataTimeout() {
        log.info("-- Starting {} test --", methodName);

        final String topicName = "persistent://my-property/my-ns/my-topic1";

        String url = "http://localhost:" + getFreePort() + ",localhost:" + getFreePort();

        try {
            @Cleanup
            PulsarClient client = PulsarClient.builder()
                .serviceUrl(url)
                .statsInterval(0, TimeUnit.SECONDS)
                .operationTimeout(3, TimeUnit.SECONDS)
                .build();

            Producer<byte[]> producer = client.newProducer().topic(topicName).create();

            fail();
        } catch (PulsarClientException pce) {
            log.error("create producer error: ", pce);
        }

        log.info("-- Exiting {} test --", methodName);
    }

    private static int getFreePort() {
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            return serverSocket.getLocalPort();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Test
    public void testMultiHostUrlRetrySuccess() throws Exception {
        log.info("-- Starting {} test --", methodName);

        final String topicName = "persistent://my-property/my-ns/my-topic1";
        final String subscriptionName = "my-subscriber-name";

        // Multi hosts included an unreached port and the actual port for verify retry logic
        String urlsWithUnreached = "http://localhost:51000,localhost:" + new URI(pulsar.getWebServiceAddress()).getPort();
        if (isTcpLookup) {
            urlsWithUnreached = "pulsar://localhost:51000,localhost" + new URI(pulsar.getBrokerServiceUrl()).getPort();
        }
        @Cleanup
        PulsarClient client = newPulsarClient(urlsWithUnreached, 0);

        Consumer<byte[]> consumer = client.newConsumer().topic(topicName).subscriptionName(subscriptionName)
            .acknowledgmentGroupTime(0, TimeUnit.SECONDS).subscribe();
        Producer<byte[]> producer = client.newProducer().topic(topicName).create();

        for (int i = 0; i < 5; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
            log.info("Produced message: [{}]", message);
        }

        Message<byte[]> msg = null;
        Set<String> messageSet = new HashSet<>();
        for (int i = 0; i < 5; i++) {
            msg = consumer.receive(5, TimeUnit.SECONDS);
            String receivedMessage = new String(msg.getData());
            log.info("Received message: [{}]", receivedMessage);
            String expectedMessage = "my-message-" + i;
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
        }

        // Acknowledge the consumption of all messages at once
        consumer.acknowledgeCumulative(msg);
        consumer.close();

        producer.close();

        log.info("-- Exiting {} test --", methodName);
    }
}
