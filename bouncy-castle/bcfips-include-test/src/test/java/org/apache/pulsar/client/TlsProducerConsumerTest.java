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
package org.apache.pulsar.client;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.SubscriptionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TlsProducerConsumerTest extends TlsProducerConsumerBase {
    private static final Logger log = LoggerFactory.getLogger(TlsProducerConsumerTest.class);

    /**
     * verifies that messages whose size is larger than 2^14 bytes (max size of single TLS chunk) can be
     * produced/consumed
     *
     * @throws Exception
     */
    @Test(timeOut = 30000)
    public void testTlsLargeSizeMessage() throws Exception {
        log.info("-- Starting {} test --", methodName);

        final int MESSAGE_SIZE = 16 * 1024 + 1;
        log.info("-- message size {} --", MESSAGE_SIZE);

        internalSetUpForClient(true, pulsar.getBrokerServiceUrlTls());
        internalSetUpForNamespace();

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic("persistent://my-property/use/my-ns/my-topic1")
                .subscriptionName("my-subscriber-name").subscribe();

        Producer<byte[]> producer = pulsarClient.newProducer().topic("persistent://my-property/use/my-ns/my-topic1")
                .create();
        for (int i = 0; i < 10; i++) {
            byte[] message = new byte[MESSAGE_SIZE];
            Arrays.fill(message, (byte) i);
            producer.send(message);
        }

        Message<byte[]> msg = null;
        for (int i = 0; i < 10; i++) {
            msg = consumer.receive(5, TimeUnit.SECONDS);
            byte[] expected = new byte[MESSAGE_SIZE];
            Arrays.fill(expected, (byte) i);
            Assert.assertEquals(expected, msg.getData());
        }
        // Acknowledge the consumption of all messages at once
        consumer.acknowledgeCumulative(msg);
        consumer.close();
        log.info("-- Exiting {} test --", methodName);
    }

    @Test(timeOut = 30000)
    public void testTlsClientAuthOverBinaryProtocol() throws Exception {
        log.info("-- Starting {} test --", methodName);

        final int MESSAGE_SIZE = 16 * 1024 + 1;
        log.info("-- message size {} --", MESSAGE_SIZE);
        internalSetUpForNamespace();

        // Test 1 - Using TLS on binary protocol without sending certs - expect failure
        internalSetUpForClient(false, pulsar.getBrokerServiceUrlTls());
        try {
            pulsarClient.newConsumer().topic("persistent://my-property/use/my-ns/my-topic1")
                    .subscriptionName("my-subscriber-name").subscriptionType(SubscriptionType.Exclusive).subscribe();
            Assert.fail("Server should have failed the TLS handshake since client didn't .");
        } catch (Exception ex) {
            // OK
            log.info("first test success: without certs set, meet exception ", ex);
        }

        // Test 2 - Using TLS on binary protocol - sending certs
        internalSetUpForClient(true, pulsar.getBrokerServiceUrlTls());
        try {
            pulsarClient.newConsumer().topic("persistent://my-property/use/my-ns/my-topic1")
                    .subscriptionName("my-subscriber-name").subscriptionType(SubscriptionType.Exclusive).subscribe();
            log.info("second test success: with certs set, consumer sub success");
        } catch (Exception ex) {
            Assert.fail("Should not fail since certs are sent.");
        }
    }

    @Test(timeOut = 30000)
    public void testTlsClientAuthOverHTTPProtocol() throws Exception {
        log.info("-- Starting {} test --", methodName);

        final int MESSAGE_SIZE = 16 * 1024 + 1;
        log.info("-- message size {} --", MESSAGE_SIZE);
        internalSetUpForNamespace();

        // Test 1 - Using TLS on https without sending certs - expect failure
        internalSetUpForClient(false, pulsar.getWebServiceAddressTls());
        try {
            pulsarClient.newConsumer().topic("persistent://my-property/use/my-ns/my-topic1")
                    .subscriptionName("my-subscriber-name").subscriptionType(SubscriptionType.Exclusive).subscribe();
            Assert.fail("Server should have failed the TLS handshake since client didn't .");
        } catch (Exception ex) {
            // OK
            log.info("first test success: without certs set, meet exception ", ex);
        }

        // Test 2 - Using TLS on https - sending certs
        internalSetUpForClient(true, pulsar.getWebServiceAddressTls());
        try {
            pulsarClient.newConsumer().topic("persistent://my-property/use/my-ns/my-topic1")
                    .subscriptionName("my-subscriber-name").subscriptionType(SubscriptionType.Exclusive).subscribe();
            log.info("second test success: with certs set, consumer sub success");
        } catch (Exception ex) {
            Assert.fail("Should not fail since certs are sent.");
        }
    }
}
