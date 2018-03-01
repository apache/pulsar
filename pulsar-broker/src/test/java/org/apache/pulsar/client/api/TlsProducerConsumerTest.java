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

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

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
        log.info("-- message size --", MESSAGE_SIZE);

        internalSetUpForClient(true, "pulsar+ssl://localhost:" + BROKER_PORT_TLS);
        internalSetUpForNamespace();

        ConsumerConfiguration conf = new ConsumerConfiguration();
        conf.setSubscriptionType(SubscriptionType.Exclusive);
        Consumer consumer = pulsarClient.subscribe("persistent://my-property/use/my-ns/my-topic1", "my-subscriber-name",
                conf);

        ProducerConfiguration producerConf = new ProducerConfiguration();

        Producer producer = pulsarClient.createProducer("persistent://my-property/use/my-ns/my-topic1", producerConf);
        for (int i = 0; i < 10; i++) {
            byte[] message = new byte[MESSAGE_SIZE];
            Arrays.fill(message, (byte) i);
            producer.send(message);
        }

        Message msg = null;
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

    /**
     * Test verifies that the broker throws an error when client tries to connect without a valid certificate.
     *
     * @throws Exception
     */
    @Test(timeOut = 30000)
    public void testTlsClientAuth() throws Exception {
        log.info("-- Starting {} test --", methodName);

        final int MESSAGE_SIZE = 16 * 1024 + 1;
        log.info("-- message size --", MESSAGE_SIZE);
        internalSetUpForNamespace();
//
//        // Test 1 - Using TLS on binary protocol without sending certs - expect failure
//        internalSetUpForClient(false, "pulsar+ssl://localhost:" + BROKER_PORT_TLS);
//        try {
//            ConsumerConfiguration conf = new ConsumerConfiguration();
//            conf.setSubscriptionType(SubscriptionType.Exclusive);
//            Consumer consumer = pulsarClient.subscribe("persistent://my-property/use/my-ns/my-topic1",
//                    "my-subscriber-name", conf);
//            Assert.fail("Server should have failed the TLS handshake since client didn't .");
//        } catch (Exception ex) {
//            // OK
//        }
//
//        // Test 2 - Using TLS on binary protocol - sending certs
//        internalSetUpForClient(true, "pulsar+ssl://localhost:" + BROKER_PORT_TLS);
//        try {
//            ConsumerConfiguration conf = new ConsumerConfiguration();
//            conf.setSubscriptionType(SubscriptionType.Exclusive);
//            Consumer consumer = pulsarClient.subscribe("persistent://my-property/use/my-ns/my-topic1",
//                    "my-subscriber-name", conf);
//        } catch (Exception ex) {
//            Assert.fail("Should not fail since certs are sent.");
//        }
//
//        // Test 3 - Using TLS on https  without sending certs - expect failure
//        internalSetUpForClient(false, "https://localhost:" + BROKER_WEBSERVICE_PORT_TLS);
//        try {
//            ConsumerConfiguration conf = new ConsumerConfiguration();
//            conf.setSubscriptionType(SubscriptionType.Exclusive);
//            Consumer consumer = pulsarClient.subscribe("persistent://my-property/use/my-ns/my-topic1",
//                    "my-subscriber-name", conf);
//            Assert.fail("Server should have failed the TLS handshake since client didn't .");
//        } catch (Exception ex) {
//            // OK
//        }

        // Test 4 - Using TLS on https - sending certs
        internalSetUpForClient(false, "https://localhost:" + BROKER_WEBSERVICE_PORT_TLS);
        try {
            ConsumerConfiguration conf = new ConsumerConfiguration();
            conf.setSubscriptionType(SubscriptionType.Exclusive);
            Consumer consumer = pulsarClient.subscribe("persistent://my-property/use/my-ns/my-topic1",
                    "my-subscriber-name", conf);
        } catch (Exception ex) {
            Assert.fail("Should not fail since certs are sent.");
        }
    }
}