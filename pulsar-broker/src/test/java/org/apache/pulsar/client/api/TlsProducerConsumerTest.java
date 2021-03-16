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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.impl.auth.AuthenticationTls;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import lombok.Cleanup;

@Test(groups = "broker-api")
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
        log.info("-- message size -- {}", MESSAGE_SIZE);

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
        log.info("-- message size -- {}", MESSAGE_SIZE);
        internalSetUpForNamespace();

        // Test 1 - Using TLS on binary protocol without sending certs - expect failure
        internalSetUpForClient(false, pulsar.getBrokerServiceUrlTls());
        try {
            pulsarClient.newConsumer().topic("persistent://my-property/use/my-ns/my-topic1")
                    .subscriptionName("my-subscriber-name").subscriptionType(SubscriptionType.Exclusive).subscribe();
            Assert.fail("Server should have failed the TLS handshake since client didn't .");
        } catch (Exception ex) {
            // OK
        }

        // Test 2 - Using TLS on binary protocol - sending certs
        internalSetUpForClient(true, pulsar.getBrokerServiceUrlTls());
        try {
            pulsarClient.newConsumer().topic("persistent://my-property/use/my-ns/my-topic1")
                    .subscriptionName("my-subscriber-name").subscriptionType(SubscriptionType.Exclusive).subscribe();
        } catch (Exception ex) {
            Assert.fail("Should not fail since certs are sent.");
        }
    }

    @Test(timeOut = 30000)
    public void testTlsClientAuthOverHTTPProtocol() throws Exception {
        log.info("-- Starting {} test --", methodName);

        final int MESSAGE_SIZE = 16 * 1024 + 1;
        log.info("-- message size -- {}", MESSAGE_SIZE);
        internalSetUpForNamespace();

        // Test 1 - Using TLS on https without sending certs - expect failure
        internalSetUpForClient(false, pulsar.getWebServiceAddressTls());
        try {
            pulsarClient.newConsumer().topic("persistent://my-property/use/my-ns/my-topic1")
                    .subscriptionName("my-subscriber-name").subscriptionType(SubscriptionType.Exclusive).subscribe();
            Assert.fail("Server should have failed the TLS handshake since client didn't .");
        } catch (Exception ex) {
            // OK
        }

        // Test 2 - Using TLS on https - sending certs
        internalSetUpForClient(true, pulsar.getWebServiceAddressTls());
        try {
            pulsarClient.newConsumer().topic("persistent://my-property/use/my-ns/my-topic1")
                    .subscriptionName("my-subscriber-name").subscriptionType(SubscriptionType.Exclusive).subscribe();
        } catch (Exception ex) {
            Assert.fail("Should not fail since certs are sent.");
        }
    }

    @Test(timeOut = 60000)
    public void testTlsCertsFromDynamicStream() throws Exception {
        log.info("-- Starting {} test --", methodName);
        String topicName = "persistent://my-property/use/my-ns/my-topic1";
        ClientBuilder clientBuilder = PulsarClient.builder().serviceUrl(pulsar.getBrokerServiceUrlTls())
                .enableTls(true).allowTlsInsecureConnection(false)
                .operationTimeout(1000, TimeUnit.MILLISECONDS);
        AtomicInteger index = new AtomicInteger(0);

        ByteArrayInputStream certStream = createByteInputStream(TLS_CLIENT_CERT_FILE_PATH);
        ByteArrayInputStream keyStream = createByteInputStream(TLS_CLIENT_KEY_FILE_PATH);
        ByteArrayInputStream trustStoreStream = createByteInputStream(TLS_TRUST_CERT_FILE_PATH);

        Supplier<ByteArrayInputStream> certProvider = () -> getStream(index, certStream);
        Supplier<ByteArrayInputStream> keyProvider = () -> getStream(index, keyStream);
        Supplier<ByteArrayInputStream> trustStoreProvider = () -> getStream(index, trustStoreStream);
        AuthenticationTls auth = new AuthenticationTls(certProvider, keyProvider, trustStoreProvider);
        clientBuilder.authentication(auth);
        @Cleanup
        PulsarClient pulsarClient = clientBuilder.build();
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName("my-subscriber-name")
                .subscribe();

        // unload the topic so, new connection will be made and read the cert streams again
        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();
        topicRef.close(false);

        Producer<byte[]> producer = pulsarClient.newProducer().topic("persistent://my-property/use/my-ns/my-topic1")
                .createAsync().get(30, TimeUnit.SECONDS);
        for (int i = 0; i < 10; i++) {
            producer.send(("test" + i).getBytes());
        }

        Message<byte[]> msg = null;
        for (int i = 0; i < 10; i++) {
            msg = consumer.receive(5, TimeUnit.SECONDS);
            String exepctedMsg = "test" + i;
            Assert.assertEquals(exepctedMsg.getBytes(), msg.getData());
        }
        // Acknowledge the consumption of all messages at once
        consumer.acknowledgeCumulative(msg);
        consumer.close();
        log.info("-- Exiting {} test --", methodName);
    }

    /**
     * It verifies that AuthenticationTls provides cert refresh functionality.
     *
     * <pre>
     *  a. Create Auth with invalid cert
     *  b. Consumer fails with invalid tls certs
     *  c. refresh cert in provider
     *  d. Consumer successfully gets created
     * </pre>
     *
     * @throws Exception
     */
    @Test
    public void testTlsCertsFromDynamicStreamExpiredAndRenewCert() throws Exception {
        log.info("-- Starting {} test --", methodName);
        ClientBuilder clientBuilder = PulsarClient.builder().serviceUrl(pulsar.getBrokerServiceUrlTls())
                .enableTls(true).allowTlsInsecureConnection(false)
                .operationTimeout(1000, TimeUnit.MILLISECONDS);
        AtomicInteger certIndex = new AtomicInteger(1);
        AtomicInteger keyIndex = new AtomicInteger(0);
        AtomicInteger trustStoreIndex = new AtomicInteger(1);
        ByteArrayInputStream certStream = createByteInputStream(TLS_CLIENT_CERT_FILE_PATH);
        ByteArrayInputStream keyStream = createByteInputStream(TLS_CLIENT_KEY_FILE_PATH);
        ByteArrayInputStream trustStoreStream = createByteInputStream(TLS_TRUST_CERT_FILE_PATH);
        Supplier<ByteArrayInputStream> certProvider = () -> getStream(certIndex, certStream,
                keyStream/* invalid cert file */);
        Supplier<ByteArrayInputStream> keyProvider = () -> getStream(keyIndex, keyStream);
        Supplier<ByteArrayInputStream> trustStoreProvider = () -> getStream(trustStoreIndex, trustStoreStream,
                keyStream/* invalid cert file */);
        AuthenticationTls auth = new AuthenticationTls(certProvider, keyProvider, trustStoreProvider);
        clientBuilder.authentication(auth);
        @Cleanup
        PulsarClient pulsarClient = clientBuilder.build();
        Consumer<byte[]> consumer = null;
        try {
            consumer = pulsarClient.newConsumer().topic("persistent://my-property/use/my-ns/my-topic1")
                    .subscriptionName("my-subscriber-name").subscribe();
            Assert.fail("should have failed due to invalid tls cert");
        } catch (PulsarClientException e) {
            // Ok..
        }

        certIndex.set(0);
        try {
            consumer = pulsarClient.newConsumer().topic("persistent://my-property/use/my-ns/my-topic1")
                    .subscriptionName("my-subscriber-name").subscribe();
            Assert.fail("should have failed due to invalid tls cert");
        } catch (PulsarClientException e) {
            // Ok..
        }

        trustStoreIndex.set(0);
        consumer = pulsarClient.newConsumer().topic("persistent://my-property/use/my-ns/my-topic1")
                .subscriptionName("my-subscriber-name").subscribe();
        consumer.close();
        log.info("-- Exiting {} test --", methodName);
    }

    private ByteArrayInputStream createByteInputStream(String filePath) throws IOException {
        try (InputStream inStream = new FileInputStream(filePath)) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            IOUtils.copy(inStream, baos);
            return new ByteArrayInputStream(baos.toByteArray());
        }
    }

    private ByteArrayInputStream getStream(AtomicInteger index, ByteArrayInputStream... streams) {
        return streams[index.intValue()];
    }
}
