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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.EncryptionKeyInfo;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClientException;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "broker-impl")
public class ProducerEncryptionKeyFailureTest extends ProducerConsumerBase {

    @BeforeClass(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    /** A key reader that cannot find any key, as a misconfigured deployment would. */
    private static final class MissingKeyReader implements CryptoKeyReader {
        @Override
        public EncryptionKeyInfo getPublicKey(String keyName, Map<String, String> metadata) {
            return null;
        }

        @Override
        public EncryptionKeyInfo getPrivateKey(String keyName, Map<String, String> metadata) {
            return null;
        }
    }

    /** A key reader that takes a while to answer, as one backed by a remote key management service does. */
    private static final class SlowKeyReader implements CryptoKeyReader {
        private final CryptoKeyReader delegate;
        private final long delayMs;

        SlowKeyReader(CryptoKeyReader delegate, long delayMs) {
            this.delegate = delegate;
            this.delayMs = delayMs;
        }

        @Override
        public EncryptionKeyInfo getPublicKey(String keyName, Map<String, String> metadata) {
            try {
                Thread.sleep(delayMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return delegate.getPublicKey(keyName, metadata);
        }

        @Override
        public EncryptionKeyInfo getPrivateKey(String keyName, Map<String, String> metadata) {
            return delegate.getPrivateKey(keyName, metadata);
        }
    }

    /** Reads the RSA test key pair from the test resources. */
    private static final class FileKeyReader implements CryptoKeyReader {
        @Override
        public EncryptionKeyInfo getPublicKey(String keyName, Map<String, String> metadata) {
            return read("./src/test/resources/certificate/public-key.client-rsa.pem");
        }

        @Override
        public EncryptionKeyInfo getPrivateKey(String keyName, Map<String, String> metadata) {
            return read("./src/test/resources/certificate/private-key.client-rsa.pem");
        }

        private static EncryptionKeyInfo read(String path) {
            try {
                EncryptionKeyInfo keyInfo = new EncryptionKeyInfo();
                keyInfo.setKey(Files.readAllBytes(Paths.get(path)));
                return keyInfo;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    @Test(timeOut = 60_000)
    public void testProducerWhoseKeyCannotBeLoadedIsNotLeftBehind() throws Exception {
        String topic = "persistent://my-property/my-ns/crypto-key-failure-" + System.nanoTime();
        // Created up front so that the broker can report its publishers whether or not a producer connects.
        admin.topics().createNonPartitionedTopic(topic);
        PulsarClientImpl client = (PulsarClientImpl) pulsarClient;

        expectThrows(PulsarClientException.CryptoException.class, () -> pulsarClient.newProducer()
                .topic(topic)
                .addEncryptionKey("missing-key")
                .cryptoKeyReader(new MissingKeyReader())
                .create());

        // The connection is opened by another thread: give it time to register the producer, which it must not.
        Awaitility.await().during(3, TimeUnit.SECONDS).atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                assertTrue(admin.topics().getStats(topic).getPublishers().isEmpty(),
                        "A producer whose creation failed must not stay registered on the broker"));
        assertEquals(client.producersCount(), 0, "A producer whose creation failed must not stay in the client");
    }

    /**
     * The key is loaded by a task that runs apart from the connection. When the key reader is slower than the
     * connection, the producer must still not be created without its key: the creation waits for the first load.
     */
    @Test(timeOut = 60_000)
    public void testProducerWhoseKeyLoadsSlowlyAndCannotBeLoadedIsNotCreated() throws Exception {
        String topic = "persistent://my-property/my-ns/crypto-slow-key-failure-" + System.nanoTime();
        admin.topics().createNonPartitionedTopic(topic);
        PulsarClientImpl client = (PulsarClientImpl) pulsarClient;

        expectThrows(PulsarClientException.CryptoException.class, () -> pulsarClient.newProducer()
                .topic(topic)
                .addEncryptionKey("missing-key")
                .cryptoKeyReader(new SlowKeyReader(new MissingKeyReader(), 1_500))
                .create());

        Awaitility.await().during(3, TimeUnit.SECONDS).atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                assertTrue(admin.topics().getStats(topic).getPublishers().isEmpty(),
                        "A producer whose creation failed must not stay registered on the broker"));
        assertEquals(client.producersCount(), 0, "A producer whose creation failed must not stay in the client");
    }

    /** A slow key reader that does find the key only delays the creation; the messages are encrypted. */
    @Test(timeOut = 60_000)
    public void testProducerWhoseKeyLoadsSlowlyIsCreatedOnceTheKeyIsLoaded() throws Exception {
        String topic = "persistent://my-property/my-ns/crypto-slow-key-" + System.nanoTime();
        FileKeyReader keys = new FileKeyReader();
        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topic)
                .subscriptionName("sub")
                .cryptoKeyReader(keys)
                .subscribe();

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic)
                .addEncryptionKey("client-rsa.pem")
                .cryptoKeyReader(new SlowKeyReader(keys, 1_500))
                .create();
        producer.send("secret".getBytes(StandardCharsets.UTF_8));

        Message<byte[]> message = consumer.receive(10, TimeUnit.SECONDS);
        assertNotNull(message);
        assertEquals(new String(message.getValue(), StandardCharsets.UTF_8), "secret");
        assertTrue(message.getEncryptionCtx().isPresent(), "The message must have been encrypted");
    }
}
