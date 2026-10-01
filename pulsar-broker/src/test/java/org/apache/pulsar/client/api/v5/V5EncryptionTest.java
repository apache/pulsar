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
package org.apache.pulsar.client.api.v5;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import lombok.Cleanup;
import org.apache.pulsar.client.api.v5.auth.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.v5.auth.PemFileKeyProvider;
import org.apache.pulsar.client.api.v5.config.BatchingPolicy;
import org.apache.pulsar.client.api.v5.config.ConsumerEncryptionPolicy;
import org.apache.pulsar.client.api.v5.config.ProducerEncryptionPolicy;
import org.apache.pulsar.client.api.v5.config.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.testng.annotations.Test;

/**
 * End-to-end coverage for V5 message encryption: produce → broker → consume
 * round-trip, with payloads encrypted on the producer side and decrypted on the
 * consumer side. Reuses the test PEM keys under {@code certificate/} that the v4
 * tests already use.
 *
 * <p>Wiring under test:
 * <ul>
 *   <li>{@link PemFileKeyProvider} loads PEM bytes from disk on each side.</li>
 *   <li>{@link ProducerEncryptionPolicy} / {@link ConsumerEncryptionPolicy} carry
 *       the providers + key names + failure actions through the V5 builders.</li>
 *   <li>{@link org.apache.pulsar.client.impl.v5.CryptoKeyReaderAdapter} bridges
 *       to the v4 {@code ConsumerImpl} / {@code ProducerImpl} crypto paths.</li>
 * </ul>
 */
public class V5EncryptionTest extends V5ClientBaseTest {

    private static final String KEY_NAME = "client-rsa";
    private static final Path PUB_KEY =
            Path.of("./src/test/resources/certificate/public-key.client-rsa.pem");
    private static final Path PRIV_KEY =
            Path.of("./src/test/resources/certificate/private-key.client-rsa.pem");

    private static PemFileKeyProvider producerKeys() {
        return PemFileKeyProvider.builder()
                .publicKey(KEY_NAME, PUB_KEY)
                .build();
    }

    private static PemFileKeyProvider consumerKeys() {
        return PemFileKeyProvider.builder()
                .privateKey(KEY_NAME, PRIV_KEY)
                .build();
    }

    private static ProducerEncryptionPolicy producerPolicy() {
        return ProducerEncryptionPolicy.builder()
                .publicKeyProvider(producerKeys())
                .keyName(KEY_NAME)
                .build();
    }

    private static ConsumerEncryptionPolicy consumerPolicy() {
        return ConsumerEncryptionPolicy.builder()
                .privateKeyProvider(consumerKeys())
                .failureAction(ConsumerCryptoFailureAction.FAIL)
                .build();
    }

    /** Single-segment round trip: producer encrypts, consumer decrypts, payload matches. */
    @Test
    public void testProducerConsumerRoundTrip() throws Exception {
        String topic = newScalableTopic(1);

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .encryptionPolicy(producerPolicy())
                .create();
        @Cleanup
        QueueConsumer<String> consumer = v5Client.newQueueConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("crypto-sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .encryptionPolicy(consumerPolicy())
                .subscribe();

        producer.newMessage().value("hello-encrypted").send();

        Message<String> msg = consumer.receive(Duration.ofSeconds(5));
        assertNotNull(msg, "consumer must receive the encrypted-then-decrypted message");
        assertEquals(msg.value(), "hello-encrypted");
        consumer.acknowledge(msg.id());
    }

    /**
     * Multi-segment scalable topic: messages spread across segments by key, each
     * segment's per-segment v4 producer/consumer carries the same crypto config,
     * so every message decrypts correctly regardless of which segment it landed on.
     */
    @Test
    public void testEncryptionAcrossMultipleSegments() throws Exception {
        String topic = newScalableTopic(3);

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .encryptionPolicy(producerPolicy())
                .create();
        @Cleanup
        QueueConsumer<String> consumer = v5Client.newQueueConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("crypto-multi-sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .encryptionPolicy(consumerPolicy())
                .subscribe();

        int n = 30;
        Set<String> sent = new HashSet<>();
        for (int i = 0; i < n; i++) {
            String value = "msg-" + i;
            producer.newMessage().key("k-" + i).value(value).send();
            sent.add(value);
        }

        Set<String> received = new HashSet<>();
        for (int i = 0; i < n; i++) {
            Message<String> msg = consumer.receive(Duration.ofSeconds(5));
            assertNotNull(msg, "expected message #" + (i + 1));
            received.add(msg.value());
            consumer.acknowledge(msg.id());
        }
        assertEquals(received, sent, "every encrypted message must decrypt to its original value");
    }

    /**
     * Consumer with {@link ConsumerCryptoFailureAction#CONSUME} and no
     * {@link org.apache.pulsar.client.api.v5.auth.PrivateKeyProvider} configured
     * sees the still-encrypted payload, demonstrating the "I don't decrypt; just
     * give me the bytes" mode.
     *
     * <p>Batching disabled on the producer: v4 drops batched encrypted messages
     * even under CONSUME because it can't reframe a batch envelope it can't open.
     */
    @Test
    public void testConsumerWithoutProviderAndConsumeAction() throws Exception {
        String topic = newScalableTopic(1);

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .batchingPolicy(BatchingPolicy.ofDisabled())
                .encryptionPolicy(producerPolicy())
                .create();

        @Cleanup
        QueueConsumer<byte[]> consumer = v5Client.newQueueConsumer(Schema.bytes())
                .topic(topic)
                .subscriptionName("crypto-consume-sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .encryptionPolicy(ConsumerEncryptionPolicy.builder()
                        .failureAction(ConsumerCryptoFailureAction.CONSUME)
                        .build())
                .subscribe();

        producer.newMessage().value("plaintext-marker").send();

        Message<byte[]> msg = consumer.receive(Duration.ofSeconds(5));
        assertNotNull(msg, "CONSUME must deliver the message even without a private key");
        // Payload is still encrypted — must not contain the plaintext marker.
        String body = new String(msg.value());
        assertTrue(!body.contains("plaintext-marker"),
                "payload should still be encrypted, got: " + body);
        consumer.acknowledge(msg.id());
    }

    /**
     * Consumer with {@link ConsumerCryptoFailureAction#DISCARD} and no provider
     * silently drops undecryptable messages (cursor advances) — the application
     * never sees them.
     */
    @Test
    public void testConsumerWithoutProviderAndDiscardAction() throws Exception {
        String topic = newScalableTopic(1);

        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string())
                .topic(topic)
                .batchingPolicy(BatchingPolicy.ofDisabled())
                .encryptionPolicy(producerPolicy())
                .create();

        @Cleanup
        QueueConsumer<String> consumer = v5Client.newQueueConsumer(Schema.string())
                .topic(topic)
                .subscriptionName("crypto-discard-sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .encryptionPolicy(ConsumerEncryptionPolicy.builder()
                        .failureAction(ConsumerCryptoFailureAction.DISCARD)
                        .build())
                .subscribe();

        producer.newMessage().value("classified").send();

        Message<String> msg = consumer.receive(Duration.ofMillis(500));
        assertNull(msg, "DISCARD must drop the undecryptable message before delivery");
    }
}
