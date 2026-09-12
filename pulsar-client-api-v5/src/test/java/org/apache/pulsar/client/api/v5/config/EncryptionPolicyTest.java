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
package org.apache.pulsar.client.api.v5.config;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.expectThrows;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.v5.auth.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.v5.auth.EncryptionKey;
import org.apache.pulsar.client.api.v5.auth.PrivateKeyProvider;
import org.apache.pulsar.client.api.v5.auth.ProducerCryptoFailureAction;
import org.apache.pulsar.client.api.v5.auth.PublicKeyProvider;
import org.testng.annotations.Test;

/**
 * Builder validation for {@link ProducerEncryptionPolicy} and
 * {@link ConsumerEncryptionPolicy}.
 */
public class EncryptionPolicyTest {

    private static final PublicKeyProvider STUB_PUB =
            keyName -> CompletableFuture.completedFuture(EncryptionKey.of(new byte[0]));
    private static final PrivateKeyProvider STUB_PRIV =
            (keyName, metadata) -> CompletableFuture.completedFuture(EncryptionKey.of(new byte[0]));

    // --- Producer side ---

    @Test
    public void testProducerPolicyMissingProviderRejected() {
        expectThrows(NullPointerException.class, () ->
                ProducerEncryptionPolicy.builder()
                        .keyName("k1")
                        .build());
    }

    @Test
    public void testProducerPolicyMissingKeyNameRejected() {
        expectThrows(IllegalArgumentException.class, () ->
                ProducerEncryptionPolicy.builder()
                        .publicKeyProvider(STUB_PUB)
                        .build());
    }

    @Test
    public void testProducerPolicyDefaultsFailureActionToFail() {
        ProducerEncryptionPolicy p = ProducerEncryptionPolicy.builder()
                .publicKeyProvider(STUB_PUB)
                .keyName("k1")
                .build();
        assertSame(p.failureAction(), ProducerCryptoFailureAction.FAIL);
        assertSame(p.publicKeyProvider(), STUB_PUB);
        assertEquals(p.keyNames(), List.of("k1"));
    }

    @Test
    public void testProducerPolicyMultipleKeyNames() {
        ProducerEncryptionPolicy p = ProducerEncryptionPolicy.builder()
                .publicKeyProvider(STUB_PUB)
                .keyNames("k1", "k2", "k3")
                .failureAction(ProducerCryptoFailureAction.SEND_UNENCRYPTED)
                .build();
        assertEquals(p.keyNames(), List.of("k1", "k2", "k3"));
        assertSame(p.failureAction(), ProducerCryptoFailureAction.SEND_UNENCRYPTED);
    }

    // --- Consumer side ---

    @Test
    public void testConsumerPolicyFailModeRequiresProvider() {
        // FAIL is the default failure action and requires a provider.
        expectThrows(IllegalArgumentException.class, () ->
                ConsumerEncryptionPolicy.builder().build());
    }

    @Test
    public void testConsumerPolicyDiscardModeWithoutProviderAllowed() {
        // DISCARD / CONSUME accept a null provider (consumer doesn't decrypt).
        ConsumerEncryptionPolicy p = ConsumerEncryptionPolicy.builder()
                .failureAction(ConsumerCryptoFailureAction.DISCARD)
                .build();
        assertSame(p.failureAction(), ConsumerCryptoFailureAction.DISCARD);
        org.testng.Assert.assertNull(p.privateKeyProvider());
    }

    @Test
    public void testConsumerPolicyDefaultsFailureActionToFail() {
        ConsumerEncryptionPolicy p = ConsumerEncryptionPolicy.builder()
                .privateKeyProvider(STUB_PRIV)
                .build();
        assertSame(p.failureAction(), ConsumerCryptoFailureAction.FAIL);
        assertSame(p.privateKeyProvider(), STUB_PRIV);
    }

    @Test
    public void testConsumerPolicyExplicitFailureAction() {
        ConsumerEncryptionPolicy p = ConsumerEncryptionPolicy.builder()
                .privateKeyProvider(STUB_PRIV)
                .failureAction(ConsumerCryptoFailureAction.DISCARD)
                .build();
        assertSame(p.failureAction(), ConsumerCryptoFailureAction.DISCARD);
    }

    // --- EncryptionKey factories ---

    @Test
    public void testEncryptionKeyFactories() {
        byte[] bytes = "key-material".getBytes();
        EncryptionKey k1 = EncryptionKey.of(bytes);
        assertSame(k1.key(), bytes);
        assertEquals(k1.metadata(), Map.of());

        EncryptionKey k2 = EncryptionKey.of(bytes, Map.of("version", "v1"));
        assertEquals(k2.metadata(), Map.of("version", "v1"));
    }
}
