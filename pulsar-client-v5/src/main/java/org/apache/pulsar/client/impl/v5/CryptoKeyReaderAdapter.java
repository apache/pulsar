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
package org.apache.pulsar.client.impl.v5;

import java.util.Map;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.EncryptionKeyInfo;
import org.apache.pulsar.client.api.v5.auth.PrivateKeyProvider;
import org.apache.pulsar.client.api.v5.auth.PublicKeyProvider;

/**
 * Bridges the V5 split-by-role key SPIs ({@link PublicKeyProvider},
 * {@link PrivateKeyProvider}) to the v4 {@link CryptoKeyReader}, which has both
 * methods on a single interface.
 *
 * <p>v4's {@code CryptoKeyReader} is synchronous, so the adapter blocks on the V5
 * provider's {@link java.util.concurrent.CompletableFuture future} via {@code join()}.
 * For local providers (e.g. {@code PemFileKeyProvider}) the future is already
 * complete; for remote providers (e.g. KMS-backed) this blocks the v4 thread that
 * called {@code getXxxKey} — same constraint v4 already imposes today. Async
 * end-to-end requires deeper plumbing into {@code MessageCrypto}; out of scope here.
 */
final class CryptoKeyReaderAdapter implements CryptoKeyReader {

    private final PublicKeyProvider publicKeyProvider;
    private final PrivateKeyProvider privateKeyProvider;

    private CryptoKeyReaderAdapter(PublicKeyProvider publicKeyProvider,
                                   PrivateKeyProvider privateKeyProvider) {
        this.publicKeyProvider = publicKeyProvider;
        this.privateKeyProvider = privateKeyProvider;
    }

    /**
     * Producer-side adapter: only {@link CryptoKeyReader#getPublicKey} is supported.
     */
    static CryptoKeyReader forProducer(PublicKeyProvider provider) {
        return new CryptoKeyReaderAdapter(provider, null);
    }

    /**
     * Consumer-side adapter: only {@link CryptoKeyReader#getPrivateKey} is supported.
     */
    static CryptoKeyReader forConsumer(PrivateKeyProvider provider) {
        return new CryptoKeyReaderAdapter(null, provider);
    }

    @Override
    public EncryptionKeyInfo getPublicKey(String keyName, Map<String, String> metadata) {
        if (publicKeyProvider == null) {
            throw new UnsupportedOperationException(
                    "getPublicKey called on a consumer-side CryptoKeyReaderAdapter");
        }
        var v5Key = publicKeyProvider.getPublicKey(keyName).join();
        return new EncryptionKeyInfo(v5Key.key(), v5Key.metadata());
    }

    @Override
    public EncryptionKeyInfo getPrivateKey(String keyName, Map<String, String> metadata) {
        if (privateKeyProvider == null) {
            throw new UnsupportedOperationException(
                    "getPrivateKey called on a producer-side CryptoKeyReaderAdapter");
        }
        var v5Key = privateKeyProvider.getPrivateKey(keyName, metadata).join();
        return new EncryptionKeyInfo(v5Key.key(), v5Key.metadata());
    }
}
