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

/**
 * Adapts a V5 {@link org.apache.pulsar.client.api.v5.auth.CryptoKeyReader} to the V4
 * {@link CryptoKeyReader} interface used by the underlying client implementation.
 */
final class CryptoKeyReaderAdapter implements CryptoKeyReader {

    private final org.apache.pulsar.client.api.v5.auth.CryptoKeyReader v5Reader;

    private CryptoKeyReaderAdapter(org.apache.pulsar.client.api.v5.auth.CryptoKeyReader v5Reader) {
        this.v5Reader = v5Reader;
    }

    static CryptoKeyReader wrap(org.apache.pulsar.client.api.v5.auth.CryptoKeyReader v5Reader) {
        return new CryptoKeyReaderAdapter(v5Reader);
    }

    @Override
    public EncryptionKeyInfo getPublicKey(String keyName, Map<String, String> metadata) {
        var v5Key = v5Reader.getPublicKey(keyName, metadata);
        return new EncryptionKeyInfo(v5Key.key(), v5Key.metadata());
    }

    @Override
    public EncryptionKeyInfo getPrivateKey(String keyName, Map<String, String> metadata) {
        var v5Key = v5Reader.getPrivateKey(keyName, metadata);
        return new EncryptionKeyInfo(v5Key.key(), v5Key.metadata());
    }
}
