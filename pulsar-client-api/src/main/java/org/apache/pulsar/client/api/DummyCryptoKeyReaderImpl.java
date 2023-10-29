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
package org.apache.pulsar.client.api;

import java.util.Map;

/**
 * An empty implement. Doesn't provide any public key or private key, and just returns `null`.
 */
public class DummyCryptoKeyReaderImpl implements CryptoKeyReader {

    public static final DummyCryptoKeyReaderImpl INSTANCE = new DummyCryptoKeyReaderImpl();

    private DummyCryptoKeyReaderImpl(){}

    @Override
    public EncryptionKeyInfo getPublicKey(String keyName, Map<String, String> metadata) {
        return null;
    }

    @Override
    public EncryptionKeyInfo getPrivateKey(String keyName, Map<String, String> metadata) {
        return null;
    }
}