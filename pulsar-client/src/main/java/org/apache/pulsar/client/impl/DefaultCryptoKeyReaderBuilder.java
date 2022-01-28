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

import java.util.Map;
import org.apache.pulsar.client.impl.conf.DefaultCryptoKeyReaderConfigurationData;

public class DefaultCryptoKeyReaderBuilder implements Cloneable {

    private DefaultCryptoKeyReaderConfigurationData conf;

    DefaultCryptoKeyReaderBuilder() {
        this(new DefaultCryptoKeyReaderConfigurationData());
    }

    DefaultCryptoKeyReaderBuilder(DefaultCryptoKeyReaderConfigurationData conf) {
        this.conf = conf;
    }

    public DefaultCryptoKeyReaderBuilder defaultPublicKey(String defaultPublicKey) {
        conf.setDefaultPublicKey(defaultPublicKey);
        return this;
    }

    public DefaultCryptoKeyReaderBuilder defaultPrivateKey(String defaultPrivateKey) {
        conf.setDefaultPrivateKey(defaultPrivateKey);
        return this;
    }

    public DefaultCryptoKeyReaderBuilder publicKey(String keyName, String publicKey) {
        conf.setPublicKey(keyName, publicKey);
        return this;
    }

    public DefaultCryptoKeyReaderBuilder privateKey(String keyName, String privateKey) {
        conf.setPrivateKey(keyName, privateKey);
        return this;
    }

    public DefaultCryptoKeyReaderBuilder publicKeys(Map<String, String> publicKeys) {
        conf.getPublicKeys().putAll(publicKeys);
        return this;
    }

    public DefaultCryptoKeyReaderBuilder privateKeys(Map<String, String> privateKeys) {
        conf.getPrivateKeys().putAll(privateKeys);
        return this;
    }

    public DefaultCryptoKeyReader build() {
        return new DefaultCryptoKeyReader(conf);
    }

    @Override
    public DefaultCryptoKeyReaderBuilder clone() {
        return new DefaultCryptoKeyReaderBuilder(conf.clone());
    }

}
