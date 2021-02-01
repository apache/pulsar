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

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URLConnection;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.EncryptionKeyInfo;
import org.apache.pulsar.client.api.url.URL;
import org.apache.pulsar.client.impl.conf.DefaultCryptoKeyReaderConfigurationData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
 
public class DefaultCryptoKeyReader implements CryptoKeyReader {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultCryptoKeyReader.class);

    private static final String APPLICATION_X_PEM_FILE = "application/x-pem-file";
 
    private String defaultPublicKey;
    private String defaultPrivateKey;

    private Map<String, String> publicKeys;
    private Map<String, String> privateKeys;

    public static DefaultCryptoKeyReaderBuilder builder() {
        return new DefaultCryptoKeyReaderBuilder();
    }
 
    DefaultCryptoKeyReader(DefaultCryptoKeyReaderConfigurationData conf) {
        this.defaultPublicKey = conf.getDefaultPublicKey();
        this.defaultPrivateKey = conf.getDefaultPrivateKey();
        this.publicKeys = conf.getPublicKeys();
        this.privateKeys = conf.getPrivateKeys();
    }
 
    @Override
    public EncryptionKeyInfo getPublicKey(String keyName, Map<String, String> metadata) {
        EncryptionKeyInfo keyInfo = new EncryptionKeyInfo();
        String publicKey = publicKeys.getOrDefault(keyName, defaultPublicKey);

        if (publicKey == null) {
            LOG.warn("Public key named {} is not set", keyName);
        } else {
            try {
                keyInfo.setKey(loadKey(publicKey));
            } catch (Exception e) {
                LOG.error("Failed to load public key named {}", keyName, e);
            }
        }

        return keyInfo;
    }
 
    @Override
    public EncryptionKeyInfo getPrivateKey(String keyName, Map<String, String> metadata) {
        EncryptionKeyInfo keyInfo = new EncryptionKeyInfo();
        String privateKey = privateKeys.getOrDefault(keyName, defaultPrivateKey);

        if (privateKey == null) {
            LOG.warn("Private key named {} is not set", keyName);
        } else {
            try {
                keyInfo.setKey(loadKey(privateKey));
            } catch (Exception e) {
                LOG.error("Failed to load private key named {}", keyName, e);
            }
        }

        return keyInfo;
    }

    private byte[] loadKey(String keyUrl) throws IOException, IllegalAccessException, InstantiationException {
        try {
            URLConnection urlConnection = new URL(keyUrl).openConnection();
            String protocol = urlConnection.getURL().getProtocol();
            if ("data".equals(protocol) && !APPLICATION_X_PEM_FILE.equals(urlConnection.getContentType())) {
                throw new IllegalArgumentException(
                        "Unsupported media type or encoding format: " + urlConnection.getContentType());
            }
            return IOUtils.toByteArray(urlConnection);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid key format");
        }
    }

}
