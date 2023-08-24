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
package org.apache.pulsar.client.impl.crypto;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Optional;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.EncryptionKeyInfo;
import org.testng.Assert;

public class EncKeyReader implements CryptoKeyReader {

    public enum KeyType {
        PUBLIC("public"), PRIVATE("private");
        private final String id;

        KeyType(String id) {
            this.id = id;
        }
    }

    public static byte[] getKeyAsPEM(KeyType keyType, String keyName, Map<String, String> keyMeta) {
        String CERT_FILE_PATH = "../src/test/resources/certificate/" + keyType.id + "-key." + keyName;
        if (Files.isReadable(Paths.get(CERT_FILE_PATH))) {
            try {
                return Files.readAllBytes(Paths.get(CERT_FILE_PATH));
            } catch (IOException e) {
                Assert.fail("Failed to read certificate from " + CERT_FILE_PATH);
            }
        } else {
            Assert.fail("Certificate file " + CERT_FILE_PATH + " is not present or not readable.");
        }
        return null;
    }

    @Override
    public EncryptionKeyInfo getPublicKey(String keyName, Map<String, String> keyMeta) {
        return Optional.ofNullable(getKeyAsPEM(KeyType.PUBLIC, keyName, keyMeta))
                .map(keyData -> {
                    EncryptionKeyInfo keyInfo = new EncryptionKeyInfo();
                    keyInfo.setKey(keyData);
                    return keyInfo;
                })
                .orElse(null);
    }

    @Override
    public EncryptionKeyInfo getPrivateKey(String keyName, Map<String, String> keyMeta) {
        return Optional.ofNullable(getKeyAsPEM(KeyType.PRIVATE, keyName, keyMeta))
                .map(keyData -> {
                    EncryptionKeyInfo keyInfo = new EncryptionKeyInfo();
                    keyInfo.setKey(keyData);
                    return keyInfo;
                })
                .orElse(null);
    }
}