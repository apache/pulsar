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
package org.apache.pulsar.client.impl.conf;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class DefaultCryptoKeyReaderConfigurationData implements Serializable, Cloneable {

    private static final long serialVersionUID = 1L;

    private static final String TO_STRING_FORMAT =
            "%s(defaultPublicKey=%s, defaultPrivateKey=%s, publicKeys=%s, privateKeys=%s)";

    @NonNull
    private String defaultPublicKey;
    @NonNull
    private String defaultPrivateKey;

    @NonNull
    private Map<String, String> publicKeys = new HashMap<>();
    @NonNull
    private Map<String, String> privateKeys = new HashMap<>();

    public void setPublicKey(@NonNull String keyName, @NonNull String publicKey) {
        publicKeys.put(keyName, publicKey);
    }

    public void setPrivateKey(@NonNull String keyName, @NonNull String privateKey) {
        privateKeys.put(keyName, privateKey);
    }

    @Override
    public DefaultCryptoKeyReaderConfigurationData clone() {
        DefaultCryptoKeyReaderConfigurationData clone = new DefaultCryptoKeyReaderConfigurationData();

        if (defaultPublicKey != null) {
            clone.setDefaultPublicKey(defaultPublicKey);
        }

        if (defaultPrivateKey != null) {
            clone.setDefaultPrivateKey(defaultPrivateKey);
        }

        if (publicKeys != null) {
            clone.setPublicKeys(new HashMap<String, String>(publicKeys));
        }

        if (privateKeys != null) {
            clone.setPrivateKeys(new HashMap<String, String>(privateKeys));
        }

        return clone;
    }

    @Override
    public String toString() {
        return String.format(TO_STRING_FORMAT, getClass().getSimpleName(), maskKeyData(defaultPublicKey),
                maskKeyData(defaultPrivateKey), maskKeyData(publicKeys), maskKeyData(privateKeys));
    }

    private static String maskKeyData(Map<String, String> keys) {
        if (keys == null) {
            return "null";
        } else {
            StringBuilder keysStr = new StringBuilder();
            keysStr.append("{");

            List<String> kvList = new ArrayList<>();
            keys.forEach((k, v) -> kvList.add(k + "=" + maskKeyData(v)));
            keysStr.append(String.join(", ", kvList));

            keysStr.append("}");
            return keysStr.toString();
        }
    }

    private static String maskKeyData(String key) {
        if (key == null) {
            return "null";
        } else if (key.startsWith("data:")) {
            return "data:*****";
        } else {
            return key;
        }
    }

}
